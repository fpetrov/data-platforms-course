#!/usr/bin/env python3
import argparse
import csv
import hashlib
import io
import ipaddress
import json
import os
import queue
import re
import shlex
import socket
import subprocess
import tempfile
import threading
import time
import uuid
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from urllib.parse import unquote_plus
from urllib.request import urlopen


def check(condition, message):
    if not condition:
        raise ValueError(message)


def run(args, timeout=60, binary=False):
    result = subprocess.run(["timeout", "-k", "5s", f"{timeout}s", *map(str, args)],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=not binary)
    if result.returncode:
        detail = result.stderr.decode(errors="replace") if binary else result.stderr
        raise ValueError(f"Команда завершилась с кодом {result.returncode}: {shlex.join(map(str, args[:3]))}\n{detail[-5000:]}")
    return result


def remote(args, host=None):
    return run(["ssh", "-n", "-i", env["ADMIN_KEY"], "-o", "BatchMode=yes", "-o",
                "StrictHostKeyChecking=yes", "-o", "ConnectTimeout=10",
                f"{env['ADMIN_USER']}@{host or env['HMS_HOST']}", shlex.join(map(str, args))], timeout=30).stdout


def properties(path):
    return {item.findtext("name"): item.findtext("value")
            for item in ET.parse(path).getroot().findall("property")}


def api(url):
    with urlopen(url, timeout=5) as response:
        return json.load(response)


def hdfs(*args, binary=False):
    return run(["sudo", "-u", "hadoop", env["HADOOP_HOME"] + "/bin/hdfs",
                "--config", env["HADOOP_CONF_DIR"], "dfs", *args], binary=binary).stdout


def sql(query, username="hadoop", timeout=60, verbose=False):
    options = ["--silent=false"] if verbose else []
    return run(["bash", task / "scripts/client.sh", "-n", username, *options, "-e",
                "SET hive.fetch.task.conversion=more; " + query], timeout=timeout)


def sections(queries):
    statement = "\n".join(f"SELECT '__section_{i}__';\n{query}" for i, query in enumerate(queries))
    output = sql(statement).stdout
    result = []
    for i in range(len(queries)):
        marker = f"__section_{i}__\n"
        check(marker in output, f"Нет результата SQL-блока {i}")
        segment = output.split(marker, 1)[1]
        result.append(segment.split(f"__section_{i + 1}__\n", 1)[0])
    return result


def listeners(text, ports):
    result = set()
    for line in text.splitlines():
        host, port = line.split()[3].rsplit(":", 1)
        if port not in {str(value) for value in ports}:
            continue
        address = ipaddress.ip_address(host.strip("[]"))
        if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped:
            address = address.ipv4_mapped
        result.add(f"{address}:{port}")
    return result


def health():
    nn_beans = {bean["name"].split("name=")[-1]: bean for bean in api(nn_url + "/jmx")["beans"]}
    state, fs, info = (nn_beans[name] for name in ("FSNamesystemState", "FSNamesystem", "NameNodeInfo"))
    check(state["NumLiveDataNodes"] == 3 and not info["Safemode"], "Нужны 3 DataNode и safe mode OFF")
    for key in ("NumDeadDataNodes", "NumStaleDataNodes", "NumStaleStorages", "NumDecommissioningDataNodes",
                "NumDecomLiveDataNodes", "NumDecomDeadDataNodes", "NumInMaintenanceLiveDataNodes",
                "NumInMaintenanceDeadDataNodes", "NumEnteringMaintenanceDataNodes", "VolumeFailuresTotal"):
        check(state[key] == 0, f"HDFS {key}={state[key]}")
    check(all(fs[key] == 0 for key in ("MissingBlocks", "CorruptBlocks", "UnderReplicatedBlocks")), "Есть проблемные блоки HDFS")
    nodes = json.loads(info["LiveNodes"])
    expected_ips = {socket.gethostbyname(host) for host in workers}
    check({node["xferaddr"].split(":")[0] for node in nodes.values()} == expected_ips, "Другой состав DataNode")
    check(all(node["adminState"] == "In Service" for node in nodes.values()), "DataNode выведен из обслуживания")
    check(api(rm_url + "/ws/v1/cluster/info")["clusterInfo"]["state"] == "STARTED", "RM не запущен")
    metrics = api(rm_url + "/ws/v1/cluster/metrics")["clusterMetrics"]
    check(metrics["activeNodes"] == metrics["totalNodes"] == 3, "Нужны 3 активных NM")
    for key in ("lostNodes", "unhealthyNodes", "decommissioningNodes", "decommissionedNodes", "rebootedNodes", "shutdownNodes"):
        check(metrics[key] == 0, f"YARN {key}={metrics[key]}")
    memory, cores = int(yarn_props["yarn.nodemanager.resource.memory-mb"]), int(yarn_props["yarn.nodemanager.resource.cpu-vcores"])
    check(metrics["totalMB"] == 3 * memory and metrics["totalVirtualCores"] == 3 * cores, "Неверные ресурсы YARN")
    nms = api(rm_url + "/ws/v1/cluster/nodes")["nodes"]["node"]
    check(len(nms) == 3 and {node["nodeHostName"] for node in nms} == set(workers), "Другой состав NodeManager")
    for node in nms:
        check(node["state"] == "RUNNING" and not node["healthReport"], "NodeManager нездоров")
        check(node["usedMemoryMB"] + node["availMemoryMB"] == memory, "Неверная память NodeManager")
        check(node["usedVirtualCores"] + node["availableVirtualCores"] == cores, "Неверные vcores NodeManager")
    pids = {}
    families = ("org.apache.hadoop.hdfs.server.", "org.apache.hadoop.yarn.server.", "org.apache.hadoop.mapreduce.v2.hs.JobHistoryServer")
    for host in dict.fromkeys([env["EDGE_HOST"], *workers]):
        command = ["sudo", "-u", "hadoop", env["JAVA_HOME"] + "/bin/jps", "-l"]
        output = run(command).stdout if host == env["EDGE_HOST"] else remote(command, host)
        pids[host] = sorted(line.split() for line in output.splitlines()
                            if len(line.split()) == 2 and line.split()[1].startswith(families))
        check(pids[host], f"Нет процессов HDFS/YARN на {host}")
    check(run(["systemctl", "is-active", "hive-practice3-server"]).stdout.strip() == "active", "HS2 не active")
    check(remote(["systemctl", "is-active", "hive-practice3-metastore", f"postgresql@{env['PG_VERSION']}-{env['PG_CLUSTER']}"]).split() == ["active", "active"], "HMS или PostgreSQL не active")
    ports = [env["HS2_PORT"], env["HS2_WEB_PORT"]]
    check(listeners(run(["ss", "-ltnH"]).stdout, ports) == {f"127.0.0.1:{port}" for port in ports}, "HS2 должен слушать только localhost")
    check(listeners(remote(["ss", "-ltnH"]), [env["HMS_PORT"], env["PG_PORT"]]) ==
          {f"{env['HMS_IP']}:{env['HMS_PORT']}", f"127.0.0.1:{env['PG_PORT']}"}, "Неверный bind HMS/PostgreSQL")
    server = properties(Path(env["CONF_DIR"]) / "server/hive-site.xml")
    check(server.get("hive.metastore.uris") == f"thrift://{env['HMS_HOST']}:{env['HMS_PORT']}", "Нужен удаленный Metastore")
    schema = remote(["sudo", "-u", "postgres", "psql", "-XAt", "-p", env["PG_PORT"], "-d", env["PG_DATABASE"],
                     "-c", 'SELECT "SCHEMA_VERSION" FROM "VERSION";']).strip()
    check(bool(schema), "Нет схемы Hive в PostgreSQL")
    print(f"HDFS: 3 здоровых DN, блоки в порядке, safe mode OFF; YARN: 3 NM, {3 * memory} МиБ, {3 * cores} vcores")
    print(f"HS2: localhost; HMS: отдельный узел; PostgreSQL: localhost, schema {schema}")
    return pids, schema


def sample():
    data = hdfs("-cat", "/user/hadoop/homework-01/sample.bin", binary=True)
    digest = hashlib.sha256(data).hexdigest()
    check(len(data) == 2621440 and digest == "b1ccfef6cbd6d79b9b77f126b2a2f1a58d8f3d54beccdeeec3b7f772453ffcee", "Изменился файл работы 1")
    print(f"Файл работы 1: {len(data)} байт, SHA256 {digest}")
    return digest


def sessions():
    return api(web_url + "/api/v1/sessions")


def gauge():
    try:
        for bean in api(web_url + "/jmx?qry=metrics:name=hs2_open_sessions")["beans"]:
            if "hs2_open_sessions" in bean.get("name", ""):
                return int(bean["Value"])
    except (OSError, KeyError, ValueError):
        pass
    return None


def concurrency():
    baseline = {item["sessionId"] for item in sessions()}
    initial_gauge = gauge()
    clients = []
    try:
        for label in ("a", "b"):
            name = f"verify_{suffix}_{label}"
            process = subprocess.Popen(["bash", str(task / "scripts/client.sh"), "-n", name, "--force=false"],
                                       stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                       text=True, bufsize=1, start_new_session=True)
            client = {"name": name, "process": process, "queue": queue.Queue(), "lines": [], "errors": []}
            clients.append(client)
            def drain(stream, target, output_queue=None):
                for line in stream:
                    target.append(line)
                    if output_queue is not None:
                        output_queue.put(line.strip())
            for stream, target, output_queue in ((process.stdout, client["lines"], client["queue"]),
                                                  (process.stderr, client["errors"], None)):
                threading.Thread(target=drain, args=(stream, target, output_queue), daemon=True).start()
            process.stdin.write(f"SET hive.fetch.task.conversion=more;\nSELECT concat('{name}:', CAST(order_id AS STRING)) FROM `{database}`.orders;\nSELECT '{name}_ready';\n")
            process.stdin.flush()
        deadline = time.monotonic() + 60
        ready = set()
        while len(ready) < 2 and time.monotonic() < deadline:
            for client in clients:
                check(client["process"].poll() is None, "Клиент завершился до барьера: " + "".join(client["errors"])[-2000:])
                try:
                    if client["queue"].get(timeout=0.2) == client["name"] + "_ready":
                        ready.add(client["name"])
                except queue.Empty:
                    pass
        check(len(ready) == 2, "Два JDBC-клиента не дошли до общего барьера\n" +
              "".join(line for client in clients for line in client["errors"])[-3000:])
        current = sessions()
        ids = []
        for client in clients:
            selected = [item["sessionId"] for item in current if item["username"] == client["name"]]
            check(len(selected) == 1 and selected[0] not in baseline, "Нет отдельной живой сессии клиента")
            ids.extend(selected)
            rows = [line.strip() for line in client["lines"] if re.fullmatch(re.escape(client["name"]) + r":[1-9]", line.strip())]
            check(sorted(rows) == [f"{client['name']}:{i}" for i in range(1, 10)], "Клиент не прочитал все 9 заказов")
            print(f"JDBC: {client['name']}, sessionId={selected[0]}, прочитано 9 строк")
        check(len(set(ids)) == 2, "Совпали JDBC sessionId")
        if initial_gauge is not None:
            check(gauge() == initial_gauge + 2, "Счетчик открытых сессий не вырос на 2")
            print(f"Одновременно открыты 2 JDBC-сессии: gauge {initial_gauge} → {initial_gauge + 2}")
        for client in clients:
            client["process"].stdin.write("SELECT 1;\n!quit\n")
            client["process"].stdin.flush()
            client["process"].stdin.close()
        for client in clients:
            check(client["process"].wait(timeout=20) == 0, "JDBC-клиент завершился с ошибкой")
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if {item["sessionId"] for item in sessions()} == baseline and (initial_gauge is None or gauge() == initial_gauge):
                print(f"После закрытия клиентов: исходные {len(baseline)} сессий, утечек нет")
                return
            time.sleep(0.2)
        raise ValueError("Сессии не вернулись к исходному составу")
    finally:
        for client in clients:
            process = client["process"]
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


def parsed_rows(text):
    rows = []
    for row in csv.reader(io.StringIO(text), delimiter="\t"):
        if not row or not any(value.strip() for value in row):
            continue
        check(len(row) == 4, f"Неверная SQL-строка: {row}")
        rows.append((int(row[0]), row[1], Decimal(row[2]), date.fromisoformat(row[3]).isoformat()))
    return sorted(rows)


def data_check():
    results = sections([f"SELECT order_id,city,amount,order_date FROM `{database}`.{table};" for table in ("orders_raw", "orders")] +
                       [f"SHOW PARTITIONS `{database}`.orders;"])
    for table, output in zip(("orders_raw", "orders"), results):
        check(parsed_rows(output) == expected, f"{table}: SQL-строки отличаются от CSV")
    check(sorted(line for line in results[2].splitlines() if line.strip()) == [f"order_date={day}" for day in days], "Неверный набор партиций")
    print("SQL: raw и ORC совпали с CSV, 9 строк, 9 уникальных order_id, 3 партиции")
    print("Суммы: всего 640.00; по дням 250.00/200.00/190.00; Moscow 390.00, Kazan 250.00")


def metadata(stage):
    queries = [f"DESCRIBE FORMATTED `{database}`.{table};" for table in ("orders_raw", "orders")]
    queries += [f"DESCRIBE FORMATTED `{database}`.orders PARTITION (order_date='{day}');" for day in days]
    results = sections(queries)
    for table, output in zip(("orders_raw", "orders"), results):
        path = stage / f"{table}.tsv"
        path.write_text(output)
        run(["python3", task / "scripts/check-load.py", "table", path, f"{base}/{table}", table])
    path = stage / "partitions.tsv"
    path.write_text("".join(results[2:]))
    run(["python3", task / "scripts/check-load.py", "partitions", path, *[f"{base}/orders/order_date={day}" for day in days]])
    print("Метаданные: external TEXTFILE/ORC, схема, partition DATE и HDFS LOCATION совпали")
    return results


def pruning():
    results = sections([f"EXPLAIN DEPENDENCY SELECT city FROM `{database}`.orders{predicate};"
                        for predicate in ("", f" WHERE order_date='{days[0]}'")])
    selected = []
    for output in results:
        plan = json.loads(output)
        selected.append({part["partitionName"] for part in plan["input_partitions"]})
    check(selected[0] == {f"{database}@orders@order_date={day}" for day in days}, "Полный план не содержит 3 партиции")
    check(selected[1] == {f"{database}@orders@order_date={days[0]}"}, "Фильтр не отсек лишние партиции")
    print(f"EXPLAIN DEPENDENCY: все 3 партиции → только order_date={days[0]}")


def applications():
    return {app["id"]: app for app in (api(rm_url + "/ws/v1/cluster/apps").get("apps") or {}).get("app", [])}


def job_conf(job_id):
    deadline = time.monotonic() + 60
    while True:
        try:
            values = api(history_url + f"/ws/v1/history/mapreduce/jobs/{job_id}/conf")["conf"]["property"]
            return {item["name"]: item["value"] for item in values}
        except OSError:
            check(time.monotonic() < deadline, f"Нет сохраненной конфигурации {job_id}")
            time.sleep(1)


def jobs(ids, tag=None):
    check(ids, "SQL не запустил MapReduce в YARN")
    for job_id in ids:
        app_id = job_id.replace("job_", "application_", 1)
        deadline = time.monotonic() + 60
        while True:
            try:
                app = api(rm_url + f"/ws/v1/cluster/apps/{app_id}")["app"]
                history = api(history_url + f"/ws/v1/history/mapreduce/jobs/{job_id}")["job"]
                if app["state"] == "FINISHED" and history["state"] == "SUCCEEDED":
                    break
            except OSError:
                pass
            check(time.monotonic() < deadline, f"Не появилась успешная история {job_id}")
            time.sleep(1)
        check(app["finalStatus"] == "SUCCEEDED" and app["applicationType"] == "MAPREDUCE", f"YARN не выполнил {app_id}")
        conf = job_conf(job_id)
        check(conf.get("mapreduce.client.submit.file.replication") == hdfs_props["dfs.replication"], "Staging job запрашивает неверную репликацию")
        check(history["mapsCompleted"] == history["mapsTotal"] > 0, f"Нет завершенных map в {job_id}")
        if tag:
            check(tag in app["applicationTags"].split(","), "Приложение не связано с query tag")
            check(history["reducesCompleted"] == history["reducesTotal"] > 0 and history["uberized"] is False, "Агрегат не исполнил reduce")
            groups = api(history_url + f"/ws/v1/history/mapreduce/jobs/{job_id}/counters")["jobCounters"]["counterGroup"]
            counters = {counter["name"]: counter["totalCounterValue"] for group in groups for counter in group["counter"]}
            check(counters.get("MAP_INPUT_RECORDS", 0) > 0 and counters.get("HDFS_BYTES_READ", 0) > 0, "MR не обработал данные HDFS")
            print(f"MR: query tag={tag}, input records={counters['MAP_INPUT_RECORDS']}, HDFS read={counters['HDFS_BYTES_READ']} байт")
        print(f"YARN: {app_id}, FINISHED/SUCCEEDED; JHS: map {history['mapsCompleted']}/{history['mapsTotal']}, reduce {history['reducesCompleted']}/{history['reducesTotal']}")


def load():
    before = set(applications())
    result = run(["bash", task / "scripts/load-data.sh", env["CLUSTER_ENV"], database], timeout=300)
    ids = []
    for app_id in set(applications()) - before:
        job_id = app_id.replace("application_", "job_", 1)
        if f"`{database}`.orders" in unquote_plus(job_conf(job_id).get("hive.query.string", "")):
            ids.append(job_id)
    jobs(sorted(ids))
    print(result.stdout.strip())


def aggregate():
    tag = "homework03_verify_" + suffix
    before = set(applications())
    result = sql(f"SET hive.execution.engine=mr; SET mapreduce.framework.name=yarn; "
                 f"SET hive.fetch.task.conversion=none; SET hive.exec.mode.local.auto=false; "
                 f"SET hive.compute.query.using.stats=false; SET hive.query.results.cache.enabled=false; "
                 f"SET hive.query.tag={tag}; SELECT city,count(*),sum(amount) FROM `{database}`.orders GROUP BY city;", timeout=180, verbose=True)
    actual = {row[0]: (int(row[1]), Decimal(row[2])) for row in csv.reader(io.StringIO(result.stdout), delimiter="\t") if row and any(value.strip() for value in row)}
    counts, totals = Counter(), defaultdict(Decimal)
    for _, city, amount, _ in expected:
        counts[city] += 1
        totals[city] += amount
    check(actual == {city: (counts[city], totals[city]) for city in counts}, "MR-агрегат отличается от независимого расчета")
    ids = [app_id.replace("application_", "job_", 1) for app_id, app in applications().items()
           if app_id not in before and tag in app.get("applicationTags", "").split(",")]
    operation_ids = set(re.findall(r"Starting Job = (job_[0-9_]+)", result.stdout + result.stderr))
    if operation_ids:
        check(operation_ids == set(ids), "Job ID из operation log не совпал с query tag")
    jobs(sorted(ids), tag)
    print("MR-агрегат совпал с независимым расчетом Python Decimal")


def orc_checksums():
    listing = hdfs("-ls", "-R", base + "/orders")
    files = [line.split()[-1] for line in listing.splitlines() if line.startswith("-") and not Path(line.split()[-1]).name.startswith((".", "_"))]
    check(files, "Нет файлов ORC")
    result = {}
    for path in sorted(files):
        data = hdfs("-cat", path, binary=True)
        check(data.startswith(b"ORC"), f"Файл не ORC: {path}")
        result[path] = hashlib.sha256(data).hexdigest()
        print(f"ORC SHA256: {path} {result[path]}")
    return result


env = os.environ
task = Path(__file__).resolve().parent.parent
parser = argparse.ArgumentParser()
parser.add_argument("--restart", action="store_true")
args = parser.parse_args()
try:
    suffix = uuid.uuid4().hex[:10]
    database = "homework03_check_" + suffix
    core = properties(Path(env["HADOOP_CONF_DIR"]) / "core-site.xml")
    hdfs_props = properties(Path(env["HADOOP_CONF_DIR"]) / "hdfs-site.xml")
    yarn_props = properties(Path(env["HADOOP_CONF_DIR"]) / "yarn-site.xml")
    mapred_props = properties(Path(env["HADOOP_CONF_DIR"]) / "mapred-site.xml")
    workers = Path(env["HADOOP_CONF_DIR"]).joinpath("workers").read_text().split()
    check(len(set(workers)) == 3, "Нужны 3 разных worker-узла")
    nn_url = "http://" + hdfs_props["dfs.namenode.http-address"]
    rm_url = "http://" + yarn_props["yarn.resourcemanager.webapp.address"]
    history_url = "http://" + mapred_props["mapreduce.jobhistory.webapp.address"]
    web_url = f"http://127.0.0.1:{env['HS2_WEB_PORT']}"
    base = core["fs.defaultFS"].rstrip("/") + env["EXTERNAL_DIR"] + "/" + database
    with (task / "data/orders.csv").open(newline="") as source:
        expected = sorted((int(row[0]), row[1], Decimal(row[2]), date.fromisoformat(row[3]).isoformat()) for row in csv.reader(source))
    days = sorted({row[3] for row in expected})
    check([row[0] for row in expected] == list(range(1, 10)) and len(days) == 3 and sum(row[2] for row in expected) == Decimal(640), "Изменился контрольный CSV")
    print("Начало проверки:", datetime.now(timezone.utc).isoformat())
    print(f"База проверки: {database}; HDFS: {base}")
    baseline_pids, baseline_schema = health()
    baseline_sample = sample()
    with tempfile.TemporaryDirectory(prefix="hive-verify-") as directory:
        stage = Path(directory)
        print("Первая загрузка")
        load()
        data_check()
        metadata(stage)
        concurrency()
        pruning()
        aggregate()
        print("Повторная загрузка")
        load()
        data_check()
        baseline_metadata = metadata(stage)
        baseline_orc = orc_checksums()
        if args.restart:
            print("Перезапуск только Hive и выделенного PostgreSQL")
            run(["bash", task / "scripts/cluster.sh", "restart", env["CLUSTER_ENV"]], timeout=300)
            data_check()
            check(metadata(stage) == baseline_metadata, "После restart изменились метаданные")
            check(orc_checksums() == baseline_orc, "После restart изменились файлы ORC")
            print("После restart метаданные, SQL-данные и ORC SHA256 сохранены без повторной загрузки")
        final_pids, final_schema = health()
        check(final_pids == baseline_pids, "Изменились PID процессов HDFS/YARN")
        check(final_schema == baseline_schema and sample() == baseline_sample, "Не сохранились исходные данные")
        print("PID HDFS/YARN и файл работы 1 сохранены")
    print(f"Результат оставлен: {database}.orders; {base}/orders")
    print("Проверка Hive пройдена.")
except (ValueError, KeyError, OSError, subprocess.TimeoutExpired) as error:
    raise SystemExit(f"Проверка Hive не прошла: {error}")
