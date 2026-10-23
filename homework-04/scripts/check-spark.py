#!/usr/bin/env python3
import csv
import hashlib
import io
import json
import os
import re
import shlex
import socket
import subprocess
import tempfile
import time
import uuid
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import urlopen


def check(condition, message):
    if not condition:
        raise ValueError(message)


def run(args, timeout=60, binary=False, extra_env=None, log=None):
    result = subprocess.run(["timeout", "-k", "5s", f"{timeout}s", *map(str, args)],
                            capture_output=True, text=not binary, env={**os.environ, **(extra_env or {})})
    if log:
        (evidence / log).write_text(result.stdout + "\n" + result.stderr)
    if result.returncode:
        detail = result.stderr.decode(errors="replace") if binary else result.stderr
        raise ValueError(f"Код {result.returncode}: {shlex.join(map(str, args[:3]))}\n{detail[-4000:]}")
    return result.stdout


def properties(path):
    return {p.findtext("name"): p.findtext("value") for p in ET.parse(path).getroot().findall("property")}


def api(url):
    with urlopen(url, timeout=5) as response:
        return json.load(response)


def remote(host, args):
    return run(["ssh", "-n", "-i", hive["ADMIN_KEY"], "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
                "-o", "ConnectTimeout=10", f"{env['ADMIN_USER']}@{host}", shlex.join(map(str, args))], timeout=30)


def hdfs(*args, binary=False):
    return run(["sudo", "-u", "hadoop", env["HADOOP_HOME"] + "/bin/hdfs", "--config",
                env["HADOOP_CONF_DIR"], "dfs", *args], binary=binary)


def sections(queries):
    query = "SET hive.query.results.cache.enabled=false; SET hive.fetch.task.conversion=more;\n"
    query += "\n".join(f"SELECT '__section_{i}__';\n{sql}" for i, sql in enumerate(queries))
    output = run(["bash", Path(env["HIVE_TASK_DIR"]) / "scripts/client.sh", "-e", query], timeout=120,
                 extra_env={"CLUSTER_ENV": env["HIVE_CLUSTER_ENV"]})
    result = []
    for i in range(len(queries)):
        marker = f"__section_{i}__\n"
        check(marker in output, f"Нет SQL-блока {i}")
        result.append(output.split(marker, 1)[1].split(f"__section_{i + 1}__\n", 1)[0])
    return result


def rows(text):
    return [[v.strip() for v in row] for row in csv.reader(io.StringIO(text), delimiter="\t")
            if row and any(v.strip() for v in row)]


def health():
    beans = {b["name"].split("name=")[-1]: b for b in api(nn_url + "/jmx")["beans"]}
    state, fs, info = (beans[name] for name in ("FSNamesystemState", "FSNamesystem", "NameNodeInfo"))
    check(state["NumLiveDataNodes"] == len(workers) == 3 and not info["Safemode"], "Нужны 3 DN и safe mode OFF")
    for key in ("NumDeadDataNodes", "NumStaleDataNodes", "NumStaleStorages", "VolumeFailuresTotal"):
        check(state[key] == 0, f"HDFS {key}={state[key]}")
    check(all(fs[key] == 0 for key in ("MissingBlocks", "CorruptBlocks", "UnderReplicatedBlocks")), "Есть проблемные блоки HDFS")
    live = json.loads(info["LiveNodes"])
    check({v["xferaddr"].split(":")[0] for v in live.values()} == worker_ips and
          all(v["adminState"] == "In Service" for v in live.values()), "Другой состав DataNode")
    check(api(rm_url + "/ws/v1/cluster/info")["clusterInfo"]["state"] == "STARTED", "RM не STARTED")
    metrics = api(rm_url + "/ws/v1/cluster/metrics")["clusterMetrics"]
    check(metrics["activeNodes"] == metrics["totalNodes"] == 3, "Нужны 3 активных NM")
    for key in ("lostNodes", "unhealthyNodes", "decommissioningNodes", "decommissionedNodes", "rebootedNodes", "shutdownNodes"):
        check(metrics[key] == 0, f"YARN {key}={metrics[key]}")
    memory, cores = int(yarn["yarn.nodemanager.resource.memory-mb"]), int(yarn["yarn.nodemanager.resource.cpu-vcores"])
    check(metrics["totalMB"] == 3 * memory and metrics["totalVirtualCores"] == 3 * cores, "Другие ресурсы YARN")
    nodes = api(rm_url + "/ws/v1/cluster/nodes")["nodes"]["node"]
    check(len(nodes) == 3 and {n["nodeHostName"] for n in nodes} == set(workers), "Другой состав NM")
    check(all(n["state"] == "RUNNING" and not n["healthReport"] and
              n["usedMemoryMB"] + n["availMemoryMB"] == memory and
              n["usedVirtualCores"] + n["availableVirtualCores"] == cores for n in nodes), "NM нездоров или ресурсы отличаются")
    families = ("org.apache.hadoop.hdfs.server.", "org.apache.hadoop.yarn.server.",
                "org.apache.hadoop.mapreduce.v2.hs.JobHistoryServer", "org.apache.hadoop.hive.metastore.HiveMetaStore",
                "org.apache.hive.service.server.HiveServer2")
    pids = {}
    for host in dict.fromkeys([env["EDGE_HOST"], *workers]):
        command = ["sudo", "-u", "hadoop", env["JAVA_HOME"] + "/bin/jps", "-l"]
        output = run(command) if host == env["EDGE_HOST"] else remote(host, command)
        pids[host] = sorted(line.split() for line in output.splitlines()
                            if len(line.split()) == 2 and line.split()[1].startswith(families))
        check(pids[host], f"Нет основных JVM на {host}")
    check(run(["systemctl", "is-active", "hive-practice3-server"]).strip() == "active", "HS2 не active")
    units = ["hive-practice3-metastore", f"postgresql@{hive['PG_VERSION']}-{hive['PG_CLUSTER']}"]
    check(remote(env["HMS_HOST"], ["systemctl", "is-active", *units]).split() == ["active", "active"], "HMS/PostgreSQL не active")
    pids["postgresql"] = remote(env["HMS_HOST"], ["systemctl", "show", "--property=MainPID", "--value", units[1]]).strip()
    check(int(pids["postgresql"]) > 0, "Нет PID PostgreSQL")
    check(isinstance(api(f"http://127.0.0.1:{hive['HS2_WEB_PORT']}/api/v1/sessions"), list), "Нет HS2 sessions API")
    print(f"HDFS: 3 здоровых DN, блоки в порядке; YARN: 3 NM, {3 * memory} МиБ/{3 * cores} vcores; Hive active")
    return pids


def orc_hashes(path):
    listing = hdfs("-ls", "-R", path)
    files = sorted(line.split()[-1] for line in listing.splitlines()
                   if line.startswith("-") and not Path(line.split()[-1]).name.startswith((".", "_")))
    check(files, f"Нет ORC в {path}")
    result = {}
    for name in files:
        data = hdfs("-cat", name, binary=True)
        check(data.startswith(b"ORC"), f"Файл не ORC: {name}")
        result[name] = hashlib.sha256(data).hexdigest()
    return result


def previous():
    sample = hdfs("-cat", "/user/hadoop/homework-01/sample.bin", binary=True)
    digest = hashlib.sha256(sample).hexdigest()
    check(len(sample) == 2621440 and digest == "b1ccfef6cbd6d79b9b77f126b2a2f1a58d8f3d54beccdeeec3b7f772453ffcee", "Изменился файл работы 1")
    output = sections([f"SELECT order_id,city,amount,order_date FROM homework03.{table};" for table in ("orders_raw", "orders")] +
                      ["SHOW PARTITIONS homework03.orders;"])
    actual = [sorted((int(r[0]), r[1], Decimal(r[2]), date.fromisoformat(r[3]).isoformat()) for r in rows(text))
              for text in output[:2]]
    check(actual == [source_rows, source_rows] and sorted(r[0] for r in rows(output[2])) == partitions, "Изменились данные работы 3")
    hashes = orc_hashes(f"{filesystem}{hive['EXTERNAL_DIR']}/homework03/orders")
    print(f"Предыдущие работы: sample.bin SHA256 {digest}; Hive raw/ORC 9 строк, 3 партиции, {len(hashes)} ORC")
    return digest, actual, output[2], hashes


def storage(text):
    values, params = {}, {}
    for row in rows(text):
        if len(row) >= 2 and row[0] and not row[0].startswith("#"):
            values.setdefault(row[0].rstrip(":"), row[1])
        elif len(row) >= 3 and not row[0]:
            params.setdefault(row[1], row[2])
    return values, params


def result_check():
    queries = [f"SELECT city,orders_count,total_amount,avg_amount,order_date FROM {table};",
               f"SHOW PARTITIONS {table};", f"DESCRIBE FORMATTED {table};", f"DESCRIBE DATABASE EXTENDED `{database}`;"]
    queries += [f"DESCRIBE FORMATTED {table} PARTITION (order_date='{day}');" for day in days]
    output = sections(queries)
    actual = sorted((r[0], int(r[1]), Decimal(r[2]), Decimal(r[3]), date.fromisoformat(r[4]).isoformat()) for r in rows(output[0]))
    check(actual == expected and len({(r[0], r[4]) for r in actual}) == 6, "Агрегаты отличаются от Python Decimal или есть дубли")
    check(sorted(r[0] for r in rows(output[1])) == partitions, "Нужны ровно 3 DATE-партиции")
    columns, partition_columns, current = [], [], None
    current = columns
    for row in rows(output[2]):
        if row[0].startswith("# Detailed"):
            break
        if row[0] == "# Partition Information":
            current = partition_columns
        elif row[0] and not row[0].startswith("#"):
            current.append(tuple(row[:2]))
    check(columns == [("city", "string"), ("orders_count", "bigint"), ("total_amount", "decimal(12,2)"),
                      ("avg_amount", "decimal(12,2)")] and partition_columns == [("order_date", "date")], "Другая схема таблицы")
    info, params = storage(output[2])
    required = {"Table Type": "EXTERNAL_TABLE", "Location": base, "SerDe Library": "org.apache.hadoop.hive.ql.io.orc.OrcSerde",
                "InputFormat": "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat", "OutputFormat": "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat"}
    check(all(info.get(k) == v for k, v in required.items()) and params.get("course.dataset") == marker and
          params.get("spark.sql.sources.provider") == "orc", "Другой формат, LOCATION или метка таблицы")
    db = rows(output[3])
    check(len(db) == 1 and db[0][0] == database and db[0][2] == db_uri and
          "course.dataset=" + marker in db[0][-1], "Другой LOCATION или метка базы")
    stable = [required, {"course.dataset": params["course.dataset"]}, db[0]]
    for day, text in zip(days, output[4:]):
        info, _ = storage(text)
        wanted = {k: v for k, v in required.items() if k not in ("Table Type", "Location")}
        wanted["Location"] = f"{base}/order_date={day}"
        check(all(info.get(k) == v for k, v in wanted.items()), f"Другой storage партиции {day}")
        stable.append(wanted)
    orc_hashes(base)
    print("Beeline: 6 точных групп, 9 заказов, сумма 640.00; EXTERNAL ORC, DECIMAL(12,2), 3 DATE-партиции и LOCATION совпали")
    return actual, stable


def pruning():
    output = sections([f"EXPLAIN DEPENDENCY SELECT city FROM {table}{where};"
                       for where in ("", f" WHERE order_date='{days[0]}'")])
    selected = [{p["partitionName"] for p in json.loads(text)["input_partitions"]} for text in output]
    check(selected == [{f"{database}@orders_daily@{part}" for part in partitions},
                       {f"{database}@orders_daily@order_date={days[0]}"}], "Hive не отсекает лишние партиции")
    print(f"EXPLAIN DEPENDENCY: 3 партиции → только order_date={days[0]}")


def event_check(events, app_id, tag):
    starts = [e for e in events if e["Event"] == "SparkListenerApplicationStart"]
    check(len(starts) == 1 and starts[0]["App ID"] == app_id and
          starts[0]["App Name"] == "homework04-" + database, "Event log другого приложения")
    check(any(e["Event"] == "SparkListenerApplicationEnd" for e in events), "Event log не завершен")
    config = next(e for e in events if e["Event"] == "SparkListenerEnvironmentUpdate")["Spark Properties"]
    check(config.get("spark.master") == "yarn" and config.get("spark.sql.catalogImplementation") == "hive" and
          config.get("spark.hadoop.hive.metastore.uris") == f"thrift://{env['HMS_HOST']}:{env['HMS_PORT']}" and
          tag in config.get("spark.yarn.tags", "").split(","), "Другой master/catalog/Metastore/tag в event log")
    plans = "\n".join(e.get("physicalPlanDescription", "") for e in events)
    check(input_uri in plans and "Scan csv" in plans and "Exchange" in plans and "Aggregate" in plans,
          "Нет плана чтения HDFS CSV, агрегации и shuffle")
    executors = {e["Executor ID"]: e["Executor Info"]["Host"] for e in events if e["Event"] == "SparkListenerExecutorAdded"}
    check(executors and all(key != "driver" and socket.gethostbyname(host) in worker_ips for key, host in executors.items()),
          "Нет удаленных executors на узлах YARN")
    tasks = [e for e in events if e["Event"] == "SparkListenerTaskEnd" and e["Task End Reason"]["Reason"] == "Success"
             and not e["Task Info"]["Failed"] and not e["Task Info"]["Killed"]]
    check(tasks and all(e["Task Info"]["Executor ID"] in executors and
                       socket.gethostbyname(e["Task Info"]["Host"]) in worker_ips for e in tasks), "Нет успешных удаленных tasks")
    completed = {e["Stage Info"]["Stage ID"] for e in events if e["Event"] == "SparkListenerStageCompleted"
                 and not e["Stage Info"].get("Failure Reason")}
    check(completed and all(e["Stage ID"] in completed for e in tasks), "Нет завершенных stages")
    metrics = [e["Task Metrics"] for e in tasks]
    read = sum(m["Input Metrics"]["Bytes Read"] for m in metrics)
    written = sum(m["Shuffle Write Metrics"]["Shuffle Bytes Written"] for m in metrics)
    shuffled = sum(m["Shuffle Read Metrics"]["Remote Bytes Read"] + m["Shuffle Read Metrics"]["Local Bytes Read"] for m in metrics)
    check(read > 0 and written > 0 and shuffled > 0, "Нет фактического чтения HDFS и shuffle")
    print(f"Event log: {len(tasks)} успешных remote tasks, {len(completed)} stages; input {read}, shuffle write/read {written}/{shuffled} байт")
    print("Executors: " + ", ".join(f"{key}@{host}" for key, host in sorted(executors.items())))


def pipeline(label):
    tag = f"homework04_verify_{suffix}_{label}"
    output = run(["bash", task / "scripts/run-job.sh", env["CLUSTER_ENV"], database], timeout=360,
                 extra_env={"SPARK_YARN_TAGS": tag}, log=label + ".log")
    ids = re.findall(r"Готово: " + re.escape(database) + r"\.orders_daily, [^\n]*приложение (application_\d+_\d+)\.", output)
    check(len(ids) == 1, "Нет однозначного Spark appId в результате pipeline")
    app_id = ids[0]
    deadline = time.monotonic() + 60
    while True:
        app = api(rm_url + f"/ws/v1/cluster/apps/{app_id}")["app"]
        check(app["state"] not in ("FAILED", "KILLED"), f"{app_id}: {app['state']}")
        if app["state"] == "FINISHED":
            break
        check(time.monotonic() < deadline, f"{app_id} не завершился")
        time.sleep(1)
    check(app["finalStatus"] == "SUCCEEDED" and app["applicationType"] == "SPARK" and
          tag in app.get("applicationTags", "").split(","), "Нет успешного Spark приложения с нашим tag")
    data = hdfs("-cat", f"{filesystem}{env['EVENT_LOG_DIR']}/{app_id}")
    (evidence / (app_id + ".jsonl")).write_text(data)
    event_check([json.loads(line) for line in data.splitlines() if line.strip()], app_id, tag)
    print(f"YARN: {app_id}, SPARK FINISHED/SUCCEEDED, tag={tag}")
    return app_id


def main():
    global env, hive, task, evidence, workers, worker_ips, yarn, nn_url, rm_url, filesystem
    global source_rows, days, partitions, expected, suffix, database, table, db_uri, base, input_uri, marker
    env = os.environ
    task = Path(__file__).resolve().parent.parent
    suffix = uuid.uuid4().hex[:10]
    database, marker = "homework04_check_" + suffix, "homework04.orders.v1"
    evidence = Path(tempfile.mkdtemp(prefix="spark-verify-" + suffix + "-"))
    print(f"База проверки: {database}; подробные логи: {evidence}")
    loaded = run(["bash", "-c", 'set -a; source "$1"; env -0', "bash", env["HIVE_CLUSTER_ENV"]])
    hive = dict(item.split("=", 1) for item in loaded.split("\0") if "=" in item)
    conf = Path(env["HADOOP_CONF_DIR"])
    core, hdfs_props, yarn = [properties(conf / (name + "-site.xml")) for name in ("core", "hdfs", "yarn")]
    filesystem = core["fs.defaultFS"].rstrip("/")
    check(filesystem.startswith("hdfs://"), "Нужен HDFS")
    workers = (conf / "workers").read_text().split()
    worker_ips = {socket.gethostbyname(host) for host in workers}
    nn_url = "http://" + hdfs_props["dfs.namenode.http-address"].replace("0.0.0.0", urlsplit(filesystem).hostname)
    rm_url = "http://" + yarn["yarn.resourcemanager.webapp.address"].replace("0.0.0.0", env["EDGE_HOST"])
    source = (Path(env["HIVE_TASK_DIR"]) / "data/orders.csv").read_bytes()
    source_rows = sorted((int(r[0]), r[1], Decimal(r[2]), date.fromisoformat(r[3]).isoformat())
                         for r in csv.reader(io.StringIO(source.decode())))
    check(len(source_rows) == 9 and {r[0] for r in source_rows} == set(range(1, 10)) and
          sum(r[2] for r in source_rows) == Decimal("640.00"), "Изменился исходный набор заказов")
    grouped = defaultdict(list)
    for _, city, amount, day in source_rows:
        grouped[(city.strip().upper(), day)].append(amount)
    expected = sorted((city, len(amounts), sum(amounts), (sum(amounts) / len(amounts)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP), day)
                      for (city, day), amounts in grouped.items())
    days = sorted({r[3] for r in source_rows})
    partitions = ["order_date=" + day for day in days]
    table = f"`{database}`.orders_daily"
    db_uri = f"{filesystem}{env['EXTERNAL_DIR']}/{database}"
    base, input_uri = db_uri + "/orders_daily", f"{filesystem}{env['HDFS_ROOT']}/input/{database}/orders.csv"
    baseline, old = health(), previous()
    (evidence / "previous.json").write_text(json.dumps({"pids": baseline, "orc_sha256": old[3]}, indent=2))
    run(["bash", task / "scripts/load-data.sh", env["CLUSTER_ENV"], database], log="load.log")
    check(hdfs("-cat", input_uri, binary=True) == source, "HDFS input отличается от исходного CSV")
    print(f"До Spark: HDFS input {len(source)} байт, SHA256 {hashlib.sha256(source).hexdigest()}")
    first = pipeline("first")
    result = result_check()
    pruning()
    second = pipeline("repeat")
    check(second != first and result_check() == result, "Повтор изменил строки/метаданные или appId")
    check(hdfs("-cat", input_uri, binary=True) == source, "Pipeline изменил HDFS input")
    check(health() == baseline and previous() == old, "Изменились PID или данные предыдущих работ")
    print("Повтор: строки и метаданные совпали, дублей нет; PID и данные предыдущих работ сохранены")
    print(f"Результат оставлен: {database}.orders_daily; {base}")
    print(f"Приложения: {first}, {second}")
    print("Проверка Spark пройдена.")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, OSError, StopIteration, subprocess.TimeoutExpired) as error:
        raise SystemExit(f"Проверка Spark не прошла: {error}")
