#!/usr/bin/env python3
import csv
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


def storage(text):
    values = {}
    for row in rows(text):
        if len(row) >= 2 and row[0] and not row[0].startswith("#"):
            values.setdefault(row[0].rstrip(":"), row[1])
    return values


def result_check():
    output = sections([f"SELECT city,orders_count,total_amount,avg_amount,order_date FROM {table};",
                       f"SHOW PARTITIONS {table};", f"DESCRIBE FORMATTED {table};"])
    actual = sorted((r[0], int(r[1]), Decimal(r[2]), Decimal(r[3]), date.fromisoformat(r[4]).isoformat()) for r in rows(output[0]))
    check(actual == expected, "Агрегаты отличаются от CSV или есть дубли")
    check(sorted(r[0] for r in rows(output[1])) == partitions, "Неверный набор партиций")
    columns, partition_columns = [], []
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
    info = storage(output[2])
    required = {"Table Type": "EXTERNAL_TABLE", "Location": base,
                "SerDe Library": "org.apache.hadoop.hive.ql.io.orc.OrcSerde",
                "InputFormat": "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"}
    check(all(info.get(k) == v for k, v in required.items()), "Другой формат или LOCATION таблицы")
    print(f"Beeline: {len(actual)} групп совпали с расчетом по CSV; ORC, схема и партиции верны")
    return actual, required


def pruning():
    output = sections([f"EXPLAIN DEPENDENCY SELECT city FROM {table}{where};"
                       for where in ("", f" WHERE order_date='{days[0]}'")])
    selected = [{p["partitionName"] for p in json.loads(text)["input_partitions"]} for text in output]
    check(selected == [{f"{database}@orders_daily@{part}" for part in partitions},
                       {f"{database}@orders_daily@order_date={days[0]}"}], "Hive не отсекает лишние партиции")
    print(f"Фильтр по {days[0]} выбирает одну партицию")


def event_check(events, app_id, tag):
    starts = [e for e in events if e["Event"] == "SparkListenerApplicationStart"]
    check(len(starts) == 1 and starts[0]["App ID"] == app_id and
          starts[0]["App Name"] == "homework04-" + database, "Event log другого приложения")
    check(any(e["Event"] == "SparkListenerApplicationEnd" for e in events), "Event log не завершен")
    config = next(e for e in events if e["Event"] == "SparkListenerEnvironmentUpdate")["Spark Properties"]
    check(config.get("spark.master") == "yarn" and config.get("spark.sql.catalogImplementation") == "hive" and
          config.get("spark.hadoop.hive.metastore.uris") == f"thrift://{env['HMS_HOST']}:{env['HMS_PORT']}" and
          tag in config.get("spark.yarn.tags", "").split(","), "Другой master/catalog/Metastore/tag в event log")
    executors = {e["Executor ID"]: e["Executor Info"]["Host"] for e in events if e["Event"] == "SparkListenerExecutorAdded"}
    check(executors and all(key != "driver" and socket.gethostbyname(host) in worker_ips for key, host in executors.items()),
          "Нет удаленных executors на узлах YARN")
    tasks = [e for e in events if e["Event"] == "SparkListenerTaskEnd" and e["Task End Reason"]["Reason"] == "Success"
             and not e["Task Info"]["Failed"] and not e["Task Info"]["Killed"]]
    check(tasks and all(e["Task Info"]["Executor ID"] in executors and
                       socket.gethostbyname(e["Task Info"]["Host"]) in worker_ips for e in tasks), "Нет успешных удаленных tasks")
    print("YARN: подтверждены executors и успешные задачи на worker-узлах")


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
    print(f"YARN: {app_id}, SPARK FINISHED/SUCCEEDED")
    return app_id


def main():
    global env, task, evidence, worker_ips, rm_url, filesystem
    global days, partitions, expected, suffix, database, table, base
    env = os.environ
    task = Path(__file__).resolve().parent.parent
    suffix = uuid.uuid4().hex[:10]
    database = "homework04_check_" + suffix
    evidence = Path(tempfile.mkdtemp(prefix="spark-verify-" + suffix + "-"))
    print(f"База проверки: {database}; логи: {evidence}")
    conf = Path(env["HADOOP_CONF_DIR"])
    core, yarn = [properties(conf / (name + "-site.xml")) for name in ("core", "yarn")]
    filesystem = core["fs.defaultFS"].rstrip("/")
    check(filesystem.startswith("hdfs://"), "Нужен HDFS")
    worker_ips = {socket.gethostbyname(host) for host in (conf / "workers").read_text().split()}
    rm_url = "http://" + yarn["yarn.resourcemanager.webapp.address"].replace("0.0.0.0", env["EDGE_HOST"])
    source = (Path(env["HIVE_TASK_DIR"]) / "data/orders.csv").read_bytes()
    grouped = defaultdict(list)
    for row in csv.reader(io.StringIO(source.decode())):
        _, city, amount, day = row
        grouped[(city.strip().upper(), date.fromisoformat(day).isoformat())].append(Decimal(amount))
    expected = sorted((city, len(amounts), sum(amounts), (sum(amounts) / len(amounts)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP), day)
                      for (city, day), amounts in grouped.items())
    days = sorted({row[4] for row in expected})
    partitions = ["order_date=" + day for day in days]
    table = f"`{database}`.orders_daily"
    base = f"{filesystem}{env['EXTERNAL_DIR']}/{database}/orders_daily"
    input_uri = f"{filesystem}{env['HDFS_ROOT']}/input/{database}/orders.csv"
    run(["bash", Path(env["HIVE_TASK_DIR"]) / "scripts/cluster.sh", "status", env["HIVE_CLUSTER_ENV"]])
    run(["bash", task / "scripts/load-data.sh", env["CLUSTER_ENV"], database], log="load.log")
    check(hdfs("-cat", input_uri, binary=True) == source, "HDFS input отличается от исходного CSV")
    print("CSV загружен в HDFS до запуска Spark")
    first = pipeline("first")
    result = result_check()
    pruning()
    second = pipeline("repeat")
    check(second != first and result_check() == result, "Повтор изменил строки/метаданные или appId")
    check(hdfs("-cat", input_uri, binary=True) == source, "Pipeline изменил HDFS input")
    print("Повторная обработка прошла без дублей")
    print(f"Результат оставлен: {database}.orders_daily; {base}")
    print("Проверка Spark пройдена.")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, OSError, StopIteration, subprocess.TimeoutExpired) as error:
        raise SystemExit(f"Проверка Spark не прошла: {error}")
