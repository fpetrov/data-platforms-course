#!/usr/bin/env python3
import asyncio
import csv
import hashlib
import io
import json
import os
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


def run(args, timeout=60, log=None, extra_env=None):
    result = subprocess.run(["timeout", "-k", "5s", f"{timeout}s", *map(str, args)],
                            capture_output=True, env={**os.environ, **(extra_env or {})})
    if log:
        log.write_bytes(result.stdout + b"\n" + result.stderr)
    detail = f"Полный вывод: {log}\n" if log else ""
    check(result.returncode == 0, f"Команда завершилась с кодом {result.returncode}. {detail}{result.stderr.decode(errors='replace')[-2500:]}")
    return result.stdout


def properties(path):
    return {p.findtext("name"): p.findtext("value") for p in ET.parse(path).getroot().findall("property")}


def api(url):
    with urlopen(url, timeout=5) as response:
        return json.load(response)


def hdfs(*args):
    return run(["sudo", "-u", "hadoop", env["HADOOP_HOME"] + "/bin/hdfs", "--config", env["HADOOP_CONF_DIR"], "dfs", *args])


def rows(text):
    return [[value.strip() for value in row] for row in csv.reader(io.StringIO(text), delimiter="\t")
            if row and any(value.strip() for value in row)]


async def prefect_check(info):
    from prefect.client.orchestration import get_client
    from prefect.client.schemas.filters import FlowRunFilter

    async with get_client() as client:
        flow_id = uuid.UUID(info["flow_run_id"])
        flow = await client.read_flow_run(flow_id)
        definition = await client.read_flow(flow.flow_id)
        check(definition.name == "homework05-orders" and flow.parameters.get("database") == info["database"] and
              flow.state.name == "Completed" and flow.state.is_completed(), "Flow не Completed или относится к другой базе")
        deadline = time.monotonic() + 20
        while True:
            tasks = await client.read_task_runs(flow_run_filter=FlowRunFilter(id={"any_": [flow_id]}))
            if len(tasks) == 2 and all(t.state and t.state.name == "Completed" and t.state.is_completed() for t in tasks):
                break
            check(time.monotonic() < deadline, "В Prefect не появились две Completed задачи")
            await asyncio.sleep(0.5)
        ordered = []
        for prefix in ("load-input-", "process-orders-"):
            matches = [t for t in tasks if t.name.startswith(prefix) and t.flow_run_id == flow_id]
            check(len(matches) == 1, f"Нет отдельной задачи {prefix.rstrip('-')}")
            ordered.append(matches[0])
        check(all(t.start_time and t.end_time for t in ordered) and ordered[0].end_time <= ordered[1].start_time,
              "Задачи не выполнились последовательно")
        print(f"Prefect: flow {flow_id} Completed; load-input → process-orders, обе Completed")


def spark_check(info):
    app = api(rm_url + f"/ws/v1/cluster/apps/{info['app_id']}")["app"]
    check(app["applicationType"] == "SPARK" and app["state"] == "FINISHED" and app["finalStatus"] == "SUCCEEDED" and
          info["tag"] in app.get("applicationTags", "").split(","), "YARN не подтвердил успешный Spark с flow tag")
    config = dict(line.split(None, 1) for line in (Path(env["CONF_DIR"]) / "spark-defaults.conf").read_text().splitlines()
                  if line.strip() and not line.lstrip().startswith("#"))
    events = [json.loads(line) for line in hdfs("-cat", config["spark.eventLog.dir"].rstrip("/") + "/" + info["app_id"]).splitlines()]
    check(any(e["Event"] == "SparkListenerApplicationStart" and e["App ID"] == info["app_id"] for e in events) and
          any(e["Event"] == "SparkListenerApplicationEnd" for e in events), "Нет завершенного event log приложения")
    props = next(e for e in events if e["Event"] == "SparkListenerEnvironmentUpdate")["Spark Properties"]
    check(props.get("spark.master") == "yarn" and props.get("spark.sql.catalogImplementation") == "hive" and
          props.get("spark.hadoop.hive.metastore.uris") == f"thrift://{env['HMS_HOST']}:{env['HMS_PORT']}", "Другой Spark master или Metastore")
    check(any(info["input_uri"] in e.get("physicalPlanDescription", "") and "Scan csv" in e.get("physicalPlanDescription", "") for e in events),
          "Нет плана чтения нашего HDFS CSV")
    workers = {socket.gethostbyname(host) for host in (Path(env["HADOOP_CONF_DIR"]) / "workers").read_text().split()}
    executors = {e["Executor ID"] for e in events if e["Event"] == "SparkListenerExecutorAdded" and e["Executor ID"] != "driver"
                 and socket.gethostbyname(e["Executor Info"]["Host"]) in workers}
    tasks = [e for e in events if e["Event"] == "SparkListenerTaskEnd" and e["Task End Reason"]["Reason"] == "Success"
             and not e["Task Info"]["Failed"] and not e["Task Info"]["Killed"] and e["Task Info"]["Executor ID"] in executors]
    metrics = [e["Task Metrics"] for e in tasks]
    read = sum(m["Input Metrics"]["Bytes Read"] for m in metrics)
    written = sum(m["Shuffle Write Metrics"]["Shuffle Bytes Written"] for m in metrics)
    shuffled = sum(m["Shuffle Read Metrics"]["Remote Bytes Read"] + m["Shuffle Read Metrics"]["Local Bytes Read"] for m in metrics)
    check(tasks and read > 0 and written > 0 and shuffled > 0, "Нет успешных remote tasks, чтения или shuffle")
    print(f"YARN: {info['app_id']}, SPARK FINISHED/SUCCEEDED; tag {info['tag']}")
    print(f"Spark: {len(tasks)} успешных remote tasks; input {read}, shuffle write/read {written}/{shuffled} байт")


def hive_check(info, source):
    groups = defaultdict(list)
    raw = list(csv.reader(io.StringIO(source.decode())))
    check(len(raw) == 9 and {int(r[0]) for r in raw} == set(range(1, 10)), "Другой исходный набор заказов")
    for _, city, amount, day in raw:
        groups[(city.strip().upper(), date.fromisoformat(day).isoformat())].append(Decimal(amount))
    expected = sorted((city, len(values), sum(values), (sum(values) / len(values)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP), day)
                      for (city, day), values in groups.items())
    check(len(expected) == 6 and sum(r[2] for r in expected) == Decimal("640.00"), "Другие исходные агрегаты")
    table = info["table"]
    queries = [f"SELECT city,orders_count,total_amount,avg_amount,order_date FROM {table};", f"SHOW PARTITIONS {table};", f"DESCRIBE FORMATTED {table};"]
    query = "SET hive.query.results.cache.enabled=false; SET hive.fetch.task.conversion=more;\n"
    query += "\n".join(f"SELECT '__check_{i}__';\n{statement}" for i, statement in enumerate(queries))
    output = run(["bash", Path(env["HIVE_TASK_DIR"]) / "scripts/client.sh", "-e", query], timeout=120,
                 extra_env={"CLUSTER_ENV": env["HIVE_CLUSTER_ENV"]}).decode()
    sections = [output.split(f"__check_{i}__\n", 1)[1].split(f"__check_{i + 1}__\n", 1)[0] for i in range(3)]
    actual = sorted((r[0], int(r[1]), Decimal(r[2]), Decimal(r[3]), date.fromisoformat(r[4]).isoformat()) for r in rows(sections[0]))
    check(actual == expected, "Hive-строки отличаются от независимого расчета Decimal")
    check(sorted(r[0] for r in rows(sections[1])) == ["order_date=" + day for day in sorted({r[4] for r in expected})], "Другие партиции Hive")
    metadata = rows(sections[2])
    values = {r[0].rstrip(":"): r[1] for r in metadata if len(r) >= 2 and r[0]}
    check(values.get("Table Type") == "EXTERNAL_TABLE" and values.get("InputFormat") == "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat" and
          values.get("Location") == filesystem + env["EXTERNAL_DIR"] + "/" + info["database"] + "/orders_daily" and
          any(r[:3] == ["", "course.dataset", "homework04.orders.v1"] for r in metadata), "Другой storage или метка таблицы")
    print(f"Hive: 6 точных групп, 9 заказов, сумма 640.00; EXTERNAL ORC, 3 даты; таблица {table}")


def main():
    global env, filesystem, rm_url
    env = os.environ
    task = Path(__file__).resolve().parent.parent
    conf = Path(env["HADOOP_CONF_DIR"])
    core, yarn = [properties(conf / (name + "-site.xml")) for name in ("core", "yarn")]
    filesystem = core["fs.defaultFS"].rstrip("/")
    rm_url = "http://" + yarn["yarn.resourcemanager.webapp.address"].replace("0.0.0.0", env["EDGE_HOST"])
    database = "homework05_check_" + uuid.uuid4().hex[:10]
    evidence = Path(tempfile.mkdtemp(prefix="prefect-check-"))
    output = run(["bash", task / "scripts/run-flow.sh", database], timeout=1000, log=evidence / "flow.log").decode()
    results = [json.loads(line) for line in output.splitlines() if line.startswith('{"flow_run_id":')]
    check(len(results) == 1, "Flow не вернул единственный JSON-результат")
    info = results[0]
    check(info["database"] == database and info["table"] == database + ".orders_daily" and
          info["tag"] == "hw05-" + uuid.UUID(info["flow_run_id"]).hex and
          info["input_uri"] == f"{filesystem}{env['HDFS_ROOT']}/input/{database}/orders.csv", "Результат относится к другому запуску")
    asyncio.run(asyncio.wait_for(prefect_check(info), timeout=95))
    source = (Path(env["HIVE_TASK_DIR"]) / "data/orders.csv").read_bytes()
    actual = hdfs("-cat", info["input_uri"])
    digest = hashlib.sha256(actual).hexdigest()
    check(actual == source and digest == info["input_sha256"], "HDFS input отличается от исходника или flow SHA256")
    print(f"HDFS input: {len(actual)} байт, SHA256 {digest}")
    spark_check(info)
    hive_check(info, source)
    print("Проверка Prefect пройдена.")


if __name__ == "__main__":
    try:
        main()
    except (ValueError, KeyError, IndexError, OSError, StopIteration, subprocess.TimeoutExpired, TimeoutError) as error:
        raise SystemExit(f"Проверка Prefect не прошла: {error}")
