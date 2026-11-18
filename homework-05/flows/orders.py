#!/usr/bin/env python3
import argparse
import json
import os
import re
import subprocess
from pathlib import Path

from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE
from prefect.context import get_run_context


def run_script(script, config, database, flow_id, extra_env=None):
    directory = Path(os.environ["PREFECT_HOME"]) / "runs" / flow_id
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    result = subprocess.run(["bash", str(Path(os.environ["SPARK_TASK_DIR"]) / "scripts" / script), config, database],
                            capture_output=True, text=True, timeout=900,
                            env={**os.environ, **(extra_env or {})})
    log = directory / f"{script}.txt"
    log.write_text(result.stdout + "\n" + result.stderr)
    if result.returncode:
        raise RuntimeError(f"{script}: код {result.returncode}. Полный вывод: {log}\n"
                           f"{result.stdout[-2500:]}\n{result.stderr[-1500:]}")
    return result.stdout


@task(name="load-input", cache_policy=NO_CACHE, retries=0)
def load_input(config, database, flow_id):
    output = run_script("load-data.sh", config, database, flow_id)
    uri = re.search(r"^Входной файл загружен: (hdfs://\S+)$", output, re.MULTILINE)
    digest = re.search(r"^([0-9a-f]{64})  -$", output, re.MULTILINE)
    if not uri or not digest or not uri[1].endswith(f"/homework-05/input/{database}/orders.csv"):
        raise ValueError("Не подтверждена загрузка CSV в отдельный каталог работы 5.")
    get_run_logger().info("Вход загружен: %s; SHA256 %s", uri[1], digest[1])
    return {"input_uri": uri[1], "input_sha256": digest[1]}


@task(name="process-orders", cache_policy=NO_CACHE, retries=0)
def process_orders(config, database, flow_id, tag, source):
    get_run_logger().info("Запускаю Spark: %s; вход %s", tag, source["input_uri"])
    output = run_script("run-job.sh", config, database, flow_id, {"SPARK_YARN_TAGS": tag})
    success = re.findall(rf"^Готово: {re.escape(database)}\.orders_daily, 6 строк в трех партициях; "
                         r"приложение (application_\d+_\d+)\.$", output, re.MULTILINE)
    if len(success) != 1:
        raise ValueError("Не получено подтверждение завершения одного Spark-приложения.")
    get_run_logger().info("Spark завершен: %s; результат прочитан через Beeline", success[0])
    return success[0]


@flow(name="homework05-orders", retries=0)
def orders_flow(config, database="homework05"):
    if len(database) > 63 or not re.fullmatch(r"homework05(?:_[a-z0-9_]+)?", database):
        raise ValueError("Используйте homework05 или homework05_<суффикс>.")
    flow_id = str(get_run_context().flow_run.id)
    tag = "hw05-" + flow_id.replace("-", "")
    source = load_input(config, database, flow_id)
    app_id = process_orders(config, database, flow_id, tag, source)
    return {"flow_run_id": flow_id, "tag": tag, "app_id": app_id, "database": database,
            **source, "table": f"{database}.orders_daily"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--database", default="homework05")
    args = parser.parse_args()
    result = orders_flow(str(Path(args.config).resolve()), args.database)
    print(json.dumps(result, ensure_ascii=False))
