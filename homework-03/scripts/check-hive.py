#!/usr/bin/env python3
import argparse
import csv
import io
import json
import os
import queue
import re
import shlex
import subprocess
import threading
import time
import uuid
from datetime import date
from decimal import Decimal
from pathlib import Path
from urllib.request import urlopen


def check(condition, message):
    if not condition:
        raise ValueError(message)


def run(args, timeout=60):
    result = subprocess.run(["timeout", "-k", "5s", f"{timeout}s", *map(str, args)],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode:
        raise ValueError(f"Команда завершилась с кодом {result.returncode}: {shlex.join(map(str, args[:3]))}\n{result.stderr[-5000:]}")
    return result


def sql(query):
    return run(["bash", task / "scripts/client.sh", "-e",
                "SET hive.fetch.task.conversion=more; " + query]).stdout


def sections(queries):
    statement = "\n".join(f"SELECT '__section_{i}__';\n{query}" for i, query in enumerate(queries))
    output = sql(statement)
    result = []
    for i in range(len(queries)):
        marker = f"__section_{i}__\n"
        check(marker in output, f"Нет результата SQL-блока {i}")
        segment = output.split(marker, 1)[1]
        result.append(segment.split(f"__section_{i + 1}__\n", 1)[0])
    return result


def sessions():
    with urlopen(web_url + "/api/v1/sessions", timeout=5) as response:
        return json.load(response)


def concurrency():
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
                check(client["process"].poll() is None, "Клиент завершился до проверки: " + "".join(client["errors"])[-2000:])
                try:
                    if client["queue"].get(timeout=0.2) == client["name"] + "_ready":
                        ready.add(client["name"])
                except queue.Empty:
                    pass
        check(len(ready) == 2, "Не дождались двух JDBC-клиентов\n" +
              "".join(line for client in clients for line in client["errors"])[-3000:])
        current = sessions()
        ids = []
        for client in clients:
            selected = [item["sessionId"] for item in current if item["username"] == client["name"]]
            check(len(selected) == 1, "Нет отдельной живой сессии клиента")
            ids.extend(selected)
            rows = [line.strip() for line in client["lines"] if re.fullmatch(re.escape(client["name"]) + r":[0-9]+", line.strip())]
            check(sorted(rows) == sorted(f"{client['name']}:{row[0]}" for row in expected), "Клиент прочитал не все заказы")
            print(f"JDBC: {client['name']}, sessionId={selected[0]}, прочитано {len(rows)} строк")
        check(len(set(ids)) == 2, "Совпали JDBC sessionId")
        for client in clients:
            client["process"].stdin.write("!quit\n")
            client["process"].stdin.flush()
            client["process"].stdin.close()
        for client in clients:
            check(client["process"].wait(timeout=20) == 0, "JDBC-клиент завершился с ошибкой")
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
    print(f"Данные совпали с CSV: {len(expected)} строк, {len(days)} партиции")


def pruning():
    results = sections([f"EXPLAIN DEPENDENCY SELECT city FROM `{database}`.orders{predicate};"
                        for predicate in ("", f" WHERE order_date='{days[0]}'")])
    selected = []
    for output in results:
        plan = json.loads(output)
        selected.append({part["partitionName"] for part in plan["input_partitions"]})
    check(selected[0] == {f"{database}@orders@order_date={day}" for day in days}, "План содержит неверные партиции")
    check(selected[1] == {f"{database}@orders@order_date={days[0]}"}, "Фильтр не отсек лишние партиции")
    print(f"Фильтр по {days[0]} выбирает одну партицию")


def load():
    result = run(["bash", task / "scripts/load-data.sh", env["CLUSTER_ENV"], database], timeout=300)
    print(result.stdout.strip())


env = os.environ
task = Path(__file__).resolve().parent.parent
parser = argparse.ArgumentParser()
parser.add_argument("--restart", action="store_true")
args = parser.parse_args()
try:
    suffix = uuid.uuid4().hex[:10]
    database = "homework03_check_" + suffix
    web_url = f"http://127.0.0.1:{env['HS2_WEB_PORT']}"
    with (task / "data/orders.csv").open(newline="") as source:
        expected = sorted((int(row[0]), row[1], Decimal(row[2]), date.fromisoformat(row[3]).isoformat()) for row in csv.reader(source))
    days = sorted({row[3] for row in expected})
    run(["bash", task / "scripts/cluster.sh", "status", env["CLUSTER_ENV"]])
    print(f"База проверки: {database}")
    load()
    data_check()
    concurrency()
    pruning()
    print("Повторная загрузка")
    load()
    data_check()
    if args.restart:
        print("Перезапуск Hive и PostgreSQL")
        run(["bash", task / "scripts/cluster.sh", "restart", env["CLUSTER_ENV"]], timeout=300)
        data_check()
    print(f"Результат оставлен: {database}.orders")
    print("Проверка Hive пройдена.")
except (ValueError, KeyError, OSError, subprocess.TimeoutExpired) as error:
    raise SystemExit(f"Проверка Hive не прошла: {error}")
