#!/usr/bin/env python3
import csv
import re
import sys
from collections import Counter, defaultdict
from datetime import date
from decimal import Decimal
from pathlib import Path


def check_input(path):
    counts = Counter()
    totals = defaultdict(Decimal)
    cities = defaultdict(Decimal)
    ids = []
    with Path(path).open(newline="", encoding="utf-8") as source:
        for row in csv.reader(source):
            if len(row) != 4:
                raise ValueError("В CSV должно быть четыре поля.")
            number, city, amount, day = row
            if not re.fullmatch(r"[1-9][0-9]*", number) or city not in {"Moscow", "Kazan"}:
                raise ValueError("Неверный номер заказа или город.")
            if not re.fullmatch(r"[0-9]{1,8}\.[0-9]{2}", amount) or Decimal(amount) <= 0:
                raise ValueError("Неверная сумма заказа.")
            if date.fromisoformat(day).isoformat() != day:
                raise ValueError("Неверная дата заказа.")
            ids.append(int(number))
            counts[day] += 1
            totals[day] += Decimal(amount)
            cities[city] += Decimal(amount)
    if sorted(ids) != list(range(1, 10)) or counts != {
        "2026-10-01": 3, "2026-10-02": 3, "2026-10-03": 3
    }:
        raise ValueError("Ожидаются девять заказов за три дня.")
    if totals != {"2026-10-01": Decimal(250), "2026-10-02": Decimal(200), "2026-10-03": Decimal(190)}:
        raise ValueError("Суммы по дням не совпали.")
    if cities != {"Moscow": Decimal(390), "Kazan": Decimal(250)}:
        raise ValueError("Суммы по городам не совпали.")


def metadata(path):
    with Path(path).open(newline="", encoding="utf-8") as source:
        rows = [[value.strip() for value in row] for row in csv.reader(source, delimiter="\t")]
    fields = {row[0]: row[1] for row in rows if len(row) > 1 and row[0]}
    parameters = {}
    section = ""
    for row in rows:
        if row and row[0] in {"Table Parameters:", "Storage Desc Params:"}:
            section = row[0]
        elif len(row) > 2 and not row[0] and row[1] != "NULL" and section:
            parameters[(section, row[1])] = row[2]
        else:
            section = ""
    return rows, fields, parameters


def check_table(path, uri, table):
    rows, fields, parameters = metadata(path)
    expected = [("order_id", "int"), ("city", "string"), ("amount", "decimal(10,2)"), ("order_date", "date")]
    columns = []
    for row in rows:
        if row and row[0] == "# Detailed Table Information":
            break
        if len(row) > 1 and row[0] and not row[0].startswith("#"):
            columns.append(tuple(row[:2]))
    partitioned = any(row and row[0] == "# Partition Information" for row in rows)
    file_format = "org.apache.hadoop.mapred.TextInputFormat" if table == "orders_raw" else "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"
    serde = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe" if table == "orders_raw" else "org.apache.hadoop.hive.ql.io.orc.OrcSerde"
    if fields.get("Table Type:") != "EXTERNAL_TABLE" or fields.get("Location:", "").rstrip("/") != uri:
        raise ValueError(f"У {table} другой тип или LOCATION. Загрузка отменена.")
    if (columns != expected or partitioned != (table == "orders")
            or fields.get("InputFormat:") != file_format or fields.get("SerDe Library:") != serde):
        raise ValueError(f"У {table} другая схема или формат. Загрузка отменена.")
    if parameters.get(("Table Parameters:", "course.dataset")) != "homework03.orders.v1":
        raise ValueError(f"Таблица {table} не принадлежит примеру. Загрузка отменена.")
    if table == "orders_raw" and parameters.get(("Storage Desc Params:", "field.delim")) != ",":
        raise ValueError("У orders_raw другой разделитель полей. Загрузка отменена.")


def check_partitions(path, *uris):
    rows, _, _ = metadata(path)
    locations = [row[1].rstrip("/") for row in rows if len(row) > 1 and row[0] == "Location:"]
    formats = [row[1] for row in rows if len(row) > 1 and row[0] == "InputFormat:"]
    if sorted(locations) != sorted(uris) or formats != ["org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"] * len(uris):
        raise ValueError("У целевой партиции другой LOCATION или формат. Загрузка отменена.")


try:
    if sys.argv[1] == "input":
        check_input(sys.argv[2])
    elif sys.argv[1] == "table":
        check_table(*sys.argv[2:])
    elif sys.argv[1] == "partitions":
        check_partitions(*sys.argv[2:])
    else:
        raise ValueError("Неизвестная проверка.")
except (ValueError, OSError) as error:
    raise SystemExit(str(error))
