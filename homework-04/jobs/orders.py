#!/usr/bin/env python3
import argparse
import re
from decimal import Decimal

from pyspark.sql import SparkSession, functions as F


parser = argparse.ArgumentParser()
parser.add_argument("--root", required=True)
parser.add_argument("--external", required=True)
parser.add_argument("--database", default="homework04")
args = parser.parse_args()
if not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", args.database) or args.database in {"default", "sys", "information_schema"}:
    raise SystemExit("Неверное имя учебной базы.")
for uri in (args.root, args.external):
    if not re.fullmatch(r"hdfs://[a-zA-Z0-9.-]+(?::[0-9]+)?/[a-zA-Z0-9_-]+(?:/[a-zA-Z0-9_-]+)*", uri):
        raise SystemExit("Неверный URI HDFS.")
if not args.external.startswith(args.root + "/"):
    raise SystemExit("Внешние таблицы должны находиться внутри каталога работы.")

database = args.database
db_uri = f"{args.external}/{database}"
output = f"{db_uri}/orders_daily"
input_uri = f"{args.root}/input/{database}/orders.csv"
table = f"`{database}`.orders_daily"
marker = "homework04.orders.v1"
days = ("2026-10-01", "2026-10-02", "2026-10-03")
columns = ("city", "orders_count", "total_amount", "avg_amount", "order_date")
schema = [("city", "string"), ("orders_count", "bigint"), ("total_amount", "decimal(12,2)"),
          ("avg_amount", "decimal(12,2)"), ("order_date", "date")]
spark = (SparkSession.builder.appName(f"homework04-{database}").master("yarn")
         .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
         .config("spark.sql.ansi.enabled", "true").enableHiveSupport().getOrCreate())
spark.sparkContext.setLogLevel("WARN")


def describe(sql, stop_at=None):
    info = {}
    for row in spark.sql(sql).collect():
        if row[0] == stop_at:
            break
        if row[0] and row[1]:
            info[row[0].strip()] = row[1].strip()
    return info


try:
    if spark.sparkContext.master != "yarn":
        raise ValueError("Ожидается Spark на YARN.")
    hadoop = spark.sparkContext._jsc.hadoopConfiguration()
    if not hadoop.get("hive.metastore.uris", "").startswith("thrift://"):
        raise ValueError("Ожидается отдельный Metastore Hive.")
    path = spark._jvm.org.apache.hadoop.fs.Path
    fs = path(args.root).getFileSystem(hadoop)

    def occupied(uri):
        return fs.exists(path(uri)) and (not fs.isDirectory(path(uri)) or len(fs.listStatus(path(uri))) > 0)

    if not fs.isFile(path(input_uri)):
        raise ValueError("Сначала загрузите CSV в HDFS.")
    raw = (spark.read.schema("order_id STRING, city STRING, amount STRING, order_date STRING")
           .option("mode", "FAILFAST").csv(input_uri))
    orders = raw.select(F.trim("order_id").cast("int").alias("order_id"),
                        F.upper(F.trim("city")).alias("city"),
                        F.trim("amount").cast("decimal(10,2)").alias("amount"),
                        F.to_date(F.trim("order_date"), "yyyy-MM-dd").alias("order_date"))
    rows = orders.limit(10).collect()
    if (len(rows) != 9 or any(any(value is None for value in row) for row in rows)
            or sorted(row.order_id for row in rows) != list(range(1, 10))
            or {str(row.order_date) for row in rows} != set(days)
            or {row.city for row in rows} != {"KAZAN", "MOSCOW"}
            or any(row.amount <= 0 for row in rows) or sum(row.amount for row in rows) != Decimal("640.00")):
        raise ValueError("Ожидаются девять заказов примера за три дня на сумму 640.00.")
    result = (orders.groupBy("order_date", "city")
              .agg(F.count("*").alias("orders_count"),
                   F.sum("amount").cast("decimal(12,2)").alias("total_amount"),
                   F.avg("amount").cast("decimal(12,2)").alias("avg_amount"))
              .select(*columns))
    if result.count() != 6:
        raise ValueError("Ожидаются шесть строк результата.")

    exists = spark.catalog.databaseExists(database)
    if exists:
        info = describe(f"DESCRIBE DATABASE EXTENDED `{database}`")
        if info.get("Location", "").rstrip("/") != db_uri or f"(course.dataset,{marker})" not in info.get("Properties", "").replace(" ", ""):
            raise ValueError("У существующей базы другой LOCATION или метка набора.")
    elif occupied(db_uri):
        raise ValueError("Каталог базы уже занят без регистрации в Metastore.")
    table_exists = exists and spark.catalog.tableExists(f"{database}.orders_daily")
    if table_exists:
        info = describe(f"DESCRIBE FORMATTED {table}")
        props = spark.sql(f"SHOW TBLPROPERTIES {table} ('course.dataset')").first()
        if (info.get("Type") != "EXTERNAL" or info.get("Provider", "").lower() != "orc"
                or info.get("InputFormat") != "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"
                or info.get("Serde Library") != "org.apache.hadoop.hive.ql.io.orc.OrcSerde"
                or info.get("Location", "").rstrip("/") != output or props[-1] != marker):
            raise ValueError("У существующей таблицы другой формат, LOCATION или метка набора.")
        partition_columns = [col.name for col in spark.catalog.listColumns(f"{database}.orders_daily") if col.isPartition]
        if spark.table(table).dtypes != schema or partition_columns != ["order_date"]:
            raise ValueError("У существующей таблицы другая схема или партиции.")
        partitions = {row[0] for row in spark.sql(f"SHOW PARTITIONS {table}").collect()}
        for day in days:
            partition_uri = f"{output}/order_date={day}"
            if f"order_date={day}" in partitions:
                # Spark дописывает storage таблицы после свойств партиции.
                info = describe(f"DESCRIBE FORMATTED {table} PARTITION (order_date='{day}')", "# Storage Information")
                if (info.get("Location", "").rstrip("/") != partition_uri
                        or info.get("InputFormat") != "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"
                        or info.get("Serde Library") != "org.apache.hadoop.hive.ql.io.orc.OrcSerde"):
                    raise ValueError("У целевой партиции другой LOCATION или формат.")
            elif occupied(partition_uri):
                raise ValueError("Каталог целевой даты уже занят без регистрации партиции.")
    elif occupied(output):
        raise ValueError("Каталог таблицы уже занят без регистрации в Metastore.")

    if not exists:
        spark.sql(f"CREATE DATABASE `{database}` LOCATION '{db_uri}' WITH DBPROPERTIES ('course.dataset'='{marker}')")
    if table_exists:
        result.write.mode("overwrite").insertInto(table)
    else:
        (result.write.format("orc").mode("errorifexists").partitionBy("order_date")
         .option("path", output).saveAsTable(table))
        spark.sql(f"ALTER TABLE {table} SET TBLPROPERTIES ('course.dataset'='{marker}')")
    print(f"Готово: {database}.orders_daily, 6 строк в трех партициях; приложение {spark.sparkContext.applicationId}.")
finally:
    spark.stop()
