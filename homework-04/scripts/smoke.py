import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

parser = argparse.ArgumentParser()
parser.add_argument("database")
parser.add_argument("location")
args = parser.parse_args()

spark = SparkSession.builder.appName(args.database).enableHiveSupport().getOrCreate()
try:
    print("SPARK_VERSION=" + spark.version, flush=True)
    print("HADOOP_CLIENT_VERSION=" + spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion(), flush=True)
    print("SPARK_APPLICATION_ID=" + spark.sparkContext.applicationId, flush=True)
    print("HIVE_METASTORE_URI=" + spark.sparkContext._jsc.hadoopConfiguration().get("hive.metastore.uris"), flush=True)
    spark.sql(f"CREATE DATABASE `{args.database}` LOCATION '{args.location}/database'")
    frame = spark.range(1, 4).select(col("id").cast("int").alias("value"), lit("2026-10-23").alias("day"))
    frame.write.format("orc").option("path", args.location + "/records").partitionBy("day").saveAsTable(args.database + ".records")
    assert spark.table(args.database + ".records").count() == 3
    print("SPARK_SMOKE_ROWS=3", flush=True)
finally:
    spark.stop()
