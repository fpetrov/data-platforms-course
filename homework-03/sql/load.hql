SET hive.execution.engine=mr;
SET hive.exec.mode.local.auto=false;
SET hive.fetch.task.conversion=none;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=3;
SET hive.exec.max.dynamic.partitions.pernode=3;

INSERT OVERWRITE TABLE `${hivevar:database}`.orders PARTITION (order_date)
SELECT order_id, city, amount, order_date
FROM `${hivevar:database}`.orders_raw;

SHOW PARTITIONS `${hivevar:database}`.orders;
