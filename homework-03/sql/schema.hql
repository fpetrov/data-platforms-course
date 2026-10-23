CREATE DATABASE IF NOT EXISTS `${hivevar:database}`;

CREATE EXTERNAL TABLE IF NOT EXISTS `${hivevar:database}`.orders_raw (
    order_id INT,
    city STRING,
    amount DECIMAL(10,2),
    order_date DATE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${hivevar:raw_uri}'
TBLPROPERTIES ('course.dataset'='homework03.orders.v1');

CREATE EXTERNAL TABLE IF NOT EXISTS `${hivevar:database}`.orders (
    order_id INT,
    city STRING,
    amount DECIMAL(10,2)
)
PARTITIONED BY (order_date DATE)
STORED AS ORC
LOCATION '${hivevar:orders_uri}'
TBLPROPERTIES ('orc.compress'='ZLIB', 'course.dataset'='homework03.orders.v1');
