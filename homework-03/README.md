# Практическая работа №3: Hive

Hive 4.0.0-alpha-2 работает поверх HDFS и YARN из работ [1](../homework-01/README.md) и [2](../homework-02/README.md). Клиенты подключаются по JDBC к HiveServer2. Metastore запущен отдельно и хранит метаданные в PostgreSQL, без embedded-режима.

| Узел | Сервис | Адрес |
| --- | --- | --- |
| team-30-en | HiveServer2 | JDBC `127.0.0.1:10000`, Web UI `127.0.0.1:10002` |
| team-30-00 | Metastore | `10.30.0.12:9083` |
| team-30-00 | PostgreSQL 16 | `127.0.0.1:5433` |

HS2 обращается к Metastore по `hive.metastore.uris=thrift://team-30-00:9083`. Загрузка выполняется через MapReduce в YARN.

## Развертывание

Нужны готовые HDFS/YARN, Ubuntu 24.04 amd64, Hadoop 3.4.3 и Java 11. На edge требуются Python 3, curl, tar и SSH. Пользователь `team` должен иметь sudo без пароля и SSH-доступ к внутренним узлам с проверенными ключами серверов.

Подключитесь к edge:

```bash
ssh -i ~/.ssh/id_ed25519_team30 team@2.59.83.239
cd ~/data-platforms-course
cp homework-03/cluster.env.example homework-03/cluster.env
```

В `cluster.env` проверьте имена и IP узлов, путь `ADMIN_KEY` на edge и пути Java/Hadoop. Для team-30 подходят значения примера. Имена должны разрешаться и совпадать с `hostname -s`. Все дальнейшие команды, кроме туннеля, выполняются от `team` на edge из корня репозитория.

```bash
bash homework-03/scripts/deploy.sh
bash homework-03/scripts/cluster.sh status
```

Скрипт скачивает Hive, проверяет SHA256, устанавливает его на два узла и создает PostgreSQL, конфиги и службы systemd. Схема Metastore создается через `schematool`, затем проверяется через `-validate`. После запуска скрипт ждет успешного JDBC-запроса `SELECT 1`.

JDBC и Web UI слушают localhost. Для этой учебной установки используются `authentication=NONE` и `doAs=false`, доступ с Mac идет через SSH-туннель. Пароль PostgreSQL генерируется на узле Metastore и в Git не попадает.

## Загрузка данных

```bash
bash homework-03/scripts/load-data.sh
```

Скрипт создает базу `homework03` и две внешние таблицы из [schema.hql](sql/schema.hql): `orders_raw` читает [orders.csv](data/orders.csv) из HDFS, `orders` хранит данные в ORC с партиционированием по `order_date DATE`.

В [load.hql](sql/load.hql) включены динамические партиции:

```sql
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE homework03.orders PARTITION (order_date)
SELECT order_id, city, amount, order_date
FROM homework03.orders_raw;
```

В CSV девять заказов за три даты. Последний столбец определяет партицию. Повторная загрузка перезаписывает эти даты без дублей, остальные даты сохраняются. Перед перезаписью скрипт проверяет схему и расположение таблиц, чтобы не затронуть чужие данные. Загрузчик рассчитан на приложенный учебный CSV.

## Проверка

```bash
bash homework-03/scripts/verify.sh
```

Проверка загружает данные в отдельную базу, сравнивает обе таблицы с CSV, проверяет партиции и два одновременно открытых JDBC-клиента. Затем повторяет загрузку и убеждается, что дубли не появились. Для проверки сохранности после перезапуска Hive/PostgreSQL:

```bash
bash homework-03/scripts/verify.sh homework-03/cluster.env --restart
```

Успех заканчивается строкой `Проверка Hive пройдена.` и кодом 0. База проверки остается для просмотра.

Для ручной проверки запустите в двух терминалах на edge `bash homework-03/scripts/client.sh -n client_1` и ту же команду с `-n client_2`. Не закрывая подключения, выполните в каждом:

```sql
SELECT * FROM homework03.orders;
```

Оба клиента должны получить девять строк. Выход: `!quit`. Проверить партиции и суммы можно так:

```bash
bash homework-03/scripts/client.sh -e 'SHOW PARTITIONS homework03.orders;'
bash homework-03/scripts/client.sh -e 'SELECT order_date, COUNT(*), SUM(amount) FROM homework03.orders GROUP BY order_date ORDER BY order_date;'
```

| Дата партиции | Заказов | Сумма |
| --- | --- | --- |
| 2026-10-01 | 3 | 250.00 |
| 2026-10-02 | 3 | 200.00 |
| 2026-10-03 | 3 | 190.00 |

Результаты запуска на кластере — в [отчете](report.md).

## Управление

`bash homework-03/scripts/cluster.sh` принимает `start`, `stop`, `restart` или `status` и управляет Hive и выделенным PostgreSQL. Перед повторным развертыванием выполните `cluster.sh stop`, затем `deploy.sh`: данные сохранятся. После перезагрузки узлов сначала поднимите HDFS/YARN, затем Hive. Логи: `/var/log/hive-practice3`.

Другой конфиг можно передать аргументом: `deploy.sh /path/cluster.env`, `load-data.sh /path/cluster.env my_orders`, `verify.sh /path/cluster.env --restart`, `cluster.sh status /path/cluster.env`. Для `client.sh` задайте `CLUSTER_ENV=/path/cluster.env`.

На Mac из локальной копии репозитория запустите `bash homework-03/scripts/tunnel.sh`. Откройте [Web UI](http://127.0.0.1:10002); JDBC-адрес — `jdbc:hive2://127.0.0.1:10000/default`. Терминал с туннелем должен оставаться открытым; `Ctrl+C` закрывает соединение.
