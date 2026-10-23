# Практическая работа №4: Spark

SparkSession работает под существующим YARN, читает предварительно загруженный CSV из HDFS, преобразует заказы и сохраняет итоговую таблицу с партициями по дате. После остановки Spark результат читает обычный Beeline из работы 3.

Драйвер запускается на edge в режиме `client`, executors — в контейнерах YARN на внутренних узлах. Используется общий удаленный Metastore из [работы 3](../homework-03/README.md).

| Компонент | Версия и роль |
| --- | --- |
| Spark | 3.5.7, установка на edge |
| Hadoop внутри Spark | Клиент 3.3.4 из дистрибутива Spark |
| Существующий Hadoop | Серверы 3.4.3, HDFS и YARN из работ 1–2 |
| Hive внутри Spark | Встроенный клиент Metastore 2.3.9 |
| Общий Hive Metastore | Сервер 4.0.0-alpha-2 на `team-30-00:9083` |

В этой среде проверяется путь Spark → общий Metastore → Beeline с указанными версиями.

## Подготовка

Нужны запущенные HDFS, YARN и Hive из работ [1](../homework-01/README.md), [2](../homework-02/README.md) и [3](../homework-03/README.md), четыре узла Ubuntu 24.04, Java 11 и Python 3. На edge нужны curl, tar, доступ к архиву Apache и sudo без пароля для `team`.

Подключитесь с Mac к edge:

```bash
ssh -i ~/.ssh/id_ed25519_team30 team@2.59.83.239
```

Все дальнейшие команды выполняются от `team` на edge из корня репозитория:

```bash
cd ~/data-platforms-course
git pull --ff-only
cp homework-04/cluster.env.example homework-04/cluster.env
```

В `cluster.env` задаются edge, Metastore, Java/Python, каталоги, память и пути HDFS. `HADOOP_CONF_DIR` указывает на конфиги работы 2. `HIVE_TASK_DIR` — на папку работы 3 в этой копии репозитория, `HIVE_CLUSTER_ENV` — на ее действующий `cluster.env`. Если репозиторий находится не в `~/data-platforms-course`, исправьте оба пути. Локальные `cluster.env` исключены из Git.

## Установка

```bash
bash homework-04/scripts/deploy.sh
```

Скрипт проверяет лимиты YARN и доступ к HDFS, скачивает Spark и проверяет SHA512 архива. Дистрибутив устанавливается в `/opt/spark-3.5.7-bin-hadoop3`, ссылка `/opt/spark-practice4` указывает на него. Конфиги создаются в `/etc/spark-practice4`:

| Файл | Настройка |
| --- | --- |
| `spark-defaults.conf` | `master=yarn`, режим `client`, адрес драйвера, ресурсы executors/AM, shuffle, общий Metastore, HDFS event logs |
| `hive-site.xml` | `hive.metastore.uris=thrift://team-30-00:9083` |

Заданы два executors: у каждого 1 core, heap 512 МиБ и overhead 384 МиБ. Драйвер получает 512 МиБ, ApplicationMaster — heap 256 МиБ и overhead 384 МиБ. Динамическое выделение отключено, число shuffle-партиций — 2. Параметры меняются в `cluster.env` до установки.

Spark использует свои библиотеки: `spark.yarn.populateHadoopClasspath=false`, `spark.sql.hive.metastore.jars=builtin`, версия клиента Metastore — `2.3.9`. Репликация staging-файлов берется из текущего HDFS; для team-30 это 3. Spark UI отключен.

Локальные временные файлы находятся в `/srv/spark-practice4/tmp`, каталог логов — `/var/log/spark-practice4`. В HDFS под `/user/hadoop/homework-04` используются `events`, `staging`, `warehouse`, `external` и входной каталог `input`.

## Предварительная загрузка в HDFS

```bash
bash homework-04/scripts/load-data.sh
```

Скрипт проверяет `homework-03/data/orders.csv`, копирует его в HDFS и выводит SHA256. При стандартном конфиге входной URI — `hdfs://team-30-nn:9000/user/hadoop/homework-04/input/homework04/orders.csv`. В нем девять заказов за `2026-10-01`–`2026-10-03` на сумму 640.00. Этот шаг выполняется до создания SparkSession.

## Обработка и таблица

```bash
bash homework-04/scripts/run-job.sh
```

Через `submit.sh` приложение [orders.py](jobs/orders.py) запускается от `hadoop` под YARN. Оно создает `SparkSession` с `master("yarn")` и `enableHiveSupport()`, затем читает именно загруженный HDFS CSV через `spark.read.csv`.

`trim` убирает пробелы, `upper` приводит города к верхнему регистру. Номер заказа приводится к `INT`, сумма — к `DECIMAL(10,2)`, дата — к `DATE`. `groupBy(order_date, city)` считает `count`, `sum` и `avg`. Получаются шесть строк для двух городов и трех дат:

| Поле | Тип |
| --- | --- |
| `city` | STRING |
| `orders_count` | BIGINT |
| `total_amount`, `avg_amount` | DECIMAL(12,2) |
| `order_date` | DATE, колонка партиционирования |

При первом запуске `partitionBy("order_date").saveAsTable(...)` записывает ORC и регистрирует внешнюю таблицу `homework04.orders_daily` в общем Metastore. Файлы находятся в `$EXTERNAL_DIR/homework04/orders_daily` внутри HDFS; каждая дата имеет отдельный каталог `order_date=<дата>`.

Повторный запуск использует динамический `insertInto` с перезаписью трех дат примера и сохраняет остальные даты. Перед записью проверяются схема и формат таблицы, метки базы/таблицы и `LOCATION` базы, таблицы и целевых партиций. Занятые незарегистрированные каталоги и чужие объекты приводят к отказу.

В `finally` вызывается `spark.stop()`. После его завершения `run-job.sh` запускает стандартный Beeline из работы 3 с отключенным кешем результатов и выводит строки таблицы. Для ручного чтения в стандартной структуре репозитория:

```bash
CLUSTER_ENV=homework-03/cluster.env bash homework-03/scripts/client.sh \
    -e 'SET hive.query.results.cache.enabled=false; SELECT city, orders_count, total_amount, avg_amount, order_date FROM homework04.orders_daily ORDER BY order_date, city;'
```

При другом расположении работы 3 используйте путь к клиенту из `HIVE_TASK_DIR` и конфиг `HIVE_CLUSTER_ENV`.

## Проверка

```bash
bash homework-04/scripts/verify.sh homework-04/cluster.env
```

Проверка создает отдельную базу `homework04_check_<суффикс>`, заранее загружает CSV в HDFS и сравнивает его с исходником побайтно. Затем дважды запускает обработку. Оба приложения должны завершиться в YARN как `SPARK FINISHED/SUCCEEDED`.

По event log проверяются Hive catalog, чтение входного HDFS CSV, aggregate/shuffle, executors на внутренних узлах, успешные удаленные tasks и stages, ненулевые метрики чтения и shuffle. Через Beeline данные сравниваются с независимым расчетом Python Decimal; проверяются шесть групп, типы, внешняя ORC-таблица, три DATE-партиции и их `LOCATION`. `EXPLAIN DEPENDENCY` должен сократить три партиции до одной при фильтре по дате.

После повтора сравниваются строки и метаданные, проверяются отсутствие дублей, исходный CSV, состояние и PID HDFS/YARN/Hive/PostgreSQL, файл работы 1 и таблицы работы 3 с SHA256 файлов ORC. Подробные логи сохраняются на edge в каталоге `/tmp/spark-verify-...`; его путь выводится в начале. База и результат остаются для просмотра. Успех заканчивается строкой `Проверка Spark пройдена.` и кодом 0.

Для другой базы передайте одно имя обоим повседневным шагам:

```bash
bash homework-04/scripts/load-data.sh homework-04/cluster.env my_orders
bash homework-04/scripts/run-job.sh homework-04/cluster.env my_orders
```

Другой конфиг также передается аргументом `deploy.sh` и `verify.sh`. Для прямого вызова `submit.sh` задайте `CLUSTER_ENV=/path/cluster.env`. После перезагрузки узлов поднимите HDFS, YARN и Hive по работам 1–3, затем запускайте приложение.

Фактические результаты записаны в [отчете](report.md).
