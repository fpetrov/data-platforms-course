# Практическая работа №4: Spark

Spark 3.5.7 запускается под YARN, читает CSV из HDFS и сохраняет результат в партиционированную таблицу. Используется общий удаленный Metastore Hive из [работы 3](../homework-03/README.md). После остановки Spark таблицу читает обычный Beeline.

## Подготовка и установка

Нужны работающие HDFS, YARN и Hive из работ [1](../homework-01/README.md), [2](../homework-02/README.md) и [3](../homework-03/README.md), Ubuntu 24.04, Java 11 и Python 3. На edge нужны curl, tar, доступ к архиву Apache и sudo без пароля для `team`.

Подключитесь к edge:

```bash
ssh -i ~/.ssh/id_ed25519_team30 team@2.59.83.239
cd ~/data-platforms-course
cp homework-04/cluster.env.example homework-04/cluster.env
```

В `cluster.env` проверьте имена узлов, пути Java/Hadoop и параметры памяти. `HIVE_TASK_DIR` должен указывать на папку работы 3, `HIVE_CLUSTER_ENV` — на ее действующий `cluster.env`. Для team-30 подходят значения примера. Дальнейшие команды выполняются от `team` на edge из корня репозитория.

`HADOOP_CONF_DIR` указывает на конфиги кластера из работы 2: `core-site.xml` задает адрес HDFS, `yarn-site.xml` — ResourceManager. Эти конфиги передаются в Spark при запуске.

```bash
bash homework-04/scripts/deploy.sh
```

Скрипт скачивает Spark, проверяет SHA512 и создает конфиги в `/etc/spark-practice4`. Драйвер работает на edge в режиме `client`, два executors — в контейнерах YARN. Адрес Metastore задается в `hive-site.xml`; Spark использует свои клиентские библиотеки Hadoop и Hive.

## Загрузка и обработка

Сначала загрузите исходный [orders.csv](../homework-03/data/orders.csv) в HDFS, затем запустите приложение:

```bash
bash homework-04/scripts/load-data.sh
bash homework-04/scripts/run-job.sh
```

Загрузка выполняется до создания SparkSession. Приложение [orders.py](jobs/orders.py) создает сессию с `master("yarn")` и `enableHiveSupport()`, затем читает HDFS CSV через `spark.read.csv`. Оно убирает пробелы, приводит города к верхнему регистру, номер заказа — к INT, сумму — к DECIMAL, дату — к DATE. Затем группирует заказы по дате и городу, считает количество, сумму и среднее.

`partitionBy("order_date").saveAsTable(...)` записывает внешнюю ORC-таблицу `homework04.orders_daily` в общий Metastore. Из девяти заказов получаются шесть строк в трех партициях по дате. Столбцы результата: `city`, `orders_count`, `total_amount`, `avg_amount`, `order_date`.

Повторный запуск перезаписывает три даты примера без дублей, остальные даты сохраняются. Перед записью приложение проверяет схему и расположение существующих объектов, чтобы не перезаписать чужие данные. Пример рассчитан на приложенный CSV.

В конце вызывается `spark.stop()`. После завершения приложения `run-job.sh` читает результат через Beeline. Для ручного чтения:

```bash
CLUSTER_ENV=homework-03/cluster.env bash homework-03/scripts/client.sh \
    -e 'SET hive.query.results.cache.enabled=false; SELECT city, orders_count, total_amount, avg_amount, order_date FROM homework04.orders_daily ORDER BY order_date, city;'
```

## Проверка

```bash
bash homework-04/scripts/verify.sh
```

Проверка создает отдельную базу `homework04_check_<суффикс>` и заранее загружает CSV в HDFS. Затем дважды запускает обработку и через Beeline сравнивает результат с расчетом по исходному CSV. Проверяются схема ORC-таблицы, партиции и отсутствие дублей. Фильтр по одной дате должен выбирать одну партицию.

Через YARN и event log проверяются успешное завершение приложений и выполнение задач на worker-узлах. Входной файл после обработки должен сохраниться. Логи остаются в `/tmp/spark-verify-...`, база — для просмотра. Успех заканчивается строкой `Проверка Spark пройдена.` и кодом 0.

Результаты запуска на кластере и ожидаемые строки — в [отчете](report.md).

## Другой конфиг или база

Передайте одно имя базы обоим шагам:

```bash
bash homework-04/scripts/load-data.sh homework-04/cluster.env my_orders
bash homework-04/scripts/run-job.sh homework-04/cluster.env my_orders
```

Путь к другому конфигу также принимают `deploy.sh` и `verify.sh`. Для прямого вызова `submit.sh` задайте `CLUSTER_ENV=/path/cluster.env`. После перезагрузки узлов сначала поднимите HDFS, YARN и Hive, затем запускайте приложение.
