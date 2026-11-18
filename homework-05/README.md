# Практическая работа №5: Prefect

Prefect 3.9.3 управляет загрузкой CSV и Spark-обработкой из [работы 4](../homework-04/README.md). Flow последовательно выполняет задачи `load-input` и `process-orders`.

## Установка

Нужны выполненные работы 1–4 на кластере, Python 3.12, curl и доступ к PyPI. Команды выполняются от `team` на edge из корня репозитория. В готовой среде перейдите в копию командой `cd ~/data-platforms-hw05`. Новую копию можно создать так.

```bash
git clone https://github.com/fpetrov/data-platforms-course.git data-platforms-hw05
cd data-platforms-hw05
```

Подготовьте локальные конфиги. `cp -n` сохраняет уже существующие файлы.

```bash
cp -n homework-03/cluster.env.example homework-03/cluster.env
cp -n homework-04/cluster.env.example homework-04/cluster.env
cp -n homework-05/cluster.env.example homework-05/cluster.env
```

Проверьте в конфигах работ 3–4 параметры своего кластера и пути Java/Hadoop. Конфиг работы 5 наследует настройки работы 4, задает отдельные каталоги HDFS и использует соседние папки работ 3–4.

```bash
bash homework-05/scripts/deploy.sh
```

Установщик создает отдельное Python-окружение с зависимостями из `requirements.txt`. Prefect использует временный локальный API и SQLite. Внешний UI отключен.

## Запуск

```bash
bash homework-05/scripts/run-flow.sh
```

В [orders.py](flows/orders.py) определен `@flow` с именем `homework05-orders`. Задача `load-input` сначала загружает учебный CSV в HDFS. Затем `process-orders` вызывает `run-job.sh` из работы 4 и ее [Spark-приложение](../homework-04/jobs/orders.py). Приложение выполняет пять действий.

1. Создает одну SparkSession под YARN.
2. Подключается к HDFS через конфиги `HADOOP_CONF_DIR`.
3. Читает предварительно загруженный HDFS CSV.
4. Выполняет `trim`, `upper`, приведение типов и `groupBy` с `count`, `sum`, `avg`.
5. Сохраняет внешнюю ORC-таблицу `homework05.orders_daily` с партициями по дате.

После `spark.stop()` результат читает Beeline. Из девяти заказов получаются шесть строк в трех партициях, общая сумма — 640.00. Таблицу можно прочитать вручную.

```bash
CLUSTER_ENV=homework-03/cluster.env bash homework-03/scripts/client.sh \
    -e 'SET hive.query.results.cache.enabled=false; SELECT * FROM homework05.orders_daily ORDER BY order_date, city;'
```

Другую базу можно задать командой `bash homework-05/scripts/run-flow.sh homework05_demo`, другой конфиг — через `CLUSTER_ENV=/path/cluster.env` перед командой. Повторный запуск перезаписывает даты примера без дублей.

## Проверка и логи

```bash
bash homework-05/scripts/verify.sh
```

Проверка запускает новый flow в отдельной базе. Она подтверждает состояния `Completed` и порядок двух задач, успешный Spark под YARN, чтение HDFS, преобразования и точные строки таблицы через Beeline. Успешная проверка завершается строкой `Проверка Prefect пройдена.` и кодом 0. База остается для просмотра.

Вместо `<flow_run_id>` подставьте ID из вывода запуска или проверки. Подробный вывод загрузки и Spark сохраняется локально.

```bash
source homework-05/scripts/prefect-env.sh
cat "$PREFECT_HOME/runs/<flow_run_id>/load-data.sh.txt"
cat "$PREFECT_HOME/runs/<flow_run_id>/run-job.sh.txt"
```

Результаты запуска — в [отчете](report.md).
