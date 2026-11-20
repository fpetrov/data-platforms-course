# Практическая работа №5: Prefect

Prefect 3.9.3 запускает загрузку CSV и Spark-обработку из [работы 4](../homework-04/README.md).

## Установка

Нужны работающие HDFS, YARN, Hive и Spark из работ 1–4, Python 3.12, git, curl и доступ к PyPI. Команды выполняются от `team` на edge. Если копии репозитория ещё нет:

```bash
git clone https://github.com/fpetrov/data-platforms-course.git data-platforms-hw05
cd data-platforms-hw05
```

Из корня репозитория подготовьте конфиги (`cp -n` сохраняет существующие):

```bash
cp -n homework-03/cluster.env.example homework-03/cluster.env
cp -n homework-04/cluster.env.example homework-04/cluster.env
cp -n homework-05/cluster.env.example homework-05/cluster.env
```

В конфигах работ 3–4 проверьте адреса узлов и пути Java/Hadoop. Работа 5 наследует эти настройки, использует соседние папки работ 3–4 и отдельный каталог HDFS `/user/hadoop/homework-05`.

```bash
bash homework-05/scripts/deploy.sh
```

Скрипт создаст отдельный venv и установит зависимости из `requirements.txt`. Локальный API Prefect запускается автоматически.

## Запуск

```bash
bash homework-05/scripts/run-flow.sh
```

В flow `homework05-orders` из [orders.py](flows/orders.py) две последовательные задачи: `load-input` загружает CSV в HDFS, `process-orders` вызывает `run-job.sh` из работы 4. Его [Spark-приложение](../homework-04/jobs/orders.py):

1. Создает SparkSession под YARN.
2. Подключается к HDFS через конфиги `HADOOP_CONF_DIR`.
3. Читает заранее загруженный CSV.
4. Очищает строки, приводит типы и считает `count`, `sum`, `avg` по дате и городу.
5. Сохраняет внешнюю ORC-таблицу `homework05.orders_daily` с партициями по дате.

После остановки Spark результат читает Beeline: шесть строк из девяти заказов, три партиции, сумма 640.00. Ручное чтение:

```bash
CLUSTER_ENV=homework-03/cluster.env bash homework-03/scripts/client.sh \
    -e 'SET hive.query.results.cache.enabled=false; SELECT * FROM homework05.orders_daily ORDER BY order_date, city;'
```

Другая база: `bash homework-05/scripts/run-flow.sh homework05_demo`. Другой конфиг задается через `CLUSTER_ENV=/path/cluster.env`. Повторный запуск перезаписывает даты примера без дублей.

## Проверка и логи

```bash
bash homework-05/scripts/verify.sh
```

Проверка создает отдельную базу и сверяет состояния `Completed`, порядок задач, Spark под YARN, входной CSV и строки таблицы через Beeline. Успех: `Проверка Prefect пройдена.` и код 0. База остается для просмотра.

Вывод загрузки и Spark сохраняется в логах. Подставьте `<flow_run_id>` из вывода запуска или проверки:

```bash
source homework-05/scripts/prefect-env.sh
cat "$PREFECT_HOME/runs/<flow_run_id>/load-data.sh.txt"
cat "$PREFECT_HOME/runs/<flow_run_id>/run-job.sh.txt"
```

Результаты запуска — в [отчете](report.md).
