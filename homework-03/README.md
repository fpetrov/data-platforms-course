# Практическая работа №3: Hive

Hive 4.0.0-alpha-2 использует HDFS и YARN из работ [1](../homework-01/README.md) и [2](../homework-02/README.md). HiveServer2 принимает несколько JDBC-клиентов, отдельный Metastore хранит метаданные в PostgreSQL. Загрузка и контрольный агрегат выполняются через MapReduce в YARN.

| Узел | IP | Сервис | Адрес |
| --- | --- | --- | --- |
| team-30-en | 10.30.0.10 | HiveServer2 | JDBC `127.0.0.1:10000`, Web UI `127.0.0.1:10002` |
| team-30-00 | 10.30.0.12 | Отдельный Metastore | `10.30.0.12:9083` |
| team-30-00 | 10.30.0.12 | PostgreSQL 16, кластер `practice3` | `127.0.0.1:5433` |

На остальных узлах сохраняются роли HDFS и YARN. HS2 и Metastore работают от пользователя `hadoop`. PostgreSQL обслуживает отдельную базу `hive_metastore`.

## Подготовка

Нужны четыре узла Ubuntu 24.04 amd64 с готовыми HDFS/YARN, Hadoop 3.4.3 и OpenJDK 11. На edge нужны Python 3, curl, tar, SSH и доступ к архиву Apache; на узле Metastore — доступ к репозиториям Ubuntu. Пользователь `team` должен иметь sudo без пароля и SSH-доступ к внутренним узлам с проверенными ключами серверов.

Подключитесь с Mac к edge:

```bash
ssh -i ~/.ssh/id_ed25519_team30 team@2.59.83.239
```

Все команды ниже выполняются от `team` на edge из корня репозитория. Команда туннеля в последнем разделе выполняется на Mac.

```bash
cd ~/data-platforms-course
git pull --ff-only
cp homework-03/cluster.env.example homework-03/cluster.env
```

В `cluster.env` задаются имена и IP edge/Metastore, путь `ADMIN_KEY` на edge, Java/Hadoop, каталоги Hive, порты и память. Для team-30 подходят значения из примера. `HADOOP_CONF_DIR` указывает на `/etc/hadoop-practice2`. Имена узлов должны разрешаться, а `hostname -s` — совпадать с конфигом. Пароль PostgreSQL в этот файл не записывается; локальный `cluster.env` исключен из Git.

Если репозитория еще нет, получите его командой `git clone https://github.com/fpetrov/data-platforms-course.git` и выполните работы 1–2.

## Развертывание

```bash
bash homework-03/scripts/deploy.sh
```

`deploy.sh` требует запущенный HDFS и остановленные сервисы Hive. Он скачивает Hive, проверяет SHA256 архива, устанавливает дистрибутив на edge и узел Metastore, создает конфиги, выделенный кластер PostgreSQL и службы systemd. Схема Metastore создается один раз, затем проверяется через schematool. После запуска Metastore и HS2 скрипт ждет успешный JDBC-запрос `SELECT 1`.

Hive находится в `/opt/apache-hive-4.0.0-alpha-2-bin`, ссылка `/opt/hive-practice3` указывает на него. Конфиги создаются в `/etc/hive-practice3`:

| Файл | Настройка |
| --- | --- |
| `server/hive-site.xml` | Удаленный `hive.metastore.uris`, адреса HS2, `hive.execution.engine=mr`, warehouse/scratch и память задач |
| `metastore/hive-site.xml` | Bind Metastore, JDBC PostgreSQL, проверка схемы, запрет автосоздания схемы и `hive.metastore.event.db.notification.api.auth=true` |
| `metastore/core-site.xml` | Копия Hadoop core-site с `hadoop.proxyuser.hadoop.hosts`, ограниченным IP edge |
| `runtime.env`, `run-service.sh` | Пути, heap и запуск Java для HS2, Metastore, Beeline и schematool |

В `hive.aux.jars.path` добавлен `commons-collections-3.2.2.jar` из дистрибутива Hive для задач MR. Репликация файлов отправки MapReduce берется из `dfs.replication` текущего HDFS; в team-30 это 3. HS2 использует `authentication=NONE` и `doAs=false`: это учебная частная среда, JDBC и Web UI слушают только localhost. PostgreSQL также слушает localhost на узле Metastore. Его пароль генерируется на этом узле и хранится в закрытом каталоге `metastore`; JDBC-конфиг с паролем доступен только root и группе `hadoop`.

Heap Metastore — 384 МиБ, HS2 — 768 МиБ, Beeline — 256 МиБ. AM, map и reduce получают по 512 МиБ с heap 384 МиБ. Значения меняются в `cluster.env` до установки.

Локальные временные файлы находятся в `/srv/hive-practice3/tmp`, логи — в `/var/log/hive-practice3`, логи операций HS2 — в подкаталоге `operations`. В HDFS используются `/user/hadoop/homework-03/warehouse`, `/user/hadoop/homework-03/external` и временный `/tmp/hive-practice3`.

## Загрузка данных

```bash
bash homework-03/scripts/load-data.sh
```

Скрипт проверяет [orders.csv](data/orders.csv), создает базу `homework03` и две внешние таблицы по [schema.hql](sql/schema.hql), копирует CSV в HDFS и выполняет [load.hql](sql/load.hql):

| Таблица | Формат | Назначение |
| --- | --- | --- |
| `orders_raw` | TEXTFILE, разделитель `,` | Исходные `order_id INT`, `city STRING`, `amount DECIMAL(10,2)`, `order_date DATE` |
| `orders` | ORC, сжатие ZLIB | Те же данные с партиционированием по `order_date DATE` |

`orders_raw` читает исходный текст, `orders` хранит столбцы в ORC и позволяет отбирать нужные партиции по дате. Обе таблицы используют явный `LOCATION` в `$EXTERNAL_DIR/<database>/orders_raw` и `orders` внутри HDFS.

`INSERT OVERWRITE ... PARTITION (order_date)` создает три динамические партиции: `2026-10-01`, `2026-10-02`, `2026-10-03`. Для загрузки включены динамические партиции в режиме `nonstrict`, их число ограничено тремя. В CSV девять заказов, по три на каждую дату; общая сумма — 640.00.

Повторный запуск перезаписывает три даты примера без дублей и сохраняет другие даты. Перед изменением проверяются схема, формат, метка набора и `LOCATION` таблиц/целевых партиций. При несовпадении загрузка отменяется. Каталоги без таблицы и посторонние файлы в raw-каталоге также приводят к отказу.

Для другой учебной базы передайте конфиг и имя:

```bash
bash homework-03/scripts/load-data.sh homework-03/cluster.env my_orders
```

Имя начинается со строчной латинской буквы, содержит только строчные буквы, цифры и `_`. Служебные базы не используются.

## Проверка

```bash
bash homework-03/scripts/cluster.sh status
bash homework-03/scripts/verify.sh homework-03/cluster.env --restart
```

`verify.sh` создает отдельную базу `homework03_check_<суффикс>`. Он проверяет роли и адреса сервисов, схему PostgreSQL, состояние HDFS/YARN и два одновременно открытых JDBC-клиента. Оба клиента читают все девять заказов; после закрытия сессии возвращаются к исходному составу.

Данные TEXTFILE/ORC сравниваются с CSV, проверяются типы, три партиции и их пути. `EXPLAIN DEPENDENCY` должен отобрать одну партицию при фильтре по дате. Агрегат по городам выполняется в MR без локального режима, fetch и кеша результата. Его query tag связывается с успешным приложением YARN и заданием JobHistoryServer; результат сверяется с независимым расчетом Python Decimal.

После повторной загрузки проверка снова сравнивает строки и метаданные. В режиме `--restart` перезапускаются только HS2, Metastore и выделенный PostgreSQL; затем без загрузки проверяются прежние SQL-данные, метаданные и SHA256 файлов ORC. PID процессов HDFS/YARN и файл работы 1 должны сохраниться. База проверки остается для просмотра. Успех заканчивается строкой `Проверка Hive пройдена.` и кодом 0.

Для проверки без перезапуска используйте `bash homework-03/scripts/verify.sh`.

Запрос через Beeline на edge:

```bash
bash homework-03/scripts/client.sh -e 'SELECT * FROM homework03.orders ORDER BY order_id;'
```

`client.sh` также принимает `-f /path/query.hql` и `-n client_name`. Для другого конфига клиента задайте `CLUSTER_ENV=/path/cluster.env` перед командой.

## Управление

```bash
bash homework-03/scripts/cluster.sh stop
bash homework-03/scripts/cluster.sh start
```

`start` запускает Metastore с PostgreSQL, ждет его готовность и запускает HS2. `stop` останавливает HS2, Metastore и только выделенный кластер PostgreSQL `16/practice3`. Для перезапуска используйте `cluster.sh restart`. Службы Hive включены в systemd и перезапускаются при сбое; после перезагрузки четырех узлов сначала поднимите HDFS и YARN по работам 1–2, затем проверьте `cluster.sh start` и `status` для Hive.

Для повторной установки сначала остановите Hive:

```bash
bash homework-03/scripts/cluster.sh stop
bash homework-03/scripts/deploy.sh
```

Существующая база PostgreSQL и файлы HDFS сохраняются. Для другого конфига передайте его путь: `deploy.sh /path/cluster.env`, `load-data.sh /path/cluster.env my_orders`, `verify.sh /path/cluster.env --restart` или `cluster.sh status /path/cluster.env`.

## Доступ с Mac

Из корня локальной копии репозитория откройте туннель в отдельном терминале и оставьте его работать:

```bash
bash homework-03/scripts/tunnel.sh
```

Откройте [HiveServer2 Web UI](http://127.0.0.1:10002). JDBC-адрес — `jdbc:hive2://127.0.0.1:10000/default`. Web UI показывает сессии и операции; запросы выполняются через Beeline/JDBC. Для другого edge или ключа используйте `tunnel.sh user@edge /path/key`. `Ctrl+C` закрывает туннель.

Для измененных портов явно укажите локальный конфиг:

```bash
CLUSTER_ENV=homework-03/cluster.env bash homework-03/scripts/tunnel.sh
```

Фактические результаты записаны в [отчете](report.md).
