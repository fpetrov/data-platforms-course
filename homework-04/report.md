# Результат проверки

23 октября 2026 года Spark 3.5.7 проверен на team-30 с Ubuntu 24.04.5 LTS, Java 11.0.32.1 и Python 3.12.3. Встроенные клиенты Hadoop 3.3.4 и Hive 2.3.9 работали с существующими Hadoop 3.4.3 и общим Metastore 4.0.0-alpha-2.

Проверен экспорт чистого `main`, коммит `9fe19f0`, в `/home/team/spark-final.YrvYPR`:

```bash
bash homework-04/scripts/deploy.sh
bash homework-04/scripts/verify.sh
bash homework-03/scripts/cluster.sh status
```

Все команды завершились с кодом 0. Полный вывод с версиями и метриками сохранен без изменений в [verification.txt](verification.txt).

| Проверка | Результат |
| --- | --- |
| Вход до SparkSession | CSV из работы 3 загружен в HDFS, 231 байт; содержимое совпало побайтно |
| Spark/YARN | Два приложения `SPARK FINISHED/SUCCEEDED`, executors на `team-30-nn` и `team-30-01` |
| Обработка | 9 заказов преобразованы в 6 групп, общая сумма 640.00; данные совпали с Python Decimal |
| Таблица | EXTERNAL ORC, DECIMAL(12,2), 3 DATE-партиции; схема и все `LOCATION` верны |
| Чтение Hive | Стандартный Beeline прочитал точные строки после `spark.stop()`, кеш результата отключен |
| Event log | Чтение HDFS, успешные удаленные tasks/stages и shuffle подтверждены; shuffle write/read 607/607 байт |
| Отбор партиций | `EXPLAIN DEPENDENCY`: 3 партиции → только `2026-10-01` |
| Повтор | Строки и метаданные совпали, дублей нет, входной CSV сохранен |
| Предыдущие работы | PID HDFS/YARN/Hive/PostgreSQL, `sample.bin`, таблицы работы 3 и SHA256 их ORC сохранены |
| Состояние кластера | 3 DataNode и 3 NodeManager здоровы, 4608 МиБ/6 vcores; Hive/PostgreSQL `active`, JDBC `SELECT 1` успешен |

Первое приложение — `application_1791728881215_0043`: 6 успешных удаленных tasks и 6 stages. Повторное — `application_1791728881215_0045`: 8 tasks и 8 stages. В обоих event log зафиксировано чтение 693 байт.

Результат в `homework04_check_797d1877d3.orders_daily`:

| Дата | Город | Заказов | Сумма | Среднее |
| --- | --- | --- | --- | --- |
| 2026-10-01 | KAZAN | 1 | 50.00 | 50.00 |
| 2026-10-01 | MOSCOW | 2 | 200.00 | 100.00 |
| 2026-10-02 | KAZAN | 2 | 100.00 | 50.00 |
| 2026-10-02 | MOSCOW | 1 | 100.00 | 100.00 |
| 2026-10-03 | KAZAN | 2 | 100.00 | 50.00 |
| 2026-10-03 | MOSCOW | 1 | 90.00 | 90.00 |

Таблица оставлена для просмотра. Файлы находятся в `hdfs://team-30-nn:9000/user/hadoop/homework-04/external/homework04_check_797d1877d3/orders_daily`.

SHA256 входного CSV до и после обработки:

```text
4793160b3d7b7042928dafcbee82802cfea941e10927c80976a048e6a11c2d68
```
