# Результат проверки

23 октября 2026 года на кластере team-30 проверена связка Hive 4.0.0-alpha-2, Hadoop 3.4.3 и OpenJDK 11.0.32.1 под Ubuntu 24.04.5 LTS. Metastore использует PostgreSQL 16.15 и JDBC-драйвер 42.7.2.

Проверка выполнена из чистого `main`, коммит `d4a338a`:

```bash
bash homework-03/scripts/cluster.sh stop
bash homework-03/scripts/deploy.sh
bash homework-03/scripts/verify.sh homework-03/cluster.env --restart
bash homework-03/scripts/cluster.sh status
```

Все команды завершились с кодом 0. Полный вывод с версиями и SHA256 сохранен без изменений в [verification.txt](verification.txt).

| Проверка | Результат |
| --- | --- |
| Архитектура | HS2 на edge, отдельный Metastore и PostgreSQL `16/practice3` на team-30-00 |
| Адреса | JDBC/Web UI HS2 и PostgreSQL слушают localhost, Metastore — внутренний IP |
| Схема Metastore | `4.0.0-alpha-2`, schematool validate: `SUCCESS` |
| Повторное развертывание | Строки и партиции существующей базы `homework03` сохранены |
| Одновременные клиенты | Два разных живых JDBC sessionId, оба прочитали 9 строк; счетчик сессий `0 → 2 → 0` |
| Таблицы | Внешние TEXTFILE и ORC совпали с CSV; типы, `LOCATION` и партиционирование по DATE верны |
| Данные | 9 строк, 9 уникальных заказов, сумма 640.00 |
| Отбор партиций | `EXPLAIN DEPENDENCY`: 3 партиции без фильтра, 1 для `2026-10-01` |
| MapReduce | Загрузка, агрегат и повторная загрузка создали 5 успешных приложений YARN |
| Повторная загрузка | Снова 9 строк и 3 партиции, без дублей |
| Перезапуск Hive/PostgreSQL | SQL-данные, метаданные и SHA256 всех трех файлов ORC сохранились без загрузки |
| HDFS/YARN | 3 здоровых DataNode и NodeManager, 4608 МиБ, 6 vcores; PID и файл работы 1 сохранены |
| Сервисы после проверки | HS2, Metastore и PostgreSQL в состоянии `active`, JDBC `SELECT 1` успешен |

Контрольный агрегат связан с `query tag=homework03_verify_cae3670816` и приложением `application_1791728881215_0023`. В JobHistoryServer задание `job_1791728881215_0023` завершено с одной map и одним reduce. Результат совпал с независимым расчетом Python Decimal: Moscow — 4 заказа на 390.00, Kazan — 5 заказов на 250.00.

| Дата | Заказов | Сумма |
| --- | --- | --- |
| 2026-10-01 | 3 | 250.00 |
| 2026-10-02 | 3 | 200.00 |
| 2026-10-03 | 3 | 190.00 |

Результат оставлен в `homework03_check_cae3670816.orders`. Файлы находятся в `hdfs://team-30-nn:9000/user/hadoop/homework-03/external/homework03_check_cae3670816/orders`.
