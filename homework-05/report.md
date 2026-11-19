# Результат проверки

19 ноября 2026 года по московскому времени проверен чистый `main`, коммит `dd43c64`. Использованы Prefect 3.9.3 и Spark из работы 4.

```bash
bash homework-05/scripts/deploy.sh
bash homework-05/scripts/verify.sh
```

Обе команды завершились с кодом 0. Компактный журнал сохранен в [verification.txt](verification.txt).

| Проверка | Результат |
| --- | --- |
| Prefect | Flow и задачи `load-input` → `process-orders` завершились в `Completed` |
| Вход HDFS | CSV загружен до SparkSession, 231 байт. SHA256 совпал с исходником |
| Spark под YARN | Одно приложение `SPARK FINISHED/SUCCEEDED`, 6 успешных удаленных задач, чтение и shuffle подтверждены |
| Преобразования | 9 заказов → 6 групп, сумма 640.00. Результат совпал с независимым расчетом |
| Таблица | Beeline прочитал точные строки внешней ORC-таблицы с тремя партициями по дате |

Результат оставлен в `homework05_check_1037bc34e2.orders_daily`. Для просмотра также доступна таблица обычного запуска `homework05.orders_daily`.
