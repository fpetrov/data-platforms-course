# Результат проверки

25 сентября 2026 года кластер team-30 проверен на четырех узлах Ubuntu 24.04 с OpenJDK 11 и Hadoop 3.4.3.

Выполнены `verify.sh cluster.env --restart`, повторный `verify.sh cluster.env` и `cluster.sh status cluster.env`. Все команды завершились с кодом 0. Полный вывод проверки с перезапуском сохранен в [verification.txt](verification.txt).

| Проверка | Результат |
| --- | --- |
| Процессы | NameNode, SecondaryNameNode, 3 DataNode |
| DataNode | 3 живых, все `In Service`, IP совпадают с конфигурацией |
| Dead, stale, decommission, maintenance | 0 |
| Сбои дисков | 0 |
| Missing, corrupt, under-replicated blocks | 0 |
| Safe mode | OFF |
| Последний checkpoint после перезапуска | 25.09.2026 15:41:53 UTC |
| Запись и чтение | Содержимое совпало побайтно |
| FSCK | HEALTHY, 3 блока, по 3 реплики |
| Остановка и запуск | Файл сохранился без повторной записи |
| Текущие логи | Нет ERROR/FATAL, кроме штатного SIGTERM при остановке |

Тестовый файл: `/user/hadoop/homework-01/sample.bin`, 2 621 440 байт. SHA256 до и после перезапуска одинаковый:

```text
b1ccfef6cbd6d79b9b77f126b2a2f1a58d8f3d54beccdeeec3b7f772453ffcee
```

Повторная проверка также прошла. `dfsadmin -report` подтвердил три живых DataNode, нормальное состояние каждого узла и отсутствие проблем с блоками.
