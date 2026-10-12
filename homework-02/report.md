# Результат проверки

Кластер team-30 проверен на четырех узлах Ubuntu 24.04.5 LTS с OpenJDK 11.0.32.1 и Hadoop 3.4.3. На edge установлены Python 3.12.3 и nginx 1.24.0.

Итоговая проверка выполнена из чистого `main`, коммит `2cbfff4`. Выполнены команды:

```bash
bash homework-02/scripts/publish-web.sh
bash homework-02/scripts/verify.sh homework-02/cluster.env --restart
bash homework-02/scripts/cluster.sh status
```

Все команды завершились с кодом 0. Полный вывод и версии узлов сохранены в [verification.txt](verification.txt).

| Проверка | Результат |
| --- | --- |
| Процессы YARN | ResourceManager, JobHistoryServer, ApplicationHistoryServer, WebAppProxyServer и 3 NodeManager |
| Ресурсы | 3 здоровых NodeManager в `RUNNING`, 4608 МиБ, 6 vcores |
| WordCount | `FINISHED/SUCCEEDED`, 2/2 map, 1/1 reduce, `uber=false` |
| JobHistoryServer | Задание `SUCCEEDED`, история доступна после перезапуска |
| Timeline v1 | Событие `YARN_APPLICATION_FINISHED/SUCCEEDED` сохранено |
| Агрегированные логи | `stdout` и `syslog` четырех контейнеров читаются; SHA256 после перезапуска совпал |
| Остановка и запуск | Результат, история JHS/Timeline и логи сохранились без нового запуска задания |
| HDFS | 3 живых DataNode, нет проблемных узлов и блоков, safe mode OFF; PID демонов и файл работы 1 сохранились |
| Текущие логи YARN | Нет ERROR/FATAL |
| Веб | Указатель, 12 интерфейсов, 110 CSS/JS, REST/JMX и ссылки доступны |
| Публикация | Все 13 портов слушают только `127.0.0.1`; POST возвращает 405, служебные операции — 403 |

Приложение: `application_1791728377373_0001`, задание: `job_1791728377373_0001`. Каталог проверки: `/user/hadoop/homework-02/20261011T142624Z-WUTH7K`.

WordCount обработал миллион строк, результат совпал побайтно:

```text
hadoop     1000000
mapreduce  500000
yarn       1000000
```

SHA256 результата до и после перезапуска:

```text
1b50553e28e9adb1b91b5dd52d9886e4feba1e83b116527a9f9ed8ba5d6374fb
```

SHA256 агрегированных логов:

```text
968a8207f1648aa7bf57c63a5c7a114dbbdc12f024279a158b203c0443a18d8b
```

В Safari на Mac через SSH-туннель проверены указатель, переходы ResourceManager → Nodes → NodeManager и NameNode → DataNodes → DataNode. Просмотр файла через NameNode показал его содержимое. Интерфейсы открываются на localhost.

Для работающего `application_1791728881215_0002` проверены Tracking UI, страница задания в `RUNNING` и переход к текущему `syslog` через NodeManager.
