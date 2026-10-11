# Практическая работа №2: YARN

YARN развернут поверх HDFS из [работы 1](../homework-01/README.md) на Hadoop 3.4.3. На edge работают ResourceManager и вспомогательные сервисы, на трех внутренних узлах — NodeManager.

| Узел | IP | Роль YARN | Роль HDFS из работы 1 |
| --- | --- | --- | --- |
| team-30-en | 10.30.0.10 | ResourceManager, JobHistoryServer, Timeline Server, WebAppProxy, клиент | Клиент HDFS |
| team-30-nn | 10.30.0.11 | NodeManager | NameNode, DataNode |
| team-30-00 | 10.30.0.12 | NodeManager | DataNode |
| team-30-01 | 10.30.0.13 | NodeManager | SecondaryNameNode, DataNode |

ResourceManager распределяет ресурсы, NodeManager запускает контейнеры. JobHistoryServer хранит историю MapReduce, Timeline Server v1 — события приложений. Его процесс называется `ApplicationHistoryServer`. Отдельный WebAppProxy открывает интерфейс ApplicationMaster по ссылке Tracking UI из ResourceManager.

## Подготовка

Нужны четыре узла Ubuntu 24.04 amd64 с готовым HDFS, Hadoop 3.4.3 и OpenJDK 11 из работы 1. На edge нужен Python 3, доступ к репозиториям Ubuntu, sudo без пароля для `team` и SSH-доступ к внутренним узлам. Пользователь `hadoop` использует ключ и список проверенных узлов из первой работы.

Подключитесь с Mac к edge:

```bash
ssh -i ~/.ssh/id_ed25519_team30 team@2.59.83.239
```

Все команды с файлами репозитория ниже выполняются от `team` на edge из корня `data-platforms-course`. Обновите существующую копию и создайте конфиг:

```bash
cd ~/data-platforms-course
git pull --ff-only
cp homework-02/cluster.env.example homework-02/cluster.env
```

Если репозитория еще нет, получите его командой `git clone https://github.com/fpetrov/data-platforms-course.git` и сначала выполните работу 1.

В `homework-02/cluster.env` укажите имена и IP узлов, `ADMIN_USER`, путь `ADMIN_KEY` на edge и пути установленного Hadoop. Для team-30 подходят значения из примера. `HDFS_CONF_DIR` указывает на конфиг первой работы, `CONF_DIR` — на отдельный конфиг YARN. Имена должны совпадать с `hostname -s`; установщик добавляет их в `/etc/hosts`. Локальный `cluster.env` исключен из Git.

## Развертывание

```bash
bash homework-02/scripts/deploy.sh
bash homework-02/scripts/publish-web.sh
```

`deploy.sh` проверяет доступ к HDFS, SSH и отсутствие запущенных демонов YARN. Затем [setup-node.sh](scripts/setup-node.sh) создает конфиги на четырех узлах, а `cluster.sh` запускает семь процессов от пользователя `hadoop`: ResourceManager, JobHistoryServer, ApplicationHistoryServer, WebAppProxyServer и три NodeManager. HDFS не форматируется и не перезапускается.

Конфигурация находится в `/etc/hadoop-practice2`:

| Файл | Настройка |
| --- | --- |
| `core-site.xml`, `hdfs-site.xml`, `log4j.properties` | Копии из работы 1: адрес HDFS, хранилище и логирование |
| `yarn-site.xml` | ResourceManager на edge, ресурсы NodeManager, `mapreduce_shuffle`, `env-whitelist`, агрегация логов, Timeline v1 и WebAppProxy |
| `mapred-site.xml` | `mapreduce.framework.name=yarn`, `mapreduce.application.classpath`, память AM/map/reduce, JobHistoryServer и отправка событий в Timeline |
| `capacity-scheduler.xml` | Очередь `default` получает 100% ресурсов, доля ApplicationMaster ограничена 25% |
| `workers` | Три внутренних узла с NodeManager |
| `hadoop-env.sh` | Java, пути Hadoop, heap демонов, логи, PID и SSH |

Каждый NodeManager выделяет 1536 МиБ и 2 vcores; всего доступно 4608 МиБ и 6 vcores. Контейнеры получают от 256 до 1536 МиБ. Для AM, map и reduce задано по 512 МиБ с Java heap 384 МиБ; heap демонов также 384 МиБ. Проверка физической памяти включена, виртуальной — выключена. Значения меняются в `cluster.env` до развертывания.

Локальные данные находятся в `/srv/hadoop-practice2`: `local` для контейнеров, `container-logs` для их логов, `pids` для PID, `timeline` для хранилища Timeline. Логи демонов находятся в `/var/log/hadoop-practice2`. В HDFS создаются `/yarn-logs` для агрегированных логов и `/mr-history/tmp`, `/mr-history/done` для истории MapReduce. Логи приложений хранятся семь дней.

`publish-web.sh` ставит nginx, создает указатель и прокси для интерфейсов. Конфиги находятся в `/etc/nginx/conf.d/hadoop-practice2.conf` и `/etc/nginx/snippets/hadoop-practice2-links.conf`, указатель — в `/var/www/hadoop-practice2/index.html`. Все 13 портов публикации слушают только `127.0.0.1`. Внутренние ссылки и перенаправления заменяются адресами localhost. Разрешены GET и HEAD; изменение настроек через `logLevel`, профилирование и запросы delegation token закрыты.

Повторное развертывание выполняется при остановленном YARN. История, логи и данные HDFS сохраняются:

```bash
bash homework-02/scripts/cluster.sh stop
bash homework-02/scripts/deploy.sh
bash homework-02/scripts/publish-web.sh
```

## Проверка

```bash
bash homework-02/scripts/cluster.sh status
bash homework-02/scripts/verify.sh
bash homework-02/scripts/verify.sh homework-02/cluster.env --restart
```

`status` показывает процессы через `jps` и список NodeManager. Ожидаются роли из таблицы выше и три NodeManager в состоянии `RUNNING`.

`verify.sh` проверяет состав процессов, ресурсы и здоровье YARN/HDFS, текущие логи демонов, указатель, все интерфейсы, CSS/JS, REST/JMX и ссылки между узлами. Затем запускает WordCount в YARN с двумя map и одним reduce без uber mode. Вход содержит миллион строк в двух файлах. Результат сравнивается побайтно, после чего проверяются состояние `FINISHED/SUCCEEDED`, запись JobHistoryServer, событие завершения Timeline и агрегированные логи четырех контейнеров.

Каждый запуск создает отдельный каталог `/user/hadoop/homework-02/<дата>-<суффикс>`. После проверки он остается в HDFS вместе с результатом. Пока приложение работает, скрипт выводит ссылку Tracking UI.

Режим `--restart` останавливает и запускает только YARN. Он проверяет прежний результат, историю JHS/Timeline и SHA256 агрегированных логов без нового запуска задания. Процессы HDFS должны сохранить PID, а тестовый файл работы 1 — содержимое. Успешная проверка заканчивается строкой `Проверка YARN пройдена.` и кодом 0.

Остановка и запуск без переустановки:

```bash
bash homework-02/scripts/cluster.sh stop
bash homework-02/scripts/cluster.sh start
```

После перезагрузки узлов сначала запустите HDFS: `bash homework-01/scripts/cluster.sh start`, затем YARN: `bash homework-02/scripts/cluster.sh start`. Для другого конфига передайте его путь: `deploy.sh /path/cluster.env`, `publish-web.sh /path/cluster.env`, `verify.sh /path/cluster.env --restart` или `cluster.sh status /path/cluster.env`.

## Веб-интерфейсы

На Mac из корня локальной копии репозитория откройте туннель в отдельном терминале и оставьте его работать:

```bash
bash homework-02/scripts/tunnel.sh
```

Скрипт использует `team@2.59.83.239` и `~/.ssh/id_ed25519_team30` на Mac. Для другой среды передайте адрес edge и путь к своему ключу: `tunnel.sh user@edge /path/key`. `Ctrl+C` закрывает туннель.

Откройте [указатель интерфейсов](http://127.0.0.1:18080). Доступ через localhost соответствует условию задания.

| Служба | Узел | Адрес через туннель |
| --- | --- | --- |
| NameNode | team-30-nn | [127.0.0.1:19870](http://127.0.0.1:19870/dfshealth.html) |
| SecondaryNameNode | team-30-01 | [127.0.0.1:19868](http://127.0.0.1:19868/status.html) |
| DataNode | team-30-nn | [127.0.0.1:19864](http://127.0.0.1:19864/datanode.html) |
| DataNode | team-30-00 | [127.0.0.1:19865](http://127.0.0.1:19865/datanode.html) |
| DataNode | team-30-01 | [127.0.0.1:19866](http://127.0.0.1:19866/datanode.html) |
| ResourceManager | team-30-en | [127.0.0.1:18088](http://127.0.0.1:18088/cluster) |
| NodeManager | team-30-nn | [127.0.0.1:18042](http://127.0.0.1:18042/node) |
| NodeManager | team-30-00 | [127.0.0.1:18043](http://127.0.0.1:18043/node) |
| NodeManager | team-30-01 | [127.0.0.1:18044](http://127.0.0.1:18044/node) |
| JobHistoryServer | team-30-en | [127.0.0.1:29888](http://127.0.0.1:29888/jobhistory) |
| Timeline Server | team-30-en | [127.0.0.1:18188](http://127.0.0.1:18188/applicationhistory) |
| WebAppProxy, JMX | team-30-en | [127.0.0.1:19099](http://127.0.0.1:19099/jmx) |

В ResourceManager откройте Nodes: должно быть три здоровых NodeManager. Ссылки на них открываются через тот же туннель. В NameNode доступны DataNodes и просмотр файлов HDFS, в JobHistoryServer — завершенные задания и логи. У WebAppProxy нет отдельной обзорной страницы; Tracking UI работающего приложения открывается по адресу `http://127.0.0.1:19099/proxy/<application_id>/`.

Фактические результаты записаны в [отчете](report.md).
