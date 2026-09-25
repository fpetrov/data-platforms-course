# Практическая работа №1: HDFS

Кластер Hadoop 3.4.3 состоит из NameNode, SecondaryNameNode и трех DataNode. На edge запускаются установка, управление и проверки.

| Узел | IP | Роль |
| --- | --- | --- |
| team-30-en | 10.30.0.10 | Edge, клиент HDFS |
| team-30-nn | 10.30.0.11 | NameNode, DataNode |
| team-30-00 | 10.30.0.12 | DataNode |
| team-30-01 | 10.30.0.13 | SecondaryNameNode, DataNode |

SecondaryNameNode создает контрольные точки метаданных. Это не резервный NameNode.

## Подготовка

Нужны четыре узла Ubuntu 24.04 amd64, Python 3 на edge и доступ к репозиториям Ubuntu и Apache. Пользователь `team` должен иметь sudo без пароля и SSH-доступ с edge на внутренние узлы. Имена из `hostname -s` должны совпадать с `cluster.env`. Скрипт сам добавит соответствия имен и IP в `/etc/hosts`.

Подключитесь с Mac к edge:

```bash
ssh -i ~/.ssh/id_ed25519_team30 team@2.59.83.239
```

На edge проверьте доступ к внутренним узлам. При первом подключении сверьте отпечатки ключей серверов:

```bash
for host in team-30-nn team-30-00 team-30-01; do
    ssh -i ~/.ssh/team_internal team@"$host" 'hostname -s; sudo -n true'
done
python3 --version
```

Получите файлы задания:

```bash
sudo apt-get update
sudo apt-get install -y git
git clone https://github.com/fpetrov/data-platforms-course.git
cd data-platforms-course
cp homework-01/cluster.env.example homework-01/cluster.env
```

В `homework-01/cluster.env` укажите имена и IP своей среды, пользователя `ADMIN_USER` и путь к ключу `ADMIN_KEY` на edge. Для team-30 подходят значения из примера. `ARCHIVE` задает путь к кешу дистрибутива. Приватные ключи остаются на своих узлах, а локальный `cluster.env` исключен из Git.

## Развертывание

Все дальнейшие команды выполняются от `team` на edge из корня репозитория:

```bash
bash homework-01/scripts/deploy.sh
```

`deploy.sh` проверяет, что HDFS остановлен, скачивает Hadoop и вызывает `setup-node.sh` на четырех узлах. Установка проверяет SHA512 архива, ставит OpenJDK 11 и создает пользователя `hadoop` без sudo. Дистрибутив находится в `/opt/hadoop-3.4.3`, ссылка `/opt/hadoop` указывает на него.

Конфигурация создается в `/etc/hadoop-practice1`:

| Файл | Настройка |
| --- | --- |
| `core-site.xml` | Адрес HDFS `hdfs://team-30-nn:9000`, временный каталог |
| `hdfs-site.xml` | Репликация 3, каталоги данных, адреса NameNode и SecondaryNameNode |
| `workers` | Три узла, на которых запускается DataNode |
| `hadoop-env.sh` | Java, пути Hadoop, память процессов и SSH |

Данные хранятся в `/srv/hadoop`: `name` для NameNode, `data` для DataNode, `checkpoint` для SecondaryNameNode. Логи находятся в `/var/log/hadoop-practice1`. Пути можно изменить в `cluster.env` до установки.

Для запуска демонов установщик создает SSH-ключ пользователя `hadoop` на edge и передает внутренним узлам только публичную часть. NameNode форматируется один раз, только при пустом каталоге без `current/VERSION`. Затем `start-dfs.sh` запускает NameNode, три DataNode и SecondaryNameNode от пользователя `hadoop`.

Для повторного развертывания сначала остановите кластер. Существующий NameNode повторно не форматируется:

```bash
bash homework-01/scripts/cluster.sh stop
bash homework-01/scripts/deploy.sh
```

## Проверка

```bash
bash homework-01/scripts/cluster.sh status
bash homework-01/scripts/verify.sh
bash homework-01/scripts/verify.sh homework-01/cluster.env --restart
```

`status` показывает процессы через `jps` и отчет `dfsadmin`. Ожидаются NameNode и DataNode на `team-30-nn`, DataNode на `team-30-00`, SecondaryNameNode и DataNode на `team-30-01`.

`verify.sh` проверяет через JMX три живых DataNode, отсутствие проблемных узлов и блоков, выключенный safe mode и созданный checkpoint. Затем записывает тестовый файл `/user/hadoop/homework-01/sample.bin`, читает его обратно и сравнивает содержимое. Файл перезаписывается при каждом запуске проверки. Его размер 2,5 МиБ, размер блока 1 МиБ; `fsck` должен показать `HEALTHY`, три блока и по три реплики.

Режим `--restart` дополнительно останавливает и запускает HDFS, после чего проверяет тот же файл без повторной записи. В конце проверяются состав процессов и текущие логи всех трех серверов. Из `ERROR` исключается только штатное сообщение `RECEIVED SIGNAL 15: SIGTERM` при остановке. Успешный запуск заканчивается строкой `Проверка HDFS пройдена.` и кодом 0.

Остановка и запуск без переустановки:

```bash
bash homework-01/scripts/cluster.sh stop
bash homework-01/scripts/cluster.sh start
```

Для другого файла настроек передайте его путь: `deploy.sh /path/cluster.env`, `verify.sh /path/cluster.env --restart` или `cluster.sh status /path/cluster.env`.

## Веб-интерфейс

В отдельном терминале Mac откройте SSH-туннель и оставьте его работать:

```bash
ssh -i ~/.ssh/id_ed25519_team30 -N \
    -L 127.0.0.1:9870:team-30-nn:9870 \
    -L 127.0.0.1:9868:team-30-01:9868 \
    team@2.59.83.239
```

Откройте [NameNode](http://127.0.0.1:9870): в разделе DataNodes должны быть `Live Nodes: 3`, `Dead Nodes: 0`, все узлы в состоянии `In Service`. В сводке нет потерянных, поврежденных и недостаточно реплицированных блоков. [SecondaryNameNode](http://127.0.0.1:9868) показывает время последнего checkpoint. Интерфейсы доступны через туннель без открытия портов в интернет.

Фактический результат развертывания и проверки записан в [отчете](report.md).
