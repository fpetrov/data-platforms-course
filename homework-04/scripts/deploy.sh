#!/usr/bin/env bash
set -euo pipefail

task_dir=$(cd "$(dirname "$0")/.." && pwd)
config=$(realpath "${1:-${CLUSTER_ENV:-$task_dir/cluster.env}}")
source "$config"
[[ $(hostname -s) == "$EDGE_HOST" && $(id -un) == "$ADMIN_USER" ]] || {
    echo "Запустите скрипт от $ADMIN_USER на $EDGE_HOST." >&2; exit 1;
}
[[ $SPARK_HOME == /opt/* && $SPARK_HOME != "$HADOOP_HOME" && $CONF_DIR != "$HADOOP_CONF_DIR" &&
   $DATA_ROOT == /srv/* && $LOG_DIR == /var/log/* ]] || { echo 'Проверьте отдельные пути Spark.' >&2; exit 1; }
[[ ! -e $SPARK_HOME || -L $SPARK_HOME ]] || { echo 'SPARK_HOME должен быть отдельной символьной ссылкой.' >&2; exit 1; }
for option in HMS_PORT DRIVER_MEMORY_MB EXECUTOR_MEMORY_MB EXECUTOR_OVERHEAD_MB EXECUTOR_COUNT EXECUTOR_CORES AM_MEMORY_MB AM_OVERHEAD_MB SHUFFLE_PARTITIONS; do
    [[ ${!option} =~ ^[1-9][0-9]*$ ]] || { echo "$option должен быть положительным целым числом." >&2; exit 1; }
done
(( HMS_PORT <= 65535 && DRIVER_MEMORY_MB >= 450 && EXECUTOR_MEMORY_MB >= 450 )) || {
    echo 'Проверьте порт Metastore и память JVM Spark (не меньше 450 МиБ).' >&2; exit 1;
}
for path in "$HDFS_ROOT" "$EVENT_LOG_DIR" "$STAGING_DIR" "$WAREHOUSE_DIR" "$EXTERNAL_DIR"; do
    [[ $path =~ ^/[a-zA-Z0-9_-]+(/[a-zA-Z0-9_-]+)*$ ]] || { echo 'Неверный путь HDFS.' >&2; exit 1; }
done
[[ $HDFS_ROOT != / && $EVENT_LOG_DIR == "$HDFS_ROOT/"* && $STAGING_DIR == "$HDFS_ROOT/"* &&
   $WAREHOUSE_DIR == "$HDFS_ROOT/"* && $EXTERNAL_DIR == "$HDFS_ROOT/"* ]] || { echo 'Каталоги Spark должны быть внутри HDFS_ROOT.' >&2; exit 1; }
[[ -x $JAVA_HOME/bin/java && -x $PYTHON && -f $HADOOP_CONF_DIR/yarn-site.xml ]]
sudo -n true
id hadoop >/dev/null
python3 - "$HADOOP_CONF_DIR/yarn-site.xml" "$EXECUTOR_MEMORY_MB" "$EXECUTOR_OVERHEAD_MB" "$EXECUTOR_CORES" "$AM_MEMORY_MB" "$AM_OVERHEAD_MB" <<'PY'
import sys,xml.etree.ElementTree as E
p={x.findtext('name'):x.findtext('value') for x in E.parse(sys.argv[1]).getroot().findall('property')}
limit=int(p.get('yarn.scheduler.maximum-allocation-mb',8192)); cores=int(p.get('yarn.scheduler.maximum-allocation-vcores',4))
executor,overhead,ncores,am,amoverhead=map(int,sys.argv[2:])
if executor+overhead>limit or am+amoverhead>limit or ncores>cores:
    raise SystemExit('Контейнер Spark превышает лимит YARN.')
PY
hdfs=(sudo -u hadoop "$HADOOP_HOME/bin/hdfs" --config "$HADOOP_CONF_DIR")
fs=$("${hdfs[@]}" getconf -confKey fs.defaultFS)
replication=$("${hdfs[@]}" getconf -confKey dfs.replication)
[[ $fs =~ ^hdfs://[a-zA-Z0-9.-]+(:[0-9]+)?/?$ && $replication =~ ^[1-9][0-9]*$ ]]
"${hdfs[@]}" dfs -test -d /
mkdir -p "$(dirname "$ARCHIVE")"
version=3.5.7
checksum=f3b7d5974d746b9aaecb19104473da91068b698a4d292177deb75deb83ef9dc7eb77062446940561ac9ab7ee3336fb421332b1c877292dab4ac1b6ca30f4f2e0
if [[ ! -f $ARCHIVE ]]; then
    curl -fL --retry 3 "https://archive.apache.org/dist/spark/spark-$version/spark-$version-bin-hadoop3.tgz" -o "$ARCHIVE.part"
    mv "$ARCHIVE.part" "$ARCHIVE"
fi
printf '%s  %s\n' "$checksum" "$ARCHIVE" | sha512sum -c -
if [[ ! -x /opt/spark-$version-bin-hadoop3/bin/spark-submit ]]; then
    sudo tar -xzf "$ARCHIVE" -C /opt
    sudo chown -R root:root "/opt/spark-$version-bin-hadoop3"
fi
sudo ln -sfn "/opt/spark-$version-bin-hadoop3" "$SPARK_HOME"
sudo install -d -m 755 "$CONF_DIR"
sudo install -d -o hadoop -g hadoop -m 750 "$DATA_ROOT" "$DATA_ROOT/tmp" "$LOG_DIR"
"${hdfs[@]}" dfs -mkdir -p "$HDFS_ROOT" "$EVENT_LOG_DIR" "$STAGING_DIR" "$WAREHOUSE_DIR" "$EXTERNAL_DIR"
"${hdfs[@]}" dfs -chmod 700 "$HDFS_ROOT" "$EVENT_LOG_DIR" "$STAGING_DIR" "$WAREHOUSE_DIR" "$EXTERNAL_DIR"
stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT
cat > "$stage/spark-defaults.conf" <<EOF
spark.master yarn
spark.submit.deployMode client
spark.driver.host $EDGE_IP
spark.driver.bindAddress $EDGE_IP
spark.driver.memory ${DRIVER_MEMORY_MB}m
spark.executor.memory ${EXECUTOR_MEMORY_MB}m
spark.executor.memoryOverhead $EXECUTOR_OVERHEAD_MB
spark.executor.instances $EXECUTOR_COUNT
spark.executor.cores $EXECUTOR_CORES
spark.yarn.am.memory ${AM_MEMORY_MB}m
spark.yarn.am.memoryOverhead $AM_OVERHEAD_MB
spark.dynamicAllocation.enabled false
spark.shuffle.service.enabled false
spark.sql.shuffle.partitions $SHUFFLE_PARTITIONS
spark.default.parallelism $SHUFFLE_PARTITIONS
spark.sql.warehouse.dir ${fs%/}$WAREHOUSE_DIR
spark.yarn.stagingDir ${fs%/}$STAGING_DIR
spark.yarn.submit.file.replication $replication
spark.yarn.populateHadoopClasspath false
spark.hadoop.hive.metastore.uris thrift://$HMS_HOST:$HMS_PORT
spark.sql.hive.metastore.version 2.3.9
spark.sql.hive.metastore.jars builtin
spark.eventLog.enabled true
spark.eventLog.dir ${fs%/}$EVENT_LOG_DIR
spark.eventLog.compress false
spark.eventLog.rolling.enabled false
spark.ui.enabled false
spark.pyspark.python $PYTHON
spark.pyspark.driver.python $PYTHON
EOF
python3 - "$HMS_HOST" "$HMS_PORT" "$stage/hive-site.xml" <<'PY'
import sys,xml.etree.ElementTree as E
root=E.Element('configuration'); p=E.SubElement(root,'property')
E.SubElement(p,'name').text='hive.metastore.uris'; E.SubElement(p,'value').text=f'thrift://{sys.argv[1]}:{sys.argv[2]}'
E.indent(root,space='  '); E.ElementTree(root).write(sys.argv[3],encoding='UTF-8',xml_declaration=True)
PY
sudo install -m 644 "$stage/spark-defaults.conf" "$stage/hive-site.xml" "$CONF_DIR/"
echo 'Spark настроен для YARN и отдельного Metastore Hive.'
