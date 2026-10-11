#!/usr/bin/env bash
set -euo pipefail

[[ $EUID -eq 0 && $# -eq 1 ]] || { echo 'Запуск: sudo bash setup-node.sh cluster.env' >&2; exit 1; }
source "$1"
[[ $CONF_DIR != "$HDFS_CONF_DIR" ]] || { echo 'Конфиги HDFS и YARN должны быть раздельными.' >&2; exit 1; }
for file in core-site.xml hdfs-site.xml log4j.properties; do
    [[ -f $HDFS_CONF_DIR/$file ]] || { echo "Сначала выполните работу 1: нет $HDFS_CONF_DIR/$file." >&2; exit 1; }
done
[[ -x $HADOOP_HOME/bin/yarn ]] && id hadoop >/dev/null

host=$(hostname -s)
case "$host" in
    "$EDGE_HOST") ip=$EDGE_IP ;;
    "$NAMENODE_HOST") ip=$NAMENODE_IP ;;
    "$DATANODE0_HOST") ip=$DATANODE0_IP ;;
    "$DATANODE1_HOST") ip=$DATANODE1_IP ;;
    *) echo "Неизвестный узел: $host." >&2; exit 1 ;;
esac
sed -i '/^# BEGIN yarn-practice2$/,/^# END yarn-practice2$/d' /etc/hosts
cat >> /etc/hosts <<EOF
# BEGIN yarn-practice2
$EDGE_IP $EDGE_HOST
$NAMENODE_IP $NAMENODE_HOST
$DATANODE0_IP $DATANODE0_HOST
$DATANODE1_IP $DATANODE1_HOST
# END yarn-practice2
EOF

install -d -m 755 "$CONF_DIR"
for file in core-site.xml hdfs-site.xml log4j.properties; do
    install -m 644 "$HDFS_CONF_DIR/$file" "$CONF_DIR/$file"
done
install -d -o hadoop -g hadoop -m 750 "$DATA_ROOT" "$LOG_DIR"
for dir in pids local container-logs timeline; do
    install -d -o hadoop -g hadoop -m 700 "$DATA_ROOT/$dir"
done
cat > "$CONF_DIR/hadoop-env.sh" <<EOF
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export HADOOP_YARN_HOME=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_CONF_DIR=$CONF_DIR
export HADOOP_LOG_DIR=$LOG_DIR
export HADOOP_PID_DIR=$DATA_ROOT/pids
export HADOOP_HEAPSIZE_MAX=$DAEMON_HEAP_MB
export HADOOP_SSH_OPTS="-i $HADOOP_KEY -o BatchMode=yes -o StrictHostKeyChecking=yes -o UserKnownHostsFile=$HADOOP_KNOWN_HOSTS"
EOF
printf '%s\n' "$NAMENODE_HOST" "$DATANODE0_HOST" "$DATANODE1_HOST" > "$CONF_DIR/workers"
cat > "$CONF_DIR/yarn-site.xml" <<EOF
<?xml version="1.0"?>
<configuration>
  <property><name>yarn.resourcemanager.hostname</name><value>$EDGE_HOST</value></property>
  <property><name>yarn.resourcemanager.bind-host</name><value>$EDGE_IP</value></property>
  <property><name>yarn.resourcemanager.webapp.address</name><value>$EDGE_HOST:8088</value></property>
  <property><name>yarn.resourcemanager.scheduler.class</name><value>org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler</value></property>
  <property><name>yarn.scheduler.minimum-allocation-mb</name><value>$CONTAINER_MIN_MB</value></property>
  <property><name>yarn.scheduler.maximum-allocation-mb</name><value>$CONTAINER_MAX_MB</value></property>
  <property><name>yarn.scheduler.maximum-allocation-vcores</name><value>$NODE_VCORES</value></property>
  <property><name>yarn.nodemanager.hostname</name><value>$host</value></property>
  <property><name>yarn.nodemanager.bind-host</name><value>$ip</value></property>
  <property><name>yarn.nodemanager.address</name><value>$host:8041</value></property>
  <property><name>yarn.nodemanager.localizer.address</name><value>$host:8040</value></property>
  <property><name>yarn.nodemanager.webapp.address</name><value>$host:8042</value></property>
  <property><name>yarn.nodemanager.resource.memory-mb</name><value>$NODE_MEMORY_MB</value></property>
  <property><name>yarn.nodemanager.resource.cpu-vcores</name><value>$NODE_VCORES</value></property>
  <property><name>yarn.nodemanager.pmem-check-enabled</name><value>true</value></property>
  <property><name>yarn.nodemanager.vmem-check-enabled</name><value>false</value></property>
  <property><name>yarn.nodemanager.local-dirs</name><value>$DATA_ROOT/local</value></property>
  <property><name>yarn.nodemanager.log-dirs</name><value>$DATA_ROOT/container-logs</value></property>
  <property><name>yarn.nodemanager.aux-services</name><value>mapreduce_shuffle</value></property>
  <property><name>yarn.nodemanager.env-whitelist</name><value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_HOME,PATH,LANG,TZ,HADOOP_MAPRED_HOME</value></property>
  <property><name>yarn.log-aggregation-enable</name><value>true</value></property>
  <property><name>yarn.nodemanager.remote-app-log-dir</name><value>/yarn-logs</value></property>
  <property><name>yarn.log-aggregation.retain-seconds</name><value>604800</value></property>
  <property><name>yarn.log.server.url</name><value>http://$EDGE_HOST:19888/jobhistory/logs</value></property>
  <property><name>yarn.web-proxy.address</name><value>$EDGE_HOST:9099</value></property>
  <property><name>yarn.web-proxy.bind-host</name><value>$EDGE_IP</value></property>
  <property><name>yarn.timeline-service.enabled</name><value>true</value></property>
  <property><name>yarn.timeline-service.version</name><value>1.0</value></property>
  <property><name>yarn.timeline-service.hostname</name><value>$EDGE_HOST</value></property>
  <property><name>yarn.timeline-service.bind-host</name><value>$EDGE_IP</value></property>
  <property><name>yarn.timeline-service.webapp.address</name><value>$EDGE_HOST:8188</value></property>
  <property><name>yarn.timeline-service.generic-application-history.enabled</name><value>true</value></property>
  <property><name>yarn.resourcemanager.system-metrics-publisher.enabled</name><value>true</value></property>
  <property><name>yarn.timeline-service.leveldb-timeline-store.path</name><value>$DATA_ROOT/timeline</value></property>
  <property><name>yarn.timeline-service.leveldb-timeline-store.read-cache-size</name><value>10485760</value></property>
</configuration>
EOF
cat > "$CONF_DIR/mapred-site.xml" <<EOF
<?xml version="1.0"?>
<configuration>
  <property><name>mapreduce.framework.name</name><value>yarn</value></property>
  <property><name>mapreduce.application.classpath</name><value>\$HADOOP_HOME/share/hadoop/mapreduce/*:\$HADOOP_HOME/share/hadoop/mapreduce/lib/*</value></property>
  <property><name>yarn.app.mapreduce.am.resource.mb</name><value>$TASK_MEMORY_MB</value></property>
  <property><name>yarn.app.mapreduce.am.command-opts</name><value>-Xmx${TASK_HEAP_MB}m</value></property>
  <property><name>mapreduce.map.memory.mb</name><value>$TASK_MEMORY_MB</value></property>
  <property><name>mapreduce.reduce.memory.mb</name><value>$TASK_MEMORY_MB</value></property>
  <property><name>mapreduce.map.java.opts</name><value>-Xmx${TASK_HEAP_MB}m</value></property>
  <property><name>mapreduce.reduce.java.opts</name><value>-Xmx${TASK_HEAP_MB}m</value></property>
  <property><name>mapreduce.task.io.sort.mb</name><value>64</value></property>
  <property><name>mapreduce.jobhistory.address</name><value>$EDGE_HOST:10020</value></property>
  <property><name>mapreduce.jobhistory.webapp.address</name><value>$EDGE_HOST:19888</value></property>
  <property><name>mapreduce.jobhistory.admin.address</name><value>$EDGE_HOST:10033</value></property>
  <property><name>mapreduce.jobhistory.intermediate-done-dir</name><value>/mr-history/tmp</value></property>
  <property><name>mapreduce.jobhistory.done-dir</name><value>/mr-history/done</value></property>
  <property><name>mapreduce.jobhistory.move.interval-ms</name><value>5000</value></property>
  <property><name>mapreduce.job.emit-timeline-data</name><value>true</value></property>
</configuration>
EOF
cat > "$CONF_DIR/capacity-scheduler.xml" <<EOF
<?xml version="1.0"?>
<configuration>
  <property><name>yarn.scheduler.capacity.root.queues</name><value>default</value></property>
  <property><name>yarn.scheduler.capacity.root.default.capacity</name><value>100</value></property>
  <property><name>yarn.scheduler.capacity.root.default.maximum-capacity</name><value>100</value></property>
  <property><name>yarn.scheduler.capacity.root.default.user-limit-factor</name><value>1</value></property>
  <property><name>yarn.scheduler.capacity.maximum-am-resource-percent</name><value>0.25</value></property>
</configuration>
EOF
chmod 644 "$CONF_DIR"/*.xml "$CONF_DIR/hadoop-env.sh" "$CONF_DIR/workers"
