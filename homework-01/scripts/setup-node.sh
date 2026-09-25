#!/usr/bin/env bash
set -euo pipefail

[[ $EUID -eq 0 && $# -eq 2 ]] || { echo 'Запуск: sudo bash setup-node.sh cluster.env архив' >&2; exit 1; }
source "$1"
archive=$2
version=3.4.3
checksum=e25be7e57b4d3c5bfe83895844321a21d6cf7331266d524d8983c27cf484e576c3d79b3b60d590f8cddecf16229d0e232de2491b1b61b362ee7d67072c7290e1
printf '%s  %s\n' "$checksum" "$archive" | sha512sum -c -

export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq openjdk-11-jdk-headless openssh-server curl
id hadoop >/dev/null 2>&1 || useradd -m -s /bin/bash hadoop
if LC_ALL=C sudo -l -U hadoop 2>&1 | grep -q 'may run the following commands'; then
    echo 'У пользователя hadoop не должно быть sudo.' >&2
    exit 1
fi

sed -i '/^# BEGIN hdfs-practice1$/,/^# END hdfs-practice1$/d' /etc/hosts
cat >> /etc/hosts <<EOF
# BEGIN hdfs-practice1
$EDGE_IP $EDGE_HOST
$NAMENODE_IP $NAMENODE_HOST
$DATANODE0_IP $DATANODE0_HOST
$DATANODE1_IP $DATANODE1_HOST
# END hdfs-practice1
EOF

if [[ ! -x /opt/hadoop-$version/bin/hdfs ]]; then
    tar -xzf "$archive" -C /opt
    chown -R root:root "/opt/hadoop-$version"
fi
ln -sfn "/opt/hadoop-$version" "$HADOOP_HOME"
install -d -m 755 "$CONF_DIR"
install -m 644 "$HADOOP_HOME/etc/hadoop/log4j.properties" "$CONF_DIR/log4j.properties"
install -d -o hadoop -g hadoop -m 750 "$DATA_ROOT" "$LOG_DIR"
for dir in name data checkpoint tmp pids; do
    install -d -o hadoop -g hadoop -m 700 "$DATA_ROOT/$dir"
done
install -d -o hadoop -g hadoop -m 700 /home/hadoop/.ssh

cat > "$CONF_DIR/core-site.xml" <<EOF
<?xml version="1.0"?>
<configuration>
  <property><name>fs.defaultFS</name><value>hdfs://$NAMENODE_HOST:9000</value></property>
  <property><name>hadoop.tmp.dir</name><value>$DATA_ROOT/tmp</value></property>
</configuration>
EOF
cat > "$CONF_DIR/hdfs-site.xml" <<EOF
<?xml version="1.0"?>
<configuration>
  <property><name>dfs.replication</name><value>3</value></property>
  <property><name>dfs.namenode.name.dir</name><value>file://$DATA_ROOT/name</value></property>
  <property><name>dfs.datanode.data.dir</name><value>file://$DATA_ROOT/data</value></property>
  <property><name>dfs.namenode.checkpoint.dir</name><value>file://$DATA_ROOT/checkpoint</value></property>
  <property><name>dfs.namenode.http-address</name><value>$NAMENODE_HOST:9870</value></property>
  <property><name>dfs.namenode.secondary.http-address</name><value>$DATANODE1_HOST:9868</value></property>
</configuration>
EOF
cat > "$CONF_DIR/hadoop-env.sh" <<EOF
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=$HADOOP_HOME
export HADOOP_CONF_DIR=$CONF_DIR
export HADOOP_LOG_DIR=$LOG_DIR
export HADOOP_PID_DIR=$DATA_ROOT/pids
export HADOOP_HEAPSIZE_MAX=768
export HADOOP_SSH_OPTS="-i /home/hadoop/.ssh/id_ed25519_practice1 -o BatchMode=yes -o StrictHostKeyChecking=yes -o UserKnownHostsFile=/home/hadoop/.ssh/known_hosts_practice1"
EOF
printf '%s\n' "$NAMENODE_HOST" "$DATANODE0_HOST" "$DATANODE1_HOST" > "$CONF_DIR/workers"
cat > /etc/profile.d/hdfs-practice1.sh <<EOF
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=$HADOOP_HOME
export HADOOP_CONF_DIR=$CONF_DIR
export PATH=\$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
EOF

if [[ $(hostname -s) == "$NAMENODE_HOST" && ! -f $DATA_ROOT/name/current/VERSION ]]; then
    if [[ -n $(find "$DATA_ROOT/name" -mindepth 1 -print -quit) ]]; then
        echo 'Каталог NameNode не пуст. Форматирование отменено.' >&2
        exit 1
    fi
    sudo -u hadoop "$HADOOP_HOME/bin/hdfs" --config "$CONF_DIR" namenode -format -nonInteractive
fi
