#!/usr/bin/env bash
set -euo pipefail

[[ $# -ge 2 ]] || { echo 'Запуск: run-service.sh runtime.env hms|hs2|client|schema [аргументы]' >&2; exit 2; }
source "$1"
role=$2
shift 2
export JAVA_HOME HADOOP_HOME HADOOP_CONF_DIR HIVE_HOME
case "$role" in
    hms) conf=$CONF_DIR/metastore; heap=$HMS_HEAP_MB; main=org.apache.hadoop.hive.metastore.HiveMetaStore ;;
    hs2) conf=$CONF_DIR/server; heap=$HS2_HEAP_MB; main=org.apache.hive.service.server.HiveServer2 ;;
    client) conf=$CONF_DIR/server; heap=$CLIENT_HEAP_MB; main=org.apache.hive.beeline.BeeLine ;;
    schema) conf=$CONF_DIR/metastore; heap=$HMS_HEAP_MB; main=org.apache.hive.beeline.schematool.HiveSchemaTool ;;
    *) echo "Неизвестная роль: $role." >&2; exit 2 ;;
esac
export HIVE_CONF_DIR=$conf
classpath=$("$HADOOP_HOME/bin/hadoop" --config "$HADOOP_CONF_DIR" classpath --glob)
exec "$JAVA_HOME/bin/java" -Xmx"${heap}m" -Djava.io.tmpdir="$DATA_ROOT/tmp" \
    -Dhive.home="$HIVE_HOME" -Dhive.log.dir="$LOG_DIR" -Dhive.log.file="$role.log" \
    -cp "$conf:$HIVE_HOME/lib/*:$classpath" "$main" "$@"
