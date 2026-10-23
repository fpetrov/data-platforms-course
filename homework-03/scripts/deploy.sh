#!/usr/bin/env bash
set -euo pipefail

task_dir=$(cd "$(dirname "$0")/.." && pwd)
config=$(realpath "${1:-${CLUSTER_ENV:-$task_dir/cluster.env}}")
source "$config"
[[ $(hostname -s) == "$EDGE_HOST" && $(id -un) == "$ADMIN_USER" ]] || {
    echo "Запустите скрипт от $ADMIN_USER на $EDGE_HOST." >&2; exit 1;
}
[[ $HIVE_HOME != "$HADOOP_HOME" && $CONF_DIR != "$HADOOP_CONF_DIR" &&
   $HDFS_ROOT == /* && $HDFS_ROOT != / && $WAREHOUSE_DIR == "$HDFS_ROOT/"* &&
   $EXTERNAL_DIR == "$HDFS_ROOT/"* && $HIVE_SCRATCH_DIR == /* && $HIVE_SCRATCH_DIR != / ]] || {
    echo 'Проверьте отдельные пути Hive и каталоги HDFS.' >&2; exit 1;
}
for option in HMS_PORT HS2_PORT HS2_WEB_PORT PG_PORT PG_VERSION HMS_HEAP_MB HS2_HEAP_MB CLIENT_HEAP_MB TASK_MEMORY_MB TASK_HEAP_MB; do
    [[ ${!option} =~ ^[1-9][0-9]*$ ]] || { echo "$option должен быть положительным целым числом." >&2; exit 1; }
done
for option in HMS_PORT HS2_PORT HS2_WEB_PORT PG_PORT; do
    (( ${!option} <= 65535 )) || { echo "$option должен быть не больше 65535." >&2; exit 1; }
done
(( HS2_PORT != HS2_WEB_PORT && HMS_PORT != PG_PORT )) || { echo 'Порты служб на одном узле должны различаться.' >&2; exit 1; }
max_memory=$(python3 - "$HADOOP_CONF_DIR/yarn-site.xml" <<'PY'
import sys,xml.etree.ElementTree as E
props={p.findtext('name'):p.findtext('value') for p in E.parse(sys.argv[1]).getroot().findall('property')}
print(int(props.get('yarn.scheduler.maximum-allocation-mb',8192)))
PY
)
(( TASK_HEAP_MB < TASK_MEMORY_MB && TASK_MEMORY_MB <= max_memory )) || { echo 'Нужны heap < task <= максимальный контейнер YARN.' >&2; exit 1; }
[[ $PG_CLUSTER =~ ^[a-z][a-z0-9_]*$ && $PG_DATABASE =~ ^[a-z][a-z0-9_]*$ && $PG_USER =~ ^[a-z][a-z0-9_]*$ ]] || {
    echo 'Для PostgreSQL используйте строчные латинские буквы, цифры и _. Начинайте с буквы.' >&2; exit 1;
}
ssh_opts=(-i "$ADMIN_KEY" -o BatchMode=yes -o StrictHostKeyChecking=yes -o ConnectTimeout=10)
sudo -n true
ssh "${ssh_opts[@]}" "$ADMIN_USER@$HMS_HOST" 'sudo -n true'
sudo -u hadoop "$HADOOP_HOME/bin/hdfs" --config "$HADOOP_CONF_DIR" dfs -test -d /
for target in local "$ADMIN_USER@$HMS_HOST"; do
    if [[ $target == local ]]; then
        running=$(systemctl is-active hive-practice3-server || true)
    else
        running=$(ssh "${ssh_opts[@]}" "$target" 'systemctl is-active hive-practice3-metastore || true')
    fi
    [[ $running != active && $running != activating ]] || { echo 'Сначала остановите Hive через cluster.sh stop.' >&2; exit 1; }
done
mkdir -p "$(dirname "$ARCHIVE")"
if [[ ! -f $ARCHIVE ]]; then
    curl -fL --retry 3 https://archive.apache.org/dist/hive/hive-4.0.0-alpha-2/apache-hive-4.0.0-alpha-2-bin.tar.gz -o "$ARCHIVE.part"
    mv "$ARCHIVE.part" "$ARCHIVE"
fi
remote_stage=$(ssh "${ssh_opts[@]}" "$ADMIN_USER@$HMS_HOST" 'mktemp -d /tmp/hive-practice3.XXXXXX')
trap 'ssh "${ssh_opts[@]}" "$ADMIN_USER@$HMS_HOST" "rm -rf '\''$remote_stage'\''"' EXIT
scp "${ssh_opts[@]}" "$config" "$ADMIN_USER@$HMS_HOST:$remote_stage/cluster.env"
scp "${ssh_opts[@]}" "$task_dir/scripts/setup-node.sh" "$task_dir/scripts/run-service.sh" "$ADMIN_USER@$HMS_HOST:$remote_stage/"
scp "${ssh_opts[@]}" "$ARCHIVE" "$ADMIN_USER@$HMS_HOST:$remote_stage/hive.tar.gz"
ssh "${ssh_opts[@]}" "$ADMIN_USER@$HMS_HOST" "sudo bash '$remote_stage/setup-node.sh' '$remote_stage/cluster.env' '$remote_stage/hive.tar.gz' '$remote_stage/run-service.sh' hms"
sudo bash "$task_dir/scripts/setup-node.sh" "$config" "$ARCHIVE" "$task_dir/scripts/run-service.sh" hs2
hdfs=(sudo -u hadoop "$HADOOP_HOME/bin/hdfs" --config "$HADOOP_CONF_DIR" dfs)
"${hdfs[@]}" -mkdir -p "$WAREHOUSE_DIR" "$EXTERNAL_DIR" "$HIVE_SCRATCH_DIR"
"${hdfs[@]}" -chmod 700 "$HDFS_ROOT" "$WAREHOUSE_DIR" "$EXTERNAL_DIR"
"${hdfs[@]}" -chmod 733 "$HIVE_SCRATCH_DIR"
CLUSTER_ENV="$config" bash "$task_dir/scripts/cluster.sh" start "$config"
