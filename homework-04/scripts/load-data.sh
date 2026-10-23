#!/usr/bin/env bash
set -euo pipefail

task_dir=$(cd "$(dirname "$0")/.." && pwd)
[[ $# -le 2 ]] || { echo 'Запуск: load-data.sh [cluster.env] [database]' >&2; exit 2; }
source "${1:-${CLUSTER_ENV:-$task_dir/cluster.env}}"
database=${2:-homework04}
[[ $(hostname -s) == "$EDGE_HOST" && $(id -un) == "$ADMIN_USER" ]] || {
    echo "Запустите скрипт от $ADMIN_USER на $EDGE_HOST." >&2; exit 1;
}
[[ $database =~ ^[a-z][a-z0-9_]{0,62}$ && $database != default && $database != sys && $database != information_schema ]] || {
    echo 'Неверное имя учебной базы.' >&2; exit 1;
}
[[ $HDFS_ROOT =~ ^/[a-zA-Z0-9_-]+(/[a-zA-Z0-9_-]+)*$ ]] || { echo 'Неверный путь HDFS.' >&2; exit 1; }
source_file=$HIVE_TASK_DIR/data/orders.csv
python3 "$HIVE_TASK_DIR/scripts/check-load.py" input "$source_file"
hdfs=(sudo -u hadoop "$HADOOP_HOME/bin/hdfs" --config "$HADOOP_CONF_DIR")
fs=$("${hdfs[@]}" getconf -confKey fs.defaultFS)
[[ $fs =~ ^hdfs://[a-zA-Z0-9.-]+(:[0-9]+)?/?$ ]] || { echo 'Ожидается HDFS в fs.defaultFS.' >&2; exit 1; }
input=${fs%/}$HDFS_ROOT/input/$database/orders.csv
if "${hdfs[@]}" dfs -test -e "$input" && ! "${hdfs[@]}" dfs -test -f "$input"; then
    echo 'Путь входного файла занят каталогом.' >&2; exit 1
fi
"${hdfs[@]}" dfs -mkdir -p "${input%/*}"
"${hdfs[@]}" dfs -put -f - "$input" < "$source_file"
echo "Входной файл загружен: $input"
"${hdfs[@]}" dfs -cat "$input" | sha256sum
