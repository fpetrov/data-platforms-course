#!/usr/bin/env bash
set -euo pipefail

task_dir=$(cd "$(dirname "$0")/.." && pwd)
[[ $# -le 2 ]] || { echo 'Запуск: run-job.sh [cluster.env] [database]' >&2; exit 2; }
CLUSTER_ENV=$(realpath "${1:-${CLUSTER_ENV:-$task_dir/cluster.env}}")
export CLUSTER_ENV
source "$CLUSTER_ENV"
database=${2:-homework04}
[[ $(hostname -s) == "$EDGE_HOST" && $(id -un) == "$ADMIN_USER" ]] || {
    echo "Запустите скрипт от $ADMIN_USER на $EDGE_HOST." >&2; exit 1;
}
[[ $database =~ ^[a-z][a-z0-9_]{0,62}$ && $database != default && $database != sys && $database != information_schema ]] || {
    echo 'Неверное имя учебной базы.' >&2; exit 1;
}
for path in "$HDFS_ROOT" "$EXTERNAL_DIR"; do
    [[ $path =~ ^/[a-zA-Z0-9_-]+(/[a-zA-Z0-9_-]+)*$ ]] || { echo 'Неверный путь HDFS.' >&2; exit 1; }
done
[[ $EXTERNAL_DIR == "$HDFS_ROOT/"* ]] || { echo 'EXTERNAL_DIR должен быть внутри HDFS_ROOT.' >&2; exit 1; }
hdfs=(sudo -u hadoop "$HADOOP_HOME/bin/hdfs" --config "$HADOOP_CONF_DIR")
fs=$("${hdfs[@]}" getconf -confKey fs.defaultFS)
[[ $fs =~ ^hdfs://[a-zA-Z0-9.-]+(:[0-9]+)?/?$ ]] || { echo 'Ожидается HDFS в fs.defaultFS.' >&2; exit 1; }
"${hdfs[@]}" dfs -test -f "${fs%/}$HDFS_ROOT/input/$database/orders.csv" || {
    echo 'Сначала загрузите файл через load-data.sh.' >&2; exit 1;
}
bash "$task_dir/scripts/submit.sh" "$task_dir/jobs/orders.py" \
    --root "${fs%/}$HDFS_ROOT" --external "${fs%/}$EXTERNAL_DIR" --database "$database"
stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT
cat > "$stage/result.hql" <<'SQL'
SET hive.query.results.cache.enabled=false;
SELECT city, orders_count, total_amount, avg_amount, order_date
FROM `${hivevar:database}`.orders_daily
WHERE order_date BETWEEN DATE '2026-10-01' AND DATE '2026-10-03'
ORDER BY order_date, city;
SQL
echo 'Результат через Beeline после остановки Spark:'
CLUSTER_ENV="$HIVE_CLUSTER_ENV" bash "$HIVE_TASK_DIR/scripts/client.sh" \
    --hivevar "database=$database" -f "$stage/result.hql"
