#!/usr/bin/env bash
set -euo pipefail

task_dir=$(cd "$(dirname "$0")/.." && pwd)
[[ $# -le 2 ]] || { echo 'Запуск: load-data.sh [cluster.env] [database]' >&2; exit 2; }
CLUSTER_ENV=$(realpath "${1:-${CLUSTER_ENV:-$task_dir/cluster.env}}")
export CLUSTER_ENV
source "$CLUSTER_ENV"
database=${2:-homework03}
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
python3 "$task_dir/scripts/check-load.py" input "$task_dir/data/orders.csv"
hdfs=(sudo -u hadoop "$HADOOP_HOME/bin/hdfs" --config "$HADOOP_CONF_DIR")
fs=$("${hdfs[@]}" getconf -confKey fs.defaultFS)
[[ $fs =~ ^hdfs://[a-zA-Z0-9.-]+(:[0-9]+)?/?$ ]] || { echo 'Ожидается HDFS в fs.defaultFS.' >&2; exit 1; }
base="${fs%/}$EXTERNAL_DIR/$database"
raw_uri=$base/orders_raw
orders_uri=$base/orders
stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT
client=(bash "$task_dir/scripts/client.sh" --hivevar "database=$database"
    --hivevar "raw_uri=$raw_uri" --hivevar "orders_uri=$orders_uri")

"${client[@]}" -e "SHOW DATABASES LIKE '$database';" > "$stage/databases"
touch "$stage/tables"
if grep -qxF "$database" "$stage/databases"; then
    printf 'SHOW TABLES IN `%s`;\n' "$database" > "$stage/tables.hql"
    "${client[@]}" -f "$stage/tables.hql" > "$stage/tables"
fi
# Сначала проверяем обе таблицы, затем меняем данные.
for table in orders_raw orders; do
    uri=$base/$table
    if grep -qxF "$table" "$stage/tables"; then
        printf 'DESCRIBE FORMATTED `%s`.%s;\n' "$database" "$table" > "$stage/describe.hql"
        "${client[@]}" -f "$stage/describe.hql" > "$stage/describe"
        python3 "$task_dir/scripts/check-load.py" table "$stage/describe" "$uri" "$table"
    elif "${hdfs[@]}" dfs -test -e "$uri"; then
        echo "Каталог $uri уже существует без таблицы. Загрузка отменена." >&2
        exit 1
    fi
done
if grep -qxF orders "$stage/tables"; then
    printf 'SHOW PARTITIONS `%s`.orders;\n' "$database" > "$stage/partitions.hql"
    "${client[@]}" -f "$stage/partitions.hql" > "$stage/partitions"
    expected_parts=()
    touch "$stage/describe-parts.hql"
    for day in 2026-10-01 2026-10-02 2026-10-03; do
        if grep -qxF "order_date=$day" "$stage/partitions"; then
            printf 'DESCRIBE FORMATTED `%s`.orders PARTITION (order_date=%s);\n' \
                "$database" "'$day'" >> "$stage/describe-parts.hql"
            expected_parts+=("$orders_uri/order_date=$day")
        fi
    done
    if [[ ${#expected_parts[@]} -gt 0 ]]; then
        "${client[@]}" -f "$stage/describe-parts.hql" > "$stage/describe-parts"
        python3 "$task_dir/scripts/check-load.py" partitions "$stage/describe-parts" "${expected_parts[@]}"
    fi
fi
if "${hdfs[@]}" dfs -test -d "$raw_uri"; then
    "${hdfs[@]}" dfs -ls "$raw_uri" > "$stage/raw-files"
    if awk 'NF >= 8 {print $8}' "$stage/raw-files" | grep -vxF "$raw_uri/orders.csv"; then
        echo 'В raw-каталоге есть посторонние файлы. Загрузка отменена.' >&2
        exit 1
    fi
fi

"${client[@]}" -f "$task_dir/sql/schema.hql"
"${hdfs[@]}" dfs -mkdir -p "$raw_uri"
"${hdfs[@]}" dfs -put -f - "$raw_uri/orders.csv" < "$task_dir/data/orders.csv"
"${client[@]}" -f "$task_dir/sql/load.hql"
echo "Данные загружены: $database.orders_raw и $database.orders, 9 заказов за три дня."
