#!/usr/bin/env bash
set -euo pipefail

task_dir=$(cd "$(dirname "$0")/.." && pwd)
[[ $# -le 1 ]] || { echo 'Запуск: run-flow.sh [database]' >&2; exit 2; }
CLUSTER_ENV=$(realpath "${CLUSTER_ENV:-$task_dir/cluster.env}")
export CLUSTER_ENV
set -a
source "$CLUSTER_ENV"
set +a
[[ $HDFS_ROOT == /user/hadoop/homework-05 && $EXTERNAL_DIR == "$HDFS_ROOT/external" ]] || {
    echo 'Укажите отдельные каталоги работы 5.' >&2; exit 1;
}
source "$task_dir/scripts/prefect-env.sh"
[[ -x $PREFECT_VENV/bin/python ]] || { echo 'Сначала выполните deploy.sh.' >&2; exit 1; }
exec "$PREFECT_VENV/bin/python" "$task_dir/flows/orders.py" \
    --config "$CLUSTER_ENV" --database "${1:-homework05}"
