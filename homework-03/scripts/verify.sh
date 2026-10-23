#!/usr/bin/env bash
set -euo pipefail

task_dir=$(cd "$(dirname "$0")/.." && pwd)
[[ $# -le 2 && (${2:-} == '' || ${2:-} == --restart) ]] || {
    echo 'Запуск: verify.sh [cluster.env] [--restart]' >&2; exit 2;
}
CLUSTER_ENV=$(realpath "${1:-${CLUSTER_ENV:-$task_dir/cluster.env}}")
set -a
source "$CLUSTER_ENV"
set +a
export CLUSTER_ENV
[[ $(hostname -s) == "$EDGE_HOST" && $(id -un) == "$ADMIN_USER" ]] || {
    echo "Запустите скрипт от $ADMIN_USER на $EDGE_HOST." >&2; exit 1;
}
args=()
[[ ${2:-} != --restart ]] || args+=(--restart)
exec python3 -u "$task_dir/scripts/check-hive.py" "${args[@]}"
