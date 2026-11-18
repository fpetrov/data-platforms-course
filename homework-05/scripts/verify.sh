#!/usr/bin/env bash
set -euo pipefail

task_dir=$(cd "$(dirname "$0")/.." && pwd)
[[ $# -le 1 ]] || { echo 'Запуск: verify.sh [cluster.env]' >&2; exit 2; }
CLUSTER_ENV=$(realpath "${1:-${CLUSTER_ENV:-$task_dir/cluster.env}}")
set -a
source "$CLUSTER_ENV"
set +a
export CLUSTER_ENV
source "$task_dir/scripts/prefect-env.sh"
export PREFECT_LOGGING_LEVEL=ERROR
exec "$PREFECT_VENV/bin/python" -u "$task_dir/scripts/check-flow.py"
