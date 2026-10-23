#!/usr/bin/env bash
set -euo pipefail

[[ $# -le 2 ]] || { echo 'Запуск: tunnel.sh [user@edge] [ключ]' >&2; exit 2; }
task_dir=$(cd "$(dirname "$0")/.." && pwd)
config=${CLUSTER_ENV:-$task_dir/cluster.env}
[[ -f $config ]] || config=$task_dir/cluster.env.example
source "$config"
target=${1:-${SSH_TARGET:-team@2.59.83.239}}
key=${2:-${SSH_KEY:-$HOME/.ssh/id_ed25519_team30}}
[[ -f $key ]] || { echo "Нет SSH-ключа: $key" >&2; exit 1; }
echo "JDBC: jdbc:hive2://127.0.0.1:$HS2_PORT/default; Web: http://127.0.0.1:$HS2_WEB_PORT."
exec ssh -i "$key" -N -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 -o ServerAliveCountMax=3 \
    -L "127.0.0.1:$HS2_PORT:127.0.0.1:$HS2_PORT" \
    -L "127.0.0.1:$HS2_WEB_PORT:127.0.0.1:$HS2_WEB_PORT" "$target"
