#!/usr/bin/env bash
set -euo pipefail

task_dir=$(cd "$(dirname "$0")/.." && pwd)
source "${CLUSTER_ENV:-$task_dir/cluster.env}"
[[ $(hostname -s) == "$EDGE_HOST" ]] || { echo 'Запустите клиент на edge.' >&2; exit 1; }
args=()
file=
username=hadoop
while [[ $# -gt 0 ]]; do
    case "$1" in
        -f|--file)
            [[ $# -ge 2 && -z $file ]] || { echo 'Укажите один SQL-файл после -f.' >&2; exit 2; }
            file=$2; shift 2 ;;
        -n|--name)
            [[ $# -ge 2 ]] || { echo 'Укажите имя клиента после -n.' >&2; exit 2; }
            username=$2; shift 2 ;;
        *) args+=("$1"); shift ;;
    esac
done
if [[ -n $file ]]; then
    sql_copy=$(sudo -u hadoop mktemp /tmp/hive-client.XXXXXX.sql)
    trap 'sudo -u hadoop rm -f "$sql_copy"' EXIT
    sudo -u hadoop tee "$sql_copy" < "$file" >/dev/null
    args+=(-f "$sql_copy")
fi
command=(sudo -u hadoop "$CONF_DIR/run-service.sh" "$CONF_DIR/runtime.env" client
    -u "jdbc:hive2://127.0.0.1:$HS2_PORT/default" -n "$username"
    --silent=true --showHeader=false --outputformat=tsv2 "${args[@]}")
if [[ -n $file ]]; then
    "${command[@]}"
else
    exec "${command[@]}"
fi
