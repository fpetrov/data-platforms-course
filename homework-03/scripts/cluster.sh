#!/usr/bin/env bash
set -euo pipefail

task_dir=$(cd "$(dirname "$0")/.." && pwd)
action=${1:-status}
export CLUSTER_ENV
CLUSTER_ENV=$(realpath "${2:-${CLUSTER_ENV:-$task_dir/cluster.env}}")
source "$CLUSTER_ENV"
[[ $(hostname -s) == "$EDGE_HOST" && $(id -un) == "$ADMIN_USER" ]] || {
    echo "Запустите скрипт от $ADMIN_USER на $EDGE_HOST." >&2; exit 1;
}
ssh_opts=(-n -i "$ADMIN_KEY" -o BatchMode=yes -o StrictHostKeyChecking=yes -o ConnectTimeout=10)
metastore() { ssh "${ssh_opts[@]}" "$ADMIN_USER@$HMS_HOST" "sudo -n systemctl $*"; }
start() {
    metastore start hive-practice3-metastore
    python3 - "$HMS_HOST" "$HMS_PORT" <<'PY'
import socket,sys,time
for attempt in range(120):
    try:
        with socket.create_connection((sys.argv[1], int(sys.argv[2])), timeout=2): break
    except OSError: time.sleep(1)
else: raise SystemExit('Metastore не отвечает. Проверьте journalctl -u hive-practice3-metastore.')
PY
    sudo -n systemctl start hive-practice3-server
    deadline=$((SECONDS + 120))
    while (( SECONDS < deadline )); do
        if timeout -k 3s 15s bash "$task_dir/scripts/client.sh" -e 'SET hive.fetch.task.conversion=more; SELECT 1;' </dev/null >/dev/null 2>&1; then
            echo 'HiveServer2 и Metastore готовы.'; return
        fi
        sleep 2
    done
    echo 'HiveServer2 не отвечает. Проверьте journalctl -u hive-practice3-server.' >&2
    return 1
}
stop() {
    sudo -n systemctl stop hive-practice3-server
    metastore stop hive-practice3-metastore "postgresql@$PG_VERSION-$PG_CLUSTER"
}
case "$action" in
    start) start ;;
    stop) stop ;;
    restart) stop; start ;;
    status)
        sudo -n systemctl is-active hive-practice3-server
        metastore is-active hive-practice3-metastore "postgresql@$PG_VERSION-$PG_CLUSTER"
        timeout -k 3s 30s bash "$task_dir/scripts/client.sh" -e 'SET hive.fetch.task.conversion=more; SELECT 1;' </dev/null
        ;;
    *) echo 'Запуск: cluster.sh start|stop|restart|status [cluster.env]' >&2; exit 2 ;;
esac
