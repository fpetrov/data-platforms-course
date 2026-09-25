#!/usr/bin/env bash
set -euo pipefail

task_dir=$(cd "$(dirname "$0")/.." && pwd)
action=${1:-status}
source "${2:-$task_dir/cluster.env}"
[[ $(hostname -s) == "$EDGE_HOST" && $(id -un) == "$ADMIN_USER" ]] || {
    echo "Запустите скрипт от $ADMIN_USER на $EDGE_HOST." >&2; exit 1;
}

case "$action" in
    start|stop)
        sudo -u hadoop "$HADOOP_HOME/sbin/$action-dfs.sh" --config "$CONF_DIR"
        ;;
    status)
        for host in "$NAMENODE_HOST" "$DATANODE0_HOST" "$DATANODE1_HOST"; do
            echo "$host:"
            sudo -u hadoop ssh -n -i /home/hadoop/.ssh/id_ed25519_practice1 \
                -o BatchMode=yes -o StrictHostKeyChecking=yes \
                -o UserKnownHostsFile=/home/hadoop/.ssh/known_hosts_practice1 "$host" jps
        done
        sudo -u hadoop "$HADOOP_HOME/bin/hdfs" --config "$CONF_DIR" dfsadmin -report
        ;;
    *) echo 'Запуск: cluster.sh start|stop|status [cluster.env]' >&2; exit 2 ;;
esac
