#!/usr/bin/env bash
set -euo pipefail

task_dir=$(cd "$(dirname "$0")/.." && pwd)
action=${1:-status}
source "${2:-$task_dir/cluster.env}"
[[ $(hostname -s) == "$EDGE_HOST" && $(id -un) == "$ADMIN_USER" ]] || {
    echo "Запустите скрипт от $ADMIN_USER на $EDGE_HOST." >&2; exit 1;
}
ssh_opts=(-n -i "$HADOOP_KEY" -o BatchMode=yes -o StrictHostKeyChecking=yes -o UserKnownHostsFile="$HADOOP_KNOWN_HOSTS")
workers=("$NAMENODE_HOST" "$DATANODE0_HOST" "$DATANODE1_HOST")

local_daemon() {
    sudo -u hadoop "$HADOOP_HOME/bin/$1" --config "$CONF_DIR" --daemon "$action" "$2"
}
node_daemons() {
    for host in "${workers[@]}"; do
        sudo -u hadoop ssh "${ssh_opts[@]}" "$host" \
            "$HADOOP_HOME/bin/yarn --config '$CONF_DIR' --daemon '$action' nodemanager"
    done
}
case "$action" in
    start)
        local_daemon yarn timelineserver
        local_daemon mapred historyserver
        local_daemon yarn resourcemanager
        local_daemon yarn proxyserver
        node_daemons
        ;;
    stop)
        node_daemons
        local_daemon yarn proxyserver
        local_daemon yarn resourcemanager
        local_daemon mapred historyserver
        local_daemon yarn timelineserver
        ;;
    status)
        echo "$EDGE_HOST:"
        sudo -u hadoop /usr/lib/jvm/java-11-openjdk-amd64/bin/jps
        for host in "${workers[@]}"; do
            echo "$host:"
            sudo -u hadoop ssh "${ssh_opts[@]}" "$host" /usr/lib/jvm/java-11-openjdk-amd64/bin/jps
        done
        sudo -u hadoop "$HADOOP_HOME/bin/yarn" --config "$CONF_DIR" node -list -all
        ;;
    *) echo 'Запуск: cluster.sh start|stop|status [cluster.env]' >&2; exit 2 ;;
esac
