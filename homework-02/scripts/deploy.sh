#!/usr/bin/env bash
set -euo pipefail

task_dir=$(cd "$(dirname "$0")/.." && pwd)
config=$(realpath "${1:-$task_dir/cluster.env}")
source "$config"
[[ $(hostname -s) == "$EDGE_HOST" && $(id -un) == "$ADMIN_USER" ]] || {
    echo "Запустите скрипт от $ADMIN_USER на $EDGE_HOST." >&2; exit 1;
}
for option in NODE_MEMORY_MB NODE_VCORES CONTAINER_MIN_MB CONTAINER_MAX_MB TASK_MEMORY_MB TASK_HEAP_MB DAEMON_HEAP_MB; do
    [[ ${!option} =~ ^[1-9][0-9]*$ ]] || { echo "$option должен быть положительным целым числом." >&2; exit 1; }
done
(( CONTAINER_MIN_MB <= TASK_MEMORY_MB && TASK_MEMORY_MB <= CONTAINER_MAX_MB &&
   CONTAINER_MAX_MB <= NODE_MEMORY_MB && TASK_HEAP_MB < TASK_MEMORY_MB )) || {
    echo 'Нужны min <= task <= max <= node и heap < task.' >&2; exit 1;
}
ssh_opts=(-i "$ADMIN_KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new)
hosts=("$EDGE_HOST" "$NAMENODE_HOST" "$DATANODE0_HOST" "$DATANODE1_HOST")
ips=("$EDGE_IP" "$NAMENODE_IP" "$DATANODE0_IP" "$DATANODE1_IP")
remote_stages=()
cleanup() {
    for entry in "${remote_stages[@]}"; do
        target=${entry%%:*}
        path=${entry#*:}
        ssh "${ssh_opts[@]}" "$target" "rm -rf '$path'" || true
    done
}
trap cleanup EXIT

# Конфиги меняем только при остановленном YARN.
pattern='[o]rg.apache.hadoop.yarn.server.(resourcemanager.ResourceManager|nodemanager.NodeManager|applicationhistoryservice.ApplicationHistoryServer|webproxy.WebAppProxyServer)|[o]rg.apache.hadoop.mapreduce.v2.hs.JobHistoryServer'
for i in 0 1 2 3; do
    if [[ $i -eq 0 ]]; then
        running=$(pgrep -af "$pattern" || true)
        sudo -n true
    else
        running=$(ssh "${ssh_opts[@]}" "$ADMIN_USER@${ips[$i]}" "sudo -n true && { pgrep -af '$pattern' || true; }")
    fi
    [[ -z $running ]] || { echo "Сначала остановите YARN на ${hosts[$i]}." >&2; exit 1; }
done
sudo -u hadoop "$HADOOP_HOME/bin/hdfs" --config "$HDFS_CONF_DIR" dfs -test -d /
for host in "$NAMENODE_HOST" "$DATANODE0_HOST" "$DATANODE1_HOST"; do
    sudo -u hadoop ssh -n -i "$HADOOP_KEY" -o BatchMode=yes -o StrictHostKeyChecking=yes \
        -o UserKnownHostsFile="$HADOOP_KNOWN_HOSTS" "$host" true
done
sudo bash "$task_dir/scripts/setup-node.sh" "$config"
for i in 1 2 3; do
    target="$ADMIN_USER@${ips[$i]}"
    remote_stage=$(ssh "${ssh_opts[@]}" "$target" 'mktemp -d /tmp/yarn-practice2.XXXXXX')
    remote_stages+=("$target:$remote_stage")
    scp "${ssh_opts[@]}" "$task_dir/scripts/setup-node.sh" "$target:$remote_stage/setup-node.sh"
    scp "${ssh_opts[@]}" "$config" "$target:$remote_stage/cluster.env"
    ssh "${ssh_opts[@]}" "$target" "sudo bash '$remote_stage/setup-node.sh' '$remote_stage/cluster.env'"
done

hdfs=(sudo -u hadoop "$HADOOP_HOME/bin/hdfs" --config "$CONF_DIR" dfs)
"${hdfs[@]}" -mkdir -p /yarn-logs /mr-history/tmp /mr-history/done /user/hadoop
"${hdfs[@]}" -chmod 1777 /yarn-logs /mr-history/tmp
"${hdfs[@]}" -chmod 750 /mr-history/done
bash "$task_dir/scripts/cluster.sh" start "$config"
python3 - "$EDGE_HOST" <<'PY'
import json
import sys
import time
import urllib.request

url = f"http://{sys.argv[1]}:8088/ws/v1/cluster/nodes"
for attempt in range(60):
    try:
        with urllib.request.urlopen(url, timeout=3) as response:
            nodes = (json.load(response).get("nodes") or {}).get("node", [])
        if len(nodes) == 3 and all(node["state"] == "RUNNING" for node in nodes):
            print("YARN запущен: 3 NodeManager в состоянии RUNNING.")
            break
    except (OSError, ValueError):
        pass
    time.sleep(1)
else:
    raise SystemExit("NodeManager не зарегистрировались. Проверьте status и логи.")
PY
