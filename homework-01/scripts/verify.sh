#!/usr/bin/env bash
set -euo pipefail

task_dir=$(cd "$(dirname "$0")/.." && pwd)
config=$(realpath "${1:-$task_dir/cluster.env}")
source "$config"
[[ $# -le 2 && (${2:-} == '' || ${2:-} == --restart) ]] || {
    echo 'Запуск: verify.sh [cluster.env] [--restart]' >&2; exit 2;
}
[[ $(hostname -s) == "$EDGE_HOST" && $(id -un) == "$ADMIN_USER" ]] || {
    echo "Запустите скрипт от $ADMIN_USER на $EDGE_HOST." >&2; exit 1;
}
hdfs=(sudo -u hadoop "$HADOOP_HOME/bin/hdfs" --config "$CONF_DIR")
ssh_cmd=(sudo -u hadoop ssh -n -i /home/hadoop/.ssh/id_ed25519_practice1
    -o BatchMode=yes -o StrictHostKeyChecking=yes
    -o UserKnownHostsFile=/home/hadoop/.ssh/known_hosts_practice1)
stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT
path=/user/hadoop/homework-01/sample.bin

wait_health() {
    for ((attempt=0; attempt<30; attempt++)); do
        if python3 "$task_dir/scripts/check-health.py" "$NAMENODE_HOST" "$DATANODE1_HOST" \
            "$NAMENODE_IP" "$DATANODE0_IP" "$DATANODE1_IP" > "$stage/health" 2>&1; then
            cat "$stage/health"
            return
        fi
        sleep 5
    done
    cat "$stage/health" >&2
    return 1
}

check_file() {
    "${hdfs[@]}" dfs -cat "$path" > "$stage/result.bin"
    cmp "$stage/sample.bin" "$stage/result.bin"
    echo "Файл: $(wc -c < "$stage/result.bin") байт, SHA256 $(sha256sum "$stage/result.bin" | cut -d' ' -f1)"
    "${hdfs[@]}" fsck "$path" -files -blocks -locations > "$stage/fsck"
    python3 - "$stage/fsck" <<'PY'
import re
import sys
from pathlib import Path
report = Path(sys.argv[1]).read_text()
replicas = re.findall(r"^\d+\. .*Live_repl=(\d+)", report, re.MULTILINE)
assert "is HEALTHY" in report and replicas == ["3", "3", "3"], report
print("FSCK: HEALTHY, 3 блока, по 3 реплики")
PY
}

wait_health
python3 - "$stage/sample.bin" <<'PY'
import sys
from pathlib import Path
Path(sys.argv[1]).write_bytes(bytes(range(256)) * 10240)
PY
"${hdfs[@]}" dfs -mkdir -p "${path%/*}"
"${hdfs[@]}" dfs -Ddfs.blocksize=1048576 -put -f - "$path" < "$stage/sample.bin"
"${hdfs[@]}" dfs -setrep -w 3 "$path"
wait_health
check_file

if [[ ${2:-} == --restart ]]; then
    bash "$task_dir/scripts/cluster.sh" stop "$config"
    bash "$task_dir/scripts/cluster.sh" start "$config"
    wait_health
    check_file
    echo 'После stop/start файл сохранен без повторной записи.'
fi

hosts=("$NAMENODE_HOST" "$DATANODE0_HOST" "$DATANODE1_HOST")
expected=($'DataNode\nNameNode' 'DataNode' $'DataNode\nSecondaryNameNode')
for i in 0 1 2; do
    processes=$("${ssh_cmd[@]}" "${hosts[$i]}" "jps -l | awk '/org.apache.hadoop.hdfs.server/ {sub(/.*\\./, \"\", \$2); print \$2}' | sort")
    [[ $processes == "${expected[$i]}" ]] || { echo "Неверные процессы на ${hosts[$i]}: $processes" >&2; exit 1; }
    errors=$("${ssh_cmd[@]}" "${hosts[$i]}" "grep -hE ' ERROR | FATAL ' '$LOG_DIR'/*.log || [ \$? -eq 1 ]")
    errors=$(printf '%s' "$errors" | grep -vF 'RECEIVED SIGNAL 15: SIGTERM' || true)
    [[ -z $errors ]] || { echo "Ошибки на ${hosts[$i]}: $errors" >&2; exit 1; }
    echo "${hosts[$i]}: процессы и логи в порядке"
done
echo 'Проверка HDFS пройдена.'
