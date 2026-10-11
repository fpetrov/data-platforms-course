#!/usr/bin/env bash
set -euo pipefail

task_dir=$(cd "$(dirname "$0")/.." && pwd)
[[ $# -le 2 && (${2:-} == '' || ${2:-} == --restart) ]] || {
    echo 'Запуск: verify.sh [cluster.env] [--restart]' >&2; exit 2;
}
config=$(realpath "${1:-$task_dir/cluster.env}")
set -a
source "$config"
set +a
[[ $(hostname -s) == "$EDGE_HOST" && $(id -un) == "$ADMIN_USER" ]] || {
    echo "Запустите скрипт от $ADMIN_USER на $EDGE_HOST." >&2; exit 1;
}
hdfs=(sudo -u hadoop "$HADOOP_HOME/bin/hdfs" --config "$CONF_DIR" dfs)
yarn=(sudo -u hadoop "$HADOOP_HOME/bin/yarn" --config "$CONF_DIR")
stage=$(mktemp -d /tmp/yarn-verify.XXXXXX)
run_path="/user/hadoop/homework-02/$(date -u +%Y%m%dT%H%M%SZ)-${stage##*.}"
job_pid=''
cleanup() {
    if [[ -n $job_pid ]] && kill -0 "$job_pid" 2>/dev/null; then
        kill "$job_pid" 2>/dev/null || true
    fi
    rm -rf "$stage"
}
trap cleanup EXIT

helper() { python3 "$task_dir/scripts/check-health.py" "$@"; }
wait_check() {
    for ((attempt=0; attempt<36; attempt++)); do
        if helper "$@" > "$stage/check" 2>&1; then
            cat "$stage/check"
            return
        fi
        sleep 5
    done
    cat "$stage/check" >&2
    return 1
}
hdfs_health() {
    python3 "$task_dir/../homework-01/scripts/check-health.py" "$NAMENODE_HOST" "$DATANODE1_HOST" \
        "$NAMENODE_IP" "$DATANODE0_IP" "$DATANODE1_IP"
}
check_output() {
    "${hdfs[@]}" -test -e "$run_path/output/_SUCCESS"
    "${hdfs[@]}" -cat "$run_path/output/part-r-*" > "$stage/result"
    cmp "$stage/expected" "$stage/result"
    echo "WordCount: содержимое совпало, SHA256 $(sha256sum "$stage/result" | cut -d' ' -f1)"
    cat "$stage/result"
}
read_logs() {
    "${yarn[@]}" logs -applicationId "$app_id" -appOwner hadoop -log_files stdout,syslog \
        > "$stage/container-logs" 2> "$stage/log-client"
    helper logs "$stage/container-logs"
}

echo "Начало проверки: $(date -u +%FT%TZ)"
echo "Каталог HDFS: $run_path"
wait_check health "$stage/hdfs-processes.json"
hdfs_health
helper daemon-logs
helper web
baseline=/user/hadoop/homework-01/sample.bin
if "${hdfs[@]}" -test -e "$baseline"; then
    "${hdfs[@]}" -cat "$baseline" | sha256sum > "$stage/hdfs-before"
    echo "Файл работы 1: $(cut -d' ' -f1 "$stage/hdfs-before")"
fi
python3 - "$stage" <<'PY'
import sys
from pathlib import Path
stage = Path(sys.argv[1])
stage.joinpath('first.txt').write_text('hadoop yarn hadoop\n' * 500000)
stage.joinpath('second.txt').write_text('yarn mapreduce\n' * 500000)
stage.joinpath('expected').write_text('hadoop\t1000000\nmapreduce\t500000\nyarn\t1000000\n')
PY
"${hdfs[@]}" -mkdir -p "$run_path/input"
"${hdfs[@]}" -put - "$run_path/input/first.txt" < "$stage/first.txt"
"${hdfs[@]}" -put - "$run_path/input/second.txt" < "$stage/second.txt"
examples=("$HADOOP_HOME"/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar)
[[ ${#examples[@]} -eq 1 && -f ${examples[0]} ]] || { echo 'Не найден один examples JAR.' >&2; exit 1; }
timeout 300 sudo -u hadoop "$HADOOP_HOME/bin/hadoop" --config "$CONF_DIR" jar "${examples[0]}" \
    wordcount -Dmapreduce.framework.name=yarn -Dmapreduce.job.ubertask.enable=false \
    -Dmapreduce.job.reduces=1 "$run_path/input" "$run_path/output" > "$stage/job" 2>&1 &
job_pid=$!
app_id=''
tracking_shown=false
while kill -0 "$job_pid" 2>/dev/null; do
    if [[ -z $app_id ]]; then
        app_id=$(sed -n 's/.*Submitted application \(application_[0-9_]*\).*/\1/p' "$stage/job" | head -1)
    fi
    if [[ -n $app_id && $tracking_shown == false ]]; then
        state=$(python3 - "$EDGE_HOST" "$app_id" <<'PY'
import json
import sys
from urllib.request import urlopen
try:
    with urlopen(f'http://{sys.argv[1]}:8088/ws/v1/cluster/apps/{sys.argv[2]}', timeout=3) as response:
        print(json.load(response)['app']['state'])
except OSError:
    pass
PY
)
        if [[ $state == RUNNING ]]; then
            echo "RUNNING: $app_id"
            echo "Tracking UI: http://127.0.0.1:19099/proxy/$app_id/"
            tracking_shown=true
        fi
    fi
    sleep 1
done
if ! wait "$job_pid"; then
    cat "$stage/job" >&2
    exit 1
fi
job_pid=''
cat "$stage/job"
if [[ -z $app_id ]]; then
    app_id=$(sed -n 's/.*Submitted application \(application_[0-9_]*\).*/\1/p' "$stage/job" | head -1)
fi
[[ -n $app_id ]] || { echo 'Нет YARN application id.' >&2; exit 1; }
job_id=${app_id/application_/job_}
check_output
wait_check job "$app_id" "$job_id"
read_logs
logs_hash=$(sha256sum "$stage/container-logs" | cut -d' ' -f1)
echo "Логи SHA256: $logs_hash"

if [[ ${2:-} == --restart ]]; then
    bash "$task_dir/scripts/cluster.sh" stop "$config"
    bash "$task_dir/scripts/cluster.sh" start "$config"
    wait_check health "$stage/hdfs-processes.json"
    check_output
    wait_check job "$app_id" "$job_id" --restarted
    read_logs
    [[ $(sha256sum "$stage/container-logs" | cut -d' ' -f1) == "$logs_hash" ]] || {
        echo 'Содержимое агрегированных логов изменилось.' >&2; exit 1;
    }
    if [[ -f $stage/hdfs-before ]]; then
        "${hdfs[@]}" -cat "$baseline" | sha256sum > "$stage/hdfs-after"
        cmp "$stage/hdfs-before" "$stage/hdfs-after"
        echo 'Файл работы 1 сохранился без изменений.'
    fi
    echo 'После stop/start сохранены результат, JHS, ATS и агрегированные логи. Новое задание не запускалось.'
fi
wait_check health "$stage/hdfs-processes.json"
hdfs_health
helper daemon-logs
helper web
echo 'Проверка YARN пройдена.'
