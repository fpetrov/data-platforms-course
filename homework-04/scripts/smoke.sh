#!/usr/bin/env bash
set -euo pipefail

task_dir=$(cd "$(dirname "$0")/.." && pwd)
export CLUSTER_ENV
CLUSTER_ENV=$(realpath "${1:-${CLUSTER_ENV:-$task_dir/cluster.env}}")
source "$CLUSTER_ENV"
[[ $(hostname -s) == "$EDGE_HOST" && $(id -un) == "$ADMIN_USER" ]] || {
    echo "Запустите скрипт от $ADMIN_USER на $EDGE_HOST." >&2; exit 1;
}
runid=$(python3 -c 'import uuid; print(uuid.uuid4().hex[:12])')
database=spark04_smoke_$runid
hdfs=(sudo -u hadoop "$HADOOP_HOME/bin/hdfs" --config "$HADOOP_CONF_DIR" dfs)
fs=$(sudo -u hadoop "$HADOOP_HOME/bin/hdfs" --config "$HADOOP_CONF_DIR" getconf -confKey fs.defaultFS)
location=${fs%/}$EXTERNAL_DIR/$database
client=(env CLUSTER_ENV="$HIVE_CLUSTER_ENV" bash "$HIVE_TASK_DIR/scripts/client.sh")
stage=$(mktemp -d)
cleanup() {
    status=$?
    failed=0
    "${client[@]}" -e "DROP DATABASE IF EXISTS \`$database\` CASCADE;" </dev/null >/dev/null 2>&1 || failed=1
    "${hdfs[@]}" -rm -r -f "$location" >/dev/null 2>&1 || failed=1
    rm -rf "$stage"
    if (( failed )); then
        echo "Не удалось удалить все smoke-артефакты: $database, $location." >&2
        (( status != 0 )) || status=1
    fi
    exit "$status"
}
trap cleanup EXIT
SPARK_YARN_TAGS=$database bash "$task_dir/scripts/submit.sh" "$task_dir/scripts/smoke.py" "$database" "$location"
"${client[@]}" -e "SET hive.fetch.task.conversion=more; SELECT value, day FROM \`$database\`.records; SHOW PARTITIONS \`$database\`.records;" > "$stage/result"
cat "$stage/result"
python3 - "$stage/result" <<'PY'
import pathlib,sys
lines=pathlib.Path(sys.argv[1]).read_text().splitlines()
expected=[f'{value}\t2026-10-23' for value in range(1,4)]+['day=2026-10-23']
if sorted(lines)!=sorted(expected): raise SystemExit('Beeline прочитал неожиданные данные.')
print('Spark saveAsTable → отдельный Hive Metastore → Beeline: 3 строки, 1 партиция.')
PY
