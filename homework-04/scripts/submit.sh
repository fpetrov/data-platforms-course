#!/usr/bin/env bash
set -euo pipefail

task_dir=$(cd "$(dirname "$0")/.." && pwd)
source "${CLUSTER_ENV:-$task_dir/cluster.env}"
[[ $# -ge 1 ]] || { echo 'Запуск: submit.sh приложение.py [аргументы]' >&2; exit 2; }
[[ $(hostname -s) == "$EDGE_HOST" && $(id -un) == "$ADMIN_USER" ]] || {
    echo "Запустите скрипт от $ADMIN_USER на $EDGE_HOST." >&2; exit 1;
}
[[ -f $CONF_DIR/spark-defaults.conf && -f $CONF_DIR/hive-site.xml ]] || {
    echo 'Сначала выполните deploy.sh.' >&2; exit 1;
}
python3 - "$CONF_DIR/hive-site.xml" "$HMS_HOST" "$HMS_PORT" <<'PY'
import sys,xml.etree.ElementTree as E
props={p.findtext('name'):p.findtext('value') for p in E.parse(sys.argv[1]).getroot().findall('property')}
if props.get('hive.metastore.uris')!=f'thrift://{sys.argv[2]}:{sys.argv[3]}':
    raise SystemExit('Ожидается настроенный отдельный Metastore. Повторите deploy.sh.')
PY
application=$1
shift
[[ -f $application ]] || { echo 'Файл приложения не найден.' >&2; exit 1; }
copy=$(sudo -u hadoop mktemp "$DATA_ROOT/tmp/spark-app.XXXXXX.py")
trap 'sudo -u hadoop rm -f "$copy"' EXIT
sudo -u hadoop tee "$copy" < "$application" >/dev/null
sudo -u hadoop bash -c 'cd "$1"; shift; exec "$@"' bash "$DATA_ROOT" \
    env JAVA_HOME="$JAVA_HOME" HADOOP_CONF_DIR="$HADOOP_CONF_DIR" \
    SPARK_HOME="$SPARK_HOME" SPARK_CONF_DIR="$CONF_DIR" SPARK_LOCAL_IP="$EDGE_IP" \
    SPARK_LOCAL_DIRS="$DATA_ROOT/tmp" SPARK_LOG_DIR="$LOG_DIR" \
    "$SPARK_HOME/bin/spark-submit" --master yarn --deploy-mode client \
    --conf "spark.yarn.tags=${SPARK_YARN_TAGS:-homework04}" "$copy" "$@"
