#!/usr/bin/env bash
set -euo pipefail

[[ $EUID -eq 0 && $# -eq 4 ]] || { echo 'Запуск: sudo bash setup-node.sh cluster.env архив run-service.sh hms|hs2' >&2; exit 2; }
source "$1"
archive=$2
runner=$3
role=$4
[[ $role == hms && $(hostname -s) == "$HMS_HOST" || $role == hs2 && $(hostname -s) == "$EDGE_HOST" ]] || {
    echo 'Роль не соответствует узлу.' >&2; exit 1;
}
version=4.0.0-alpha-2
checksum=f94c773e7ae3328d780138d9259c584e94d8f2e34715776cdfaaa78bcf3f25e1
printf '%s  %s\n' "$checksum" "$archive" | sha256sum -c -
[[ -x $JAVA_HOME/bin/java && -x $HADOOP_HOME/bin/hadoop && -f $HADOOP_CONF_DIR/yarn-site.xml ]]
id hadoop >/dev/null
if [[ ! -x /opt/apache-hive-$version-bin/bin/beeline ]]; then
    tar -xzf "$archive" -C /opt
    chown -R root:root "/opt/apache-hive-$version-bin"
fi
ln -sfn "/opt/apache-hive-$version-bin" "$HIVE_HOME"
install -d -m 755 "$CONF_DIR" "$CONF_DIR/server"
install -d -o root -g hadoop -m 750 "$CONF_DIR/metastore"
install -d -o hadoop -g hadoop -m 750 "$DATA_ROOT" "$DATA_ROOT/tmp" "$LOG_DIR"
install -o root -g root -m 755 "$runner" "$CONF_DIR/run-service.sh"
export JAVA_HOME HADOOP_HOME HADOOP_CONF_DIR HIVE_HOME CONF_DIR DATA_ROOT LOG_DIR
export HMS_HEAP_MB HS2_HEAP_MB CLIENT_HEAP_MB EDGE_IP HMS_HOST HMS_IP HMS_PORT HS2_PORT HS2_WEB_PORT
export PG_VERSION PG_CLUSTER PG_PORT PG_DATABASE PG_USER TASK_MEMORY_MB TASK_HEAP_MB
export WAREHOUSE_DIR EXTERNAL_DIR HIVE_SCRATCH_DIR
python3 - <<'PY'
import os, pathlib, shlex
keys='JAVA_HOME HADOOP_HOME HADOOP_CONF_DIR HIVE_HOME CONF_DIR DATA_ROOT LOG_DIR HMS_HEAP_MB HS2_HEAP_MB CLIENT_HEAP_MB'.split()
path=pathlib.Path(os.environ['CONF_DIR'])/'runtime.env'
path.write_text(''.join(f'{key}={shlex.quote(os.environ[key])}\n' for key in keys))
path.chmod(0o644)
PY

if [[ $role == hms ]]; then
    export DEBIAN_FRONTEND=noninteractive
    apt-get update -qq
    apt-get install -y -qq postgresql-common libpostgresql-jdbc-java
    pg_config=$(mktemp)
    cp /etc/postgresql-common/createcluster.conf "$pg_config"
    trap 'cp "$pg_config" /etc/postgresql-common/createcluster.conf; rm -f "$pg_config"' EXIT
    printf '\ncreate_main_cluster = false\n' >> /etc/postgresql-common/createcluster.conf
    apt-get install -y -qq "postgresql-$PG_VERSION"
    cp "$pg_config" /etc/postgresql-common/createcluster.conf
    rm -f "$pg_config"
    trap - EXIT
    if ! pg_lsclusters --no-header | awk '{print $1 ":" $2}' | grep -qxF "$PG_VERSION:$PG_CLUSTER"; then
        pg_createcluster "$PG_VERSION" "$PG_CLUSTER" -p "$PG_PORT" --start-conf=auto -- --auth-local=peer --auth-host=scram-sha-256
    fi
    install -d -m 755 "/etc/postgresql/$PG_VERSION/$PG_CLUSTER/conf.d"
    cat > "/etc/postgresql/$PG_VERSION/$PG_CLUSTER/conf.d/practice3.conf" <<EOF
listen_addresses = '127.0.0.1'
port = $PG_PORT
max_connections = 30
shared_buffers = '64MB'
work_mem = '4MB'
maintenance_work_mem = '32MB'
EOF
    systemctl restart "postgresql@$PG_VERSION-$PG_CLUSTER"
    ln -sfn /usr/share/java/postgresql.jar "$HIVE_HOME/lib/postgresql-jdbc.jar"
    python3 - <<'PY'
import os,pathlib,secrets,subprocess
secret=pathlib.Path(os.environ['CONF_DIR'])/'metastore/password'
if not secret.exists():
    secret.touch(mode=0o600)
    secret.write_text(secrets.token_hex(24))
secret.chmod(0o600)
password=secret.read_text().strip()
user=os.environ['PG_USER']; database=os.environ['PG_DATABASE']
psql=['sudo','-u','postgres','psql','-X','-v','ON_ERROR_STOP=1','-p',os.environ['PG_PORT']]
def exists(query):
    return subprocess.check_output(psql+['-At','-c',query],text=True).strip()=='1'
if not exists(f"SELECT 1 FROM pg_roles WHERE rolname='{user}'"):
    subprocess.run(psql,input=f'CREATE ROLE "{user}" LOGIN;\n',text=True,check=True,stdout=subprocess.DEVNULL)
subprocess.run(psql,input=f'ALTER ROLE "{user}" PASSWORD \'{password}\';\n',text=True,check=True,stdout=subprocess.DEVNULL)
if not exists(f"SELECT 1 FROM pg_database WHERE datname='{database}'"):
    subprocess.run(psql,input=f'CREATE DATABASE "{database}" OWNER "{user}";\n',text=True,check=True,stdout=subprocess.DEVNULL)
PY
fi

python3 - "$role" <<'PY'
import os,pathlib,sys,xml.etree.ElementTree as E
v=os.environ; role=sys.argv[1]
props={
 'hive.execution.engine':'mr', 'hive.exec.mode.local.auto':'false', 'hive.fetch.task.conversion':'more',
 'hive.metastore.warehouse.dir':v['WAREHOUSE_DIR'], 'hive.metastore.warehouse.external.dir':v['EXTERNAL_DIR'],
 'hive.exec.scratchdir':v['HIVE_SCRATCH_DIR'], 'hive.exec.local.scratchdir':v['DATA_ROOT']+'/tmp',
 'hive.metastore.schema.verification':'true', 'datanucleus.schema.autoCreateAll':'false',
 'mapreduce.map.memory.mb':v['TASK_MEMORY_MB'], 'mapreduce.reduce.memory.mb':v['TASK_MEMORY_MB'],
 'yarn.app.mapreduce.am.resource.mb':v['TASK_MEMORY_MB'],
 'mapreduce.map.java.opts':'-Xmx'+v['TASK_HEAP_MB']+'m',
 'mapreduce.reduce.java.opts':'-Xmx'+v['TASK_HEAP_MB']+'m',
 'yarn.app.mapreduce.am.command-opts':'-Xmx'+v['TASK_HEAP_MB']+'m',
 'mapreduce.task.io.sort.mb':'64', 'hive.auto.convert.join':'false'}
props['hive.aux.jars.path']=(pathlib.Path(v['HIVE_HOME'])/'lib/commons-collections-3.2.2.jar').as_uri()
if role=='hms':
    # Hadoop 3.4 читает proxy hosts из core-site раньше hive-site.
    core=E.parse(pathlib.Path(v['HADOOP_CONF_DIR'])/'core-site.xml')
    prop=E.SubElement(core.getroot(),'property')
    E.SubElement(prop,'name').text='hadoop.proxyuser.hadoop.hosts'
    E.SubElement(prop,'value').text=v['EDGE_IP']
    core_path=pathlib.Path(v['CONF_DIR'])/'metastore/core-site.xml'
    core.write(core_path,encoding='UTF-8',xml_declaration=True)
    core_path.chmod(0o644)
    props.update({'metastore.thrift.bind.host':v['HMS_IP'], 'metastore.thrift.port':v['HMS_PORT'],
      'javax.jdo.option.ConnectionURL':f"jdbc:postgresql://127.0.0.1:{v['PG_PORT']}/{v['PG_DATABASE']}",
      'javax.jdo.option.ConnectionDriverName':'org.postgresql.Driver',
      'javax.jdo.option.ConnectionUserName':v['PG_USER'],
      'javax.jdo.option.ConnectionPassword':(pathlib.Path(v['CONF_DIR'])/'metastore/password').read_text().strip(),
      'datanucleus.connectionPool.maxPoolSize':'5', 'metastore.try.direct.sql.ddl':'false',
      'hive.metastore.event.db.notification.api.auth':'true'})
    directory='metastore'
else:
    props.update({'hive.metastore.uris':f"thrift://{v['HMS_HOST']}:{v['HMS_PORT']}",
      'hive.server2.thrift.bind.host':'127.0.0.1', 'hive.server2.thrift.port':v['HS2_PORT'],
      'hive.server2.webui.host':'127.0.0.1', 'hive.server2.webui.port':v['HS2_WEB_PORT'],
      'hive.server2.enable.doAs':'false', 'hive.server2.authentication':'NONE',
      'hive.server2.metrics.enabled':'true', 'hive.server2.thrift.min.worker.threads':'5',
      'hive.server2.thrift.max.worker.threads':'20', 'hive.server2.tez.initialize.default.sessions':'false',
      'hive.server2.logging.operation.log.location':v['LOG_DIR']+'/operations'})
    directory='server'
root=E.Element('configuration')
for name,value in props.items():
    prop=E.SubElement(root,'property'); E.SubElement(prop,'name').text=name; E.SubElement(prop,'value').text=value
E.indent(root,space='  ')
path=pathlib.Path(v['CONF_DIR'])/directory/'hive-site.xml'
E.ElementTree(root).write(path,encoding='UTF-8',xml_declaration=True)
path.chmod(0o640 if role=='hms' else 0o644)
os.chown(path,0,__import__('grp').getgrnam('hadoop').gr_gid)
name='metastore' if role=='hms' else 'server'
after=f"network-online.target postgresql@{v['PG_VERSION']}-{v['PG_CLUSTER']}.service" if role=='hms' else 'network-online.target'
requires=f"Requires=postgresql@{v['PG_VERSION']}-{v['PG_CLUSTER']}.service\n" if role=='hms' else ''
unit=f'''[Unit]
Description=Hive practice 3 {name}
After={after}
{requires}
[Service]
Type=simple
User=hadoop
Group=hadoop
WorkingDirectory={v['DATA_ROOT']}
ExecStart={v['CONF_DIR']}/run-service.sh {v['CONF_DIR']}/runtime.env {role}
Restart=on-failure
RestartSec=5
TimeoutStopSec=60
KillSignal=SIGTERM
UMask=0027

[Install]
WantedBy=multi-user.target
'''
pathlib.Path(f'/etc/systemd/system/hive-practice3-{name}.service').write_text(unit)
PY
systemctl daemon-reload
systemctl enable "hive-practice3-$([[ $role == hms ]] && echo metastore || echo server)"
if [[ $role == hms ]]; then
    version_table=$(sudo -u postgres psql -X -At -p "$PG_PORT" -d "$PG_DATABASE" -c "SELECT to_regclass('\"VERSION\"') IS NOT NULL;")
    if [[ $version_table == f ]]; then
        sudo -u hadoop "$CONF_DIR/run-service.sh" "$CONF_DIR/runtime.env" schema -dbType postgres -initSchema
    fi
    sudo -u hadoop "$CONF_DIR/run-service.sh" "$CONF_DIR/runtime.env" schema -dbType postgres -info
    sudo -u hadoop "$CONF_DIR/run-service.sh" "$CONF_DIR/runtime.env" schema -dbType postgres -validate
fi
