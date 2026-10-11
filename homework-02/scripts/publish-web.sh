#!/usr/bin/env bash
set -euo pipefail

task_dir=$(cd "$(dirname "$0")/.." && pwd)
source "${1:-$task_dir/cluster.env}"
[[ $# -le 1 && $(hostname -s) == "$EDGE_HOST" && $(id -un) == "$ADMIN_USER" ]] || {
    echo "Запуск на $EDGE_HOST от $ADMIN_USER: publish-web.sh [cluster.env]" >&2; exit 1;
}
for option in EDGE_HOST NAMENODE_HOST DATANODE0_HOST DATANODE1_HOST; do
    [[ ${!option} =~ ^[a-zA-Z0-9][a-zA-Z0-9.-]*$ ]] || { echo "Неверное имя узла: $option." >&2; exit 1; }
done
for option in EDGE_IP NAMENODE_IP DATANODE0_IP DATANODE1_IP; do
    [[ ${!option} =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "Неверный IP: $option." >&2; exit 1; }
done
sudo -n true
if ! command -v nginx >/dev/null; then
    sudo apt-get update -qq
    sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y -qq nginx
fi

ports=(19870 19868 19864 19865 19866 18088 18042 18043 18044 29888 18188 19099)
backend_ports=(9870 9868 9864 9864 9864 8088 8042 8042 8042 19888 8188 9099)
hosts=("$NAMENODE_HOST" "$DATANODE1_HOST" "$NAMENODE_HOST" "$DATANODE0_HOST" "$DATANODE1_HOST"
    "$EDGE_HOST" "$NAMENODE_HOST" "$DATANODE0_HOST" "$DATANODE1_HOST" "$EDGE_HOST" "$EDGE_HOST" "$EDGE_HOST")
ips=("$NAMENODE_IP" "$DATANODE1_IP" "$NAMENODE_IP" "$DATANODE0_IP" "$DATANODE1_IP"
    "$EDGE_IP" "$NAMENODE_IP" "$DATANODE0_IP" "$DATANODE1_IP" "$EDGE_IP" "$EDGE_IP" "$EDGE_IP")
labels=('NameNode' 'SecondaryNameNode' "DataNode $NAMENODE_HOST" "DataNode $DATANODE0_HOST" "DataNode $DATANODE1_HOST"
    'ResourceManager' "NodeManager $NAMENODE_HOST" "NodeManager $DATANODE0_HOST" "NodeManager $DATANODE1_HOST"
    'JobHistoryServer' 'Timeline Server' 'WebAppProxy (JMX)')
paths=(/dfshealth.html /status.html /datanode.html /datanode.html /datanode.html /cluster /node /node /node
    /jobhistory /applicationhistory /jmx)
stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT
nginx_conf=/etc/nginx/conf.d/hadoop-practice2.conf
links_conf=/etc/nginx/snippets/hadoop-practice2-links.conf
page=/var/www/hadoop-practice2/index.html

cat > "$stage/links.conf" <<'EOF'
proxy_set_header Accept-Encoding "";
proxy_set_header Host $proxy_host;
proxy_http_version 1.1;
proxy_connect_timeout 5s;
proxy_read_timeout 60s;
proxy_hide_header Cache-Control;
add_header Cache-Control "no-store" always;
sub_filter_once off;
sub_filter_types application/json application/javascript text/javascript;
EOF
for i in "${!ports[@]}"; do
    for address in "${hosts[$i]}" "${ips[$i]}"; do
        printf "sub_filter '%s:%s' '\$host:%s';\n" "$address" "${backend_ports[$i]}" "${ports[$i]}" >> "$stage/links.conf"
        printf 'proxy_redirect http://%s:%s/ http://$host:%s/;\n' "$address" "${backend_ports[$i]}" "${ports[$i]}" >> "$stage/links.conf"
    done
done
# NameNode собирает ссылку DataNode из разных полей JMX.
cat >> "$stage/links.conf" <<'EOF'
sub_filter 'n.dnWebAddress = "http://" + dnHost + ":" + port;' 'n.dnWebAddress = "http://" + n.infoAddr;';
EOF
cat > "$stage/nginx.conf" <<'EOF'
server {
    listen 0.0.0.0:18080;
    server_name _;
    charset utf-8;
    if ($request_method !~ ^(GET|HEAD)$) { return 405; }
    root /var/www/hadoop-practice2;
    location / { try_files $uri $uri/ =404; }
}
EOF
for i in "${!ports[@]}"; do
    cat >> "$stage/nginx.conf" <<EOF
server {
    listen 0.0.0.0:${ports[$i]};
    server_name _;
    if (\$request_method !~ ^(GET|HEAD)\$) { return 405; }
    if (\$arg_op ~* DELEGATIONTOKEN) { return 403; }
    location = /logLevel { return 403; }
    location ~ ^/prof(/|\$) { return 403; }
    location / {
        proxy_pass http://${ips[$i]}:${backend_ports[$i]};
        include $links_conf;
EOF
    if [[ ${backend_ports[$i]} == 9864 ]]; then
        cat >> "$stage/nginx.conf" <<'EOF'
        proxy_hide_header Access-Control-Allow-Origin;
        add_header Access-Control-Allow-Origin "http://$host:19870" always;
EOF
    fi
    printf '    }\n}\n' >> "$stage/nginx.conf"
done
cat > "$stage/index.html" <<'EOF'
<!doctype html>
<html lang="ru">
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Hadoop: работа 2</title>
<style>
body { font: 16px system-ui, sans-serif; max-width: 800px; margin: 40px auto; padding: 0 20px; color: #222; }
table { border-collapse: collapse; width: 100%; }
th, td { text-align: left; padding: 10px; border-bottom: 1px solid #ddd; }
a { color: #145ca4; }
p { line-height: 1.5; }
</style>
<h1>Hadoop: практическая работа 2</h1>
<p>Интерфейсы HDFS и YARN доступны для просмотра. История MapReduce и логи контейнеров находятся в JobHistoryServer.</p>
<table><thead><tr><th>Служба</th><th>Узел</th><th>Порт</th></tr></thead><tbody>
EOF
for i in "${!ports[@]}"; do
    printf '<tr><td><a data-port="%s" data-path="%s">%s</a></td><td>%s</td><td>%s</td></tr>\n' \
        "${ports[$i]}" "${paths[$i]}" "${labels[$i]}" "${hosts[$i]}" "${ports[$i]}" >> "$stage/index.html"
done
cat >> "$stage/index.html" <<'EOF'
</tbody></table>
<p>WebAppProxy открывает Tracking UI работающего приложения по ссылке из ResourceManager. Ссылка в таблице показывает его состояние через JMX.</p>
<script>
for (const link of document.querySelectorAll('a[data-port]')) {
    const url = new URL(window.location.href);
    url.hostname = window.location.hostname;
    url.port = link.dataset.port;
    url.pathname = link.dataset.path;
    url.search = '';
    url.hash = '';
    link.href = url.href;
}
</script>
</html>
EOF

targets=("$nginx_conf" "$links_conf" "$page")
for i in "${!targets[@]}"; do
    if sudo test -f "${targets[$i]}"; then
        sudo cp "${targets[$i]}" "$stage/backup-$i"
    fi
done
rollback() {
    for i in "${!targets[@]}"; do
        if [[ -f $stage/backup-$i ]]; then
            sudo cp "$stage/backup-$i" "${targets[$i]}"
        else
            sudo rm -f "${targets[$i]}"
        fi
    done
}
sudo install -d -m 755 /etc/nginx/snippets /var/www/hadoop-practice2
sudo install -m 644 "$stage/links.conf" "$links_conf"
sudo install -m 644 "$stage/nginx.conf" "$nginx_conf"
sudo install -m 644 "$stage/index.html" "$page"
if ! sudo nginx -t; then
    rollback
    exit 1
fi
if sudo systemctl is-active --quiet nginx; then
    action=reload
else
    action=start
fi
if ! sudo systemctl "$action" nginx; then
    rollback
    exit 1
fi
sudo systemctl enable nginx >/dev/null
echo 'Интерфейсы опубликованы. Откройте http://127.0.0.1:18080 через tunnel.sh на Mac.'
