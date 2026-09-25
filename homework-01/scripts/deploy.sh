#!/usr/bin/env bash
set -euo pipefail

task_dir=$(cd "$(dirname "$0")/.." && pwd)
config=$(realpath "${1:-$task_dir/cluster.env}")
source "$config"
[[ $(hostname -s) == "$EDGE_HOST" ]] || { echo 'Запустите скрипт на edge.' >&2; exit 1; }
[[ $(id -un) == "$ADMIN_USER" ]] || { echo "Запустите скрипт от $ADMIN_USER." >&2; exit 1; }
ssh_opts=(-i "$ADMIN_KEY" -o BatchMode=yes -o StrictHostKeyChecking=accept-new)
hosts=("$EDGE_HOST" "$NAMENODE_HOST" "$DATANODE0_HOST" "$DATANODE1_HOST")
ips=("$EDGE_IP" "$NAMENODE_IP" "$DATANODE0_IP" "$DATANODE1_IP")
stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT

# Проверяем процессы до изменения конфигурации.
for i in 0 1 2 3; do
    command='pgrep -af "[o]rg.apache.hadoop.hdfs.server.(namenode|datanode)" || true'
    if [[ $i -eq 0 ]]; then
        running=$(bash -c "$command")
    else
        running=$(ssh "${ssh_opts[@]}" "$ADMIN_USER@${ips[$i]}" "$command")
    fi
    [[ -z $running ]] || { echo "Сначала остановите HDFS на ${hosts[$i]}." >&2; exit 1; }
done

mkdir -p "$(dirname "$ARCHIVE")"
if [[ ! -f $ARCHIVE ]]; then
    if ! command -v curl >/dev/null; then
        sudo apt-get update -qq
        sudo apt-get install -y -qq curl
    fi
    curl -fL --retry 3 https://archive.apache.org/dist/hadoop/common/hadoop-3.4.3/hadoop-3.4.3.tar.gz -o "$ARCHIVE.part"
    mv "$ARCHIVE.part" "$ARCHIVE"
fi
sudo bash "$task_dir/scripts/setup-node.sh" "$config" "$ARCHIVE"
if ! sudo test -f /home/hadoop/.ssh/id_ed25519_practice1; then
    sudo -u hadoop ssh-keygen -q -t ed25519 -N '' -C hdfs-practice1 -f /home/hadoop/.ssh/id_ed25519_practice1
fi
sudo cat /home/hadoop/.ssh/id_ed25519_practice1.pub > "$stage/hadoop.pub"

for i in 1 2 3; do
    target="$ADMIN_USER@${ips[$i]}"
    remote_stage=$(ssh "${ssh_opts[@]}" "$target" 'mktemp -d /tmp/hdfs-practice1.XXXXXX')
    scp "${ssh_opts[@]}" "$task_dir/scripts/setup-node.sh" "$stage/hadoop.pub" "$config" "$target:$remote_stage/"
    scp "${ssh_opts[@]}" "$ARCHIVE" "$target:$remote_stage/hadoop.tar.gz"
    ssh "${ssh_opts[@]}" "$target" "sudo bash '$remote_stage/setup-node.sh' '$remote_stage/$(basename "$config")' '$remote_stage/hadoop.tar.gz'"
    ssh "${ssh_opts[@]}" "$target" "sudo bash -s -- '$remote_stage/hadoop.pub'" <<'REMOTE'
set -euo pipefail
key=$(cat "$1")
auth=/home/hadoop/.ssh/authorized_keys
touch "$auth"
grep -qxF "$key" "$auth" || printf '%s\n' "$key" >> "$auth"
chown hadoop:hadoop "$auth"
chmod 600 "$auth"
REMOTE
    host_key=$(ssh "${ssh_opts[@]}" "$target" 'cat /etc/ssh/ssh_host_ed25519_key.pub')
    printf '%s,%s %s\n' "${hosts[$i]}" "${ips[$i]}" "$host_key" >> "$stage/known_hosts"
    ssh "${ssh_opts[@]}" "$target" "rm -rf '$remote_stage'"
done
sudo install -o hadoop -g hadoop -m 600 "$stage/known_hosts" /home/hadoop/.ssh/known_hosts_practice1
for host in "$NAMENODE_HOST" "$DATANODE0_HOST" "$DATANODE1_HOST"; do
    sudo -u hadoop ssh -i /home/hadoop/.ssh/id_ed25519_practice1 -o BatchMode=yes -o StrictHostKeyChecking=yes -o UserKnownHostsFile=/home/hadoop/.ssh/known_hosts_practice1 "$host" true
done
sudo -u hadoop "$HADOOP_HOME/sbin/start-dfs.sh" --config "$CONF_DIR"
echo 'HDFS запущен. Проверьте кластер скриптом verify.sh.'
