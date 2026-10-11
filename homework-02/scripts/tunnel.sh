#!/usr/bin/env bash
set -euo pipefail

[[ $# -le 2 ]] || { echo 'Запуск: tunnel.sh [user@edge] [ключ]' >&2; exit 2; }
target=${1:-${SSH_TARGET:-team@2.59.83.239}}
key=${2:-${SSH_KEY:-$HOME/.ssh/id_ed25519_team30}}
[[ -f $key ]] || { echo "Нет SSH-ключа: $key" >&2; exit 1; }
ports=(18080 19870 19868 19864 19865 19866 18088 18042 18043 18044 29888 18188 19099)
forwards=()
for port in "${ports[@]}"; do
    forwards+=(-L "127.0.0.1:$port:127.0.0.1:$port")
done
echo 'Откройте http://127.0.0.1:18080. Оставьте этот терминал работать; Ctrl+C закроет туннель.'
exec ssh -i "$key" -N -o ExitOnForwardFailure=yes -o ServerAliveInterval=30 -o ServerAliveCountMax=3 \
    "${forwards[@]}" "$target"
