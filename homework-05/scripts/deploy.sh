#!/usr/bin/env bash
set -euo pipefail
umask 077

task_dir=$(cd "$(dirname "$0")/.." && pwd)
source "$task_dir/scripts/prefect-env.sh"
python=${PYTHON:-/usr/bin/python3}
"$python" -c 'import sys; assert sys.version_info[:2] == (3, 12), "Нужен Python 3.12."'
[[ $PREFECT_VENV == /* && $PREFECT_VENV != / && $PREFECT_VENV != "$HOME" &&
   $PREFECT_HOME == /* && $PREFECT_HOME != / && $PREFECT_HOME != "$HOME" && $PREFECT_VENV != "$PREFECT_HOME" ]] || {
    echo 'Укажите отдельные абсолютные пути venv и PREFECT_HOME.' >&2; exit 1;
}
mkdir -p "$(dirname "$PREFECT_VENV")" "$PREFECT_HOME"
chmod 700 "$PREFECT_HOME"
if [[ ! -x $PREFECT_VENV/bin/python ]]; then
    "$python" -m venv --without-pip "$PREFECT_VENV"
fi
"$PREFECT_VENV/bin/python" -c 'import sys; assert sys.version_info[:2] == (3, 12) and sys.prefix != sys.base_prefix, "Нужен отдельный venv Python 3.12."'
stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT
wheel=$stage/pip-25.3-py3-none-any.whl
curl -fsSL --retry 3 https://files.pythonhosted.org/packages/44/3c/d717024885424591d5376220b5e836c2d5293ce2011523c9de23ff7bf068/pip-25.3-py3-none-any.whl -o "$wheel"
printf '%s  %s\n' 9655943313a94722b7774661c21049070f6bbb0a1516bf02f7c8d5d9201514cd "$wheel" | sha256sum -c -
PYTHONPATH="$wheel" "$PREFECT_VENV/bin/python" -m pip --disable-pip-version-check install --no-index "$wheel"
unset PIP_INDEX_URL PIP_EXTRA_INDEX_URL PIP_FIND_LINKS PIP_TRUSTED_HOST PIP_NO_INDEX
PIP_CONFIG_FILE=/dev/null "$PREFECT_VENV/bin/python" -m pip --disable-pip-version-check install \
    --index-url https://pypi.org/simple -r "$task_dir/requirements.txt"
"$PREFECT_VENV/bin/python" -m pip check
cat > "$PREFECT_PROFILES_PATH" <<'EOF'
active = "homework05"

[profiles.homework05]
PREFECT_SERVER_EPHEMERAL_ENABLED = "true"
PREFECT_SERVER_ANALYTICS_ENABLED = "false"
EOF
"$PREFECT_VENV/bin/python" -c 'import prefect; print("Prefect", prefect.__version__)'
echo "Окружение готово: $PREFECT_VENV. Настройки: scripts/prefect-env.sh."
