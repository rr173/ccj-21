#!/usr/bin/env bash
# 本地一键启动三个独立进程（无需 Docker），用于开发联调。
# 数据落在 ./data/shadow.db；Ctrl-C 会清理子进程。
set -euo pipefail
cd "$(dirname "$0")/.."

export SHADOW_DB_PATH="$PWD/data/shadow.db"
mkdir -p data

# 便于演示的激进参数，可按需通过环境变量覆盖
export CORE_HOST=127.0.0.1 CORE_PORT="${CORE_PORT:-8080}"
export INGRESS_HOST=127.0.0.1 INGRESS_PORT="${INGRESS_PORT:-8081}"
export CORE_URL="http://127.0.0.1:${CORE_PORT}"
export SHADOW_INTERNAL_TOKEN="${SHADOW_INTERNAL_TOKEN:-dev-internal-token}"
export OFFLINE_AFTER_SECONDS="${OFFLINE_AFTER_SECONDS:-30}"
export PROLONGED_OFFLINE_SECONDS="${PROLONGED_OFFLINE_SECONDS:-300}"
export ACK_TIMEOUT_SECONDS="${ACK_TIMEOUT_SECONDS:-30}"
export COMMAND_TTL_SECONDS="${COMMAND_TTL_SECONDS:-900}"

pids=()
cleanup() {
  echo
  echo "[dev] stopping services..."
  kill "${pids[@]}" 2>/dev/null || true
  wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "[dev] starting core on :$CORE_PORT"
python3 run_core.py & pids+=($!)
sleep 0.7
echo "[dev] starting ingress on :$INGRESS_PORT"
python3 run_ingress.py & pids+=($!)
echo "[dev] starting dispatcher"
python3 run_dispatcher.py & pids+=($!)

echo
echo "[dev] all services up"
echo "  control plane : http://127.0.0.1:$CORE_PORT  (GET /v1/devices)"
echo "  device gateway: http://127.0.0.1:$INGRESS_PORT (/devices/commands/poll)"
echo "  db file       : $SHADOW_DB_PATH"
wait
