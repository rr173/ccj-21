#!/usr/bin/env bash
# 控制端演示：注册设备 -> 离线时下发期望 -> 观察影子读模型。
# 设备侧请用另一个终端运行模拟器（见脚本结尾打印的命令）。
set -euo pipefail
CORE="${CORE_URL:-http://localhost:8080}"
DEV="${1:-demo-lamp}"

j() { python3 -m json.tool; }

echo "== 1) 注册设备（返回设备令牌） =="
REG=$(curl -sS -X POST "$CORE/v1/devices" -H 'Content-Type: application/json' \
  -d "{\"id\":\"$DEV\",\"name\":\"演示灯具\"}")
echo "$REG" | j
TOKEN=$(echo "$REG" | python3 -c 'import sys,json;print(json.load(sys.stdin)["token"])')
echo "TOKEN=$TOKEN"

echo
echo "== 2) 设备离线时控制端修改期望态（命令按版本入队） =="
curl -sS -X PUT "$CORE/v1/devices/$DEV/desired" -H 'Content-Type: application/json' \
  -d '{"state":{"led":"on","brightness":80}}' | j
curl -sS -X PUT "$CORE/v1/devices/$DEV/desired" -H 'Content-Type: application/json' \
  -d '{"state":{"led":"on","brightness":100}}' | j
echo "（旧的未送达版本会被标记 SUPERSEDED，只发最新版）"

echo
echo "== 3) 查询影子：下一条待发命令 / 差异原因 =="
curl -sS "$CORE/v1/devices/$DEV" | j

echo
echo "== 4) 启动设备模拟器后，命令会按版本补发，正常场景："
echo "    python3 run_sim.py --ingress-url http://localhost:8081 --token $TOKEN --scenario normal"
echo
echo "== 5) 其他场景："
echo "    重复确认: --scenario dupack"
echo "    报告乱序: --scenario outoforder"
echo "    派发掉线: --scenario drop"
echo "    不确认  : --scenario noack"
echo
echo "== 6) 追溯查询："
echo "    curl -s $CORE/v1/devices/$DEV/commands | python3 -m json.tool"
echo "    curl -s '$CORE/v1/events?device_id=$DEV' | python3 -m json.tool"
