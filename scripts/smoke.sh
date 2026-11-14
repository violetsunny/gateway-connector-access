#!/usr/bin/env bash
# 冒烟回归：无外部服务。验证 udp 上行、http 上行、kafka stdout 降级、SIGINT 优雅停机。
# 用法：scripts/smoke.sh（自动 cd 到 workspace 根；退出码 0=全过）
set -u
cd "$(dirname "$0")/.."

LOG="$(mktemp /tmp/gw-smoke.XXXXXX)"
GW_PID=""
cleanup() { if [ -n "$GW_PID" ]; then kill -INT "$GW_PID" 2>/dev/null; fi; }
trap cleanup EXIT

FAIL=0
report() { if [ "$1" -eq 0 ]; then echo "PASS: $2"; else echo "FAIL: $2"; FAIL=1; fi; }

cargo build -p gateway -q || { echo "FAIL: cargo build -p gateway"; exit 1; }
cargo run -p gateway -q -- config.example.toml >"$LOG" 2>&1 &
GW_PID=$!

ready=1
for _ in $(seq 1 60); do
  curl -s --max-time 1 -o /dev/null http://127.0.0.1:8080/ && { ready=0; break; }
  sleep 1
done
report $ready "gateway 启动（http 8080 就绪，60s 内）"

printf '\x01\x00\x2a' | nc -u -w1 127.0.0.1 9100 >/dev/null
sleep 2
grep -q '"value":42' "$LOG"; report $? "udp 9100 hex 帧上行 value=42"

resp=$(curl -s -X POST http://127.0.0.1:8080/api/productA -H 'content-type: application/json' \
  -d '{"messageType":"DEVICE_REPORT_REQ","deviceId":"dev-001","sn":"SN001","metrics":[{"ts":0,"code":"t","value":1}]}')
echo "$resp" | grep -q 'SUCCESS'; report $? "http 8080 响应 SUCCESS"
sleep 2
grep -q '"value":1' "$LOG"; report $? "http 上行 value=1 进 sink 日志"

grep -q 'falling back to stdout' "$LOG"; report $? "kafka 空配置降级 stdout"

kill -INT "$GW_PID"
wait "$GW_PID"; rc=$?
GW_PID=""
[ $rc -eq 0 ]; report $? "SIGINT 优雅停机 exit=0"
grep -q 'gateway stopped' "$LOG"; report $? "停机日志 gateway stopped"

echo "---"
if [ $FAIL -eq 0 ]; then echo "SMOKE PASS（日志: ${LOG}）"; else echo "SMOKE FAIL（日志: ${LOG}）"; fi
exit $FAIL
