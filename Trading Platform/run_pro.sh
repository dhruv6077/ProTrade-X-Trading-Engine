#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

export JETTY_PORT="${JETTY_PORT:-8080}"
export JAVA_TOOL_OPTIONS="${JAVA_TOOL_OPTIONS:-} -Xshare:off -XX:+EnableDynamicAgentLoading"

echo "Starting Vision Trader"
echo "HTTP dashboard/API: http://localhost:${JETTY_PORT}"
echo "WebSocket orders:  ws://localhost:9090/ws/orders"
echo "WebSocket market:  ws://localhost:9090/ws/market-data"

mvn -q -DskipTests compile exec:java -Dexec.mainClass=Main
