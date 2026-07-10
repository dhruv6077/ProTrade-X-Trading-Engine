#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

docker compose up -d prometheus grafana

echo "Prometheus: http://localhost:9091"
echo "Grafana:    http://localhost:3000  (admin/admin)"
