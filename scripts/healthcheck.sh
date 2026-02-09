#!/usr/bin/env bash
set -euo pipefail
curl -fsS "http://127.0.0.1:${METRICS_PORT:-9301}/metrics" >/dev/null
echo "ok"
