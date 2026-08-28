#!/usr/bin/env bash
# Cron entry point - runs one Cloud Function locally via run_local.py.
# Usage: run_local.sh <FunctionName>
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
set -a
source ./.env
set +a
mkdir -p logs
.venv/bin/python3 run_local.py "$1"
