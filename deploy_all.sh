#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

COMMIT_MSG="${1:-UpdateLogic}"

SCRIPTS=(
  deploy.sh
  deploy_req.sh
  deploy_upd.sh
  deploy_sales.sh
)

for script in "${SCRIPTS[@]}"; do
  echo "=== Running $script ==="
  ./"$script" "$COMMIT_MSG"
done

echo "All deployments completed."
