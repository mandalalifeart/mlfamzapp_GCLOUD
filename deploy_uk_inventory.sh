#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
source ./_deploy_common.sh

PROJECT_ID="mlfamzapp"
FUNCTION_NAME="GetUkInventory"
REGION="us-central1"
RUNTIME="python312"
ENTRY_POINT="GetUkInventory"
SOURCE_DIR="."
ENV_FILE=".env"
GEN2=true
FORCE_PUSH=false
TIMEOUT_SECONDS=180
SERVICE_ACCOUNT="mlfamzapp@appspot.gserviceaccount.com"
COMMIT_MSG="${1:-UpdateLogic}"

required_vars=(
  CLIENT_ID_EU
  CLIENT_SECRET_EU
  REFRESH_TOKEN_EU
)

run_deploy
