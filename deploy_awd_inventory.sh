#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
source ./_deploy_common.sh

PROJECT_ID="mlfamzapp"
FUNCTION_NAME="GetAwdInventory"
REGION="us-central1"
RUNTIME="python312"
ENTRY_POINT="GetAwdInventory"
SOURCE_DIR="."
ENV_FILE=".env"
GEN2=true
FORCE_PUSH=false
TIMEOUT_SECONDS=180
SERVICE_ACCOUNT="mlfamzapp@appspot.gserviceaccount.com"
COMMIT_MSG="${1:-UpdateLogic}"

required_vars=(
  CLIENT_ID_USA
  CLIENT_SECRET_USA
  REFRESH_TOKEN_USA
)

run_deploy
