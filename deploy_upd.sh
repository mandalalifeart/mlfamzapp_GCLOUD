#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
source ./_deploy_common.sh

PROJECT_ID="mlfamzapp"
FUNCTION_NAME="UpdateSkuSalesMonth"
REGION="us-central1"
RUNTIME="python312"
ENTRY_POINT="UpdateSkuSalesMonth"
SOURCE_DIR="."
ENV_FILE=".env"
GEN2=true
FORCE_PUSH=false
SERVICE_ACCOUNT="mlfamzapp@appspot.gserviceaccount.com"
COMMIT_MSG="${1:-UpdateLogic}"

required_vars=(
  CLIENT_SECRET_USA
  CLIENT_SECRET_EU
  REFRESH_TOKEN_USA
  REFRESH_TOKEN_EU
  CLIENT_ID_USA
  CLIENT_ID_EU
  POCKETBASE_URL
  POCKETBASE_ADMIN_EMAIL
  POCKETBASE_ADMIN_PASSWORD
  ADMIN_KEY
)

run_deploy
