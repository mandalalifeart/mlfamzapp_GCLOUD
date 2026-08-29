#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
source ./_deploy_common.sh

PROJECT_ID="mlfamzapp"
FUNCTION_NAME="GetAdsPortfolios"
REGION="us-central1"
RUNTIME="python312"
ENTRY_POINT="GetAdsPortfolios"
SOURCE_DIR="."
ENV_FILE=".env"
GEN2=true
FORCE_PUSH=false
TIMEOUT_SECONDS=60
SERVICE_ACCOUNT="mlfamzapp@appspot.gserviceaccount.com"
COMMIT_MSG="${1:-UpdateLogic}"

required_vars=(
  AD_CLIENT_ID_USA
  AD_CLIENT_SECRET_USA
  AD_CLIENT_ID_EU
  AD_CLIENT_SECRET_EU
  POCKETBASE_URL
  POCKETBASE_ADMIN_EMAIL
  POCKETBASE_ADMIN_PASSWORD
)

run_deploy
