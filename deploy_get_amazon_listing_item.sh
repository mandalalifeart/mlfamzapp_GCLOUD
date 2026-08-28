#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
source ./_deploy_common.sh

PROJECT_ID="mlfamzapp"
FUNCTION_NAME="GetAmazonListingItem"
REGION="us-central1"
RUNTIME="python312"
ENTRY_POINT="GetAmazonListingItem"
SOURCE_DIR="."
ENV_FILE=".env"
GEN2=true
FORCE_PUSH=false
TIMEOUT_SECONDS=60
SERVICE_ACCOUNT="mlfamzapp@appspot.gserviceaccount.com"
COMMIT_MSG="${1:-UpdateLogic}"

required_vars=(
  CLIENT_ID_USA
  CLIENT_SECRET_USA
  REFRESH_TOKEN_USA
  CLIENT_ID_EU
  CLIENT_SECRET_EU
  REFRESH_TOKEN_EU
  AMAZON_SELLER_ID
  POCKETBASE_URL
  POCKETBASE_ADMIN_EMAIL
  POCKETBASE_ADMIN_PASSWORD
  ADMIN_KEY
)

run_deploy
