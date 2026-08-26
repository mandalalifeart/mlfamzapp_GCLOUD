#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
source ./_deploy_common.sh

PROJECT_ID="mlfamzapp"
FUNCTION_NAME="RunEtsyMcfFulfillment"
REGION="us-central1"
RUNTIME="python312"
ENTRY_POINT="RunEtsyMcfFulfillment"
SOURCE_DIR="."
ENV_FILE=".env"
GEN2=true
FORCE_PUSH=false
TIMEOUT_SECONDS=300
SERVICE_ACCOUNT="mlfamzapp@appspot.gserviceaccount.com"
COMMIT_MSG="${1:-UpdateLogic}"

required_vars=(
  ETSY_Keystring
  ETSY_SHARED_SECRET
  POCKETBASE_URL
  POCKETBASE_ADMIN_EMAIL
  POCKETBASE_ADMIN_PASSWORD
  ADMIN_KEY
  GMAIL_USER
  GMAIL_APP_PASSWORD
  REPORT_EMAIL_TO
  MCF_TELEGRAM_BOT_TOKEN
  MCF_TELEGRAM_CHAT_ID
)

run_deploy
