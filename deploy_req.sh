#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
source ./_deploy_common.sh

PROJECT_ID="mlfamzapp"
FUNCTION_NAME="MlfReportReq"
REGION="us-central1"
RUNTIME="python312"
ENTRY_POINT="MlfReportReq"
SOURCE_DIR="."
ENV_FILE=".env"
GEN2=false
FORCE_PUSH=true
COMMIT_MSG="${1:-UpdateLogic}"

required_vars=(
  CLIENT_SECRET_USA
  CLIENT_SECRET_EU
  REFRESH_TOKEN_USA
  REFRESH_TOKEN_EU
  CLIENT_ID_USA
  CLIENT_ID_EU
)

run_deploy
