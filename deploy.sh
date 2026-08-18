#!/usr/bin/env bash
set -euo pipefail

# ---- config ----
PROJECT_ID="mlfamzapp"
FUNCTION_NAME="MlfReportGet"
REGION="us-central1"
RUNTIME="python312"
ENTRY_POINT="MlfReportGet"
SOURCE_DIR="."
ENV_FILE=".env"

# ---- checks ----
if [[ ! -f "$ENV_FILE" ]]; then
  echo "Missing $ENV_FILE"
  exit 1
fi

if [[ ! -f "main.py" ]]; then
  echo "main.py not found in $(pwd)"
  exit 1
fi

if [[ ! -f "requirements.txt" ]]; then
  echo "requirements.txt not found"
  exit 1
fi

# ---- load .env safely ----
set -a
source "$ENV_FILE"
set +a

# ---- required vars ----
required_vars=(
  CLIENT_SECRET_USA
  CLIENT_SECRET_EU
  REFRESH_TOKEN_USA
  REFRESH_TOKEN_EU
  CLIENT_ID_USA
  CLIENT_ID_EU
)

for var in "${required_vars[@]}"; do
  if [[ -z "${!var:-}" ]]; then
    echo "Missing required env var: $var"
    exit 1
  fi
done

# ---- deploy ----
gcloud config set project "$PROJECT_ID" >/dev/null

# ---- sync secrets to Secret Manager (creates on first run, adds a new version every deploy) ----
secrets_flag=""
for var in "${required_vars[@]}"; do
  if ! gcloud secrets describe "$var" >/dev/null 2>&1; then
    gcloud secrets create "$var" --replication-policy=automatic >/dev/null
  fi
  printf '%s' "${!var}" | gcloud secrets versions add "$var" --data-file=- >/dev/null
  secrets_flag+="${var}=${var}:latest,"
done
secrets_flag="${secrets_flag%,}"

git add .
git commit -m $1
git push -u origin main --force

gcloud functions deploy "$FUNCTION_NAME" \
  --runtime="$RUNTIME" \
  --region="$REGION" \
  --source="$SOURCE_DIR" \
  --entry-point="$ENTRY_POINT" \
  --trigger-http \
  --allow-unauthenticated \
  --set-secrets="$secrets_flag"

echo "Deployment completed."
