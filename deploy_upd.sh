#!/usr/bin/env bash
set -euo pipefail

# ---- config ----
PROJECT_ID="mlfamzapp"
FUNCTION_NAME="UpdateSkuSalesMonth"
REGION="us-central1"
RUNTIME="python312"
ENTRY_POINT="UpdateSkuSalesMonth"
SOURCE_DIR="."
ENV_FILE=".env"
TMP_ENV_YAML="$(mktemp)"

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
  SUPABASE_URL
  SUPABASE_SERVICE_ROLE_KEY
)

for var in "${required_vars[@]}"; do
  if [[ -z "${!var:-}" ]]; then
    echo "Missing required env var: $var"
    rm -f "$TMP_ENV_YAML"
    exit 1
  fi
done

# ---- escape values for YAML ----
yaml_escape() {
  local s="${1//\\/\\\\}"
  s="${s//\"/\\\"}"
  printf '"%s"' "$s"
}

# ---- build temporary env yaml from .env ----
cat > "$TMP_ENV_YAML" <<EOF
CLIENT_SECRET_USA: $(yaml_escape "$CLIENT_SECRET_USA")
CLIENT_SECRET_EU: $(yaml_escape "$CLIENT_SECRET_EU")
REFRESH_TOKEN_USA: $(yaml_escape "$REFRESH_TOKEN_USA")
REFRESH_TOKEN_EU: $(yaml_escape "$REFRESH_TOKEN_EU")
CLIENT_ID_USA: $(yaml_escape "$CLIENT_ID_USA")
CLIENT_ID_EU: $(yaml_escape "$CLIENT_ID_EU")
SUPABASE_URL: $(yaml_escape "$SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY: $(yaml_escape "$SUPABASE_SERVICE_ROLE_KEY")
EOF

# ---- deploy ----
gcloud config set project "$PROJECT_ID" >/dev/null

git add .
git commit -m "${1:-UpdateLogic}" || true
git push -u origin main

gcloud functions deploy "$FUNCTION_NAME" \
  --gen2 \
  --runtime="$RUNTIME" \
  --region="$REGION" \
  --source="$SOURCE_DIR" \
  --entry-point="$ENTRY_POINT" \
  --trigger-http \
  --allow-unauthenticated \
  --env-vars-file="$TMP_ENV_YAML"

# ---- cleanup ----
rm -f "$TMP_ENV_YAML"

echo "Deployment completed."