# Sourced by deploy_*.sh. Callers set PROJECT_ID, FUNCTION_NAME, REGION, RUNTIME,
# ENTRY_POINT, SOURCE_DIR, ENV_FILE, GEN2, FORCE_PUSH, COMMIT_MSG, required_vars[]
# before calling run_deploy.

run_deploy() {
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

  set -a
  source "$ENV_FILE"
  set +a

  for var in "${required_vars[@]}"; do
    if [[ -z "${!var:-}" ]]; then
      echo "Missing required env var: $var"
      exit 1
    fi
  done

  gcloud config set project "$PROJECT_ID" >/dev/null

  # sync secrets to Secret Manager (creates on first run, adds a new version every deploy)
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
  git commit -m "${COMMIT_MSG:-UpdateLogic}" || true

  if [[ "$FORCE_PUSH" == "true" ]]; then
    git push -u origin main --force
  else
    git push -u origin main
  fi

  local gen2_flag=()
  if [[ "$GEN2" == "true" ]]; then
    gen2_flag=(--gen2)
  fi

  gcloud functions deploy "$FUNCTION_NAME" \
    "${gen2_flag[@]}" \
    --runtime="$RUNTIME" \
    --region="$REGION" \
    --source="$SOURCE_DIR" \
    --entry-point="$ENTRY_POINT" \
    --trigger-http \
    --allow-unauthenticated \
    --set-secrets="$secrets_flag"

  echo "Deployment completed."
}
