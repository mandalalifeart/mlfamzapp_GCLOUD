#!/bin/bash
# One-off reminder to the user via @baba_update_bot: telegram_reminder.sh "<text>"
cd /home/bababot/AmzBot/gcloud_git && set -a && . ./.env && set +a
curl -s -X POST "https://api.telegram.org/bot${UPDATE_TELEGRAM_BOT_TOKEN}/sendMessage" \
  --data-urlencode "chat_id=${UPDATE_TELEGRAM_CHAT_ID}" --data-urlencode "text=$1" > /dev/null
