#!/bin/bash
# LATEST2

set -euo pipefail

BUCKET="contact-scraper-bucket"
ZONE="us-central1-a"
TEMPLATE="scraper-template-v14"
PROJECT="contact-scraper-463913"

# 📥 Download and clean central progress file
CENTRAL_PROGRESS_FILE=$(mktemp /tmp/central_progress_clean.XXXXXX.jsonl)
echo "📥 Downloading and cleaning central progress file..."

if gsutil cp "gs://$BUCKET/progress.jsonl" - 2>/dev/null | jq -c . 2>/dev/null > "$CENTRAL_PROGRESS_FILE"; then
  echo "✅ Cleaned progress file ready: $CENTRAL_PROGRESS_FILE"
else
  echo "⚠️ Failed to fetch or clean progress.jsonl. Proceeding with empty progress state."
  > "$CENTRAL_PROGRESS_FILE"
fi

# ✅ Build set of COMPLETED_SESSION_IDS
declare -A COMPLETED_SESSIONS
while IFS= read -r line; do
  [[ -z "$line" || "$line" == "null" ]] && continue
  run_id=$(echo "$line" | jq -r '.run_id // empty')
  [[ -n "$run_id" ]] && COMPLETED_SESSIONS["$run_id"]=1
done < "$CENTRAL_PROGRESS_FILE"

# 🔎 Get ALL session folders under users/
ALL_SESSIONS=($(gsutil ls -d "gs://$BUCKET/users/"*/ 2>/dev/null | awk -F/ '{print $(NF-1)}'))

if [ ${#ALL_SESSIONS[@]} -eq 0 ]; then
  echo "⚠️ No session folders found. Exiting..."
  exit 0
fi

echo "📋 All sessions found: ${ALL_SESSIONS[*]}"

# 🔄 Process sessions NOT in COMPLETED_SESSIONS
for RUN_ID in "${ALL_SESSIONS[@]}"; do
  if [[ "${COMPLETED_SESSIONS[$RUN_ID]+exists}" ]]; then
    echo "✅ Session $RUN_ID found in progress log. Skipping..."
    continue
  fi

  echo -e "\n🔍 Processing new session: $RUN_ID"
  CHUNK_FOLDER="gs://$BUCKET/users/$RUN_ID/chunks/"
  CHUNKS=($(gsutil ls "${CHUNK_FOLDER}chunk_*.csv" 2>/dev/null || true))

  NUM_CHUNKS=${#CHUNKS[@]}
  if [[ "$NUM_CHUNKS" -eq 0 ]]; then
    echo "❌ No chunk files found for $RUN_ID. Skipping..."
    continue
  fi

  echo "📦 Found $NUM_CHUNKS chunk files for run_id=$RUN_ID"

  for ((i=1; i<=NUM_CHUNKS; i++)); do
    VM_NAME="scraper-vm-${RUN_ID}-${i}"
    echo "🚀 Launching $VM_NAME for chunk $i"
    gcloud compute instances create "$VM_NAME" \
      --zone="$ZONE" \
      --source-instance-template="$TEMPLATE" \
      --project="$PROJECT" \
      --metadata=startup-script-url=gs://$BUCKET/startup.sh,run_id="$RUN_ID"
  done
done

rm -f "$CENTRAL_PROGRESS_FILE"
