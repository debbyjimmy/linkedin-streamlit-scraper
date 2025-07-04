#!/bin/bash

set -euo pipefail

BUCKET="contact-scraper-bucket"
ZONE="us-central1-a"
TEMPLATE="scraper-template-v14"
PROJECT="contact-scraper-463913"

# Temp file for cleaned progress log
CENTRAL_PROGRESS_FILE=$(mktemp /tmp/central_progress_clean.XXXXXX.jsonl)

echo "📥 Downloading and cleaning central progress file..."
if gsutil cp "gs://$BUCKET/progress.jsonl" - 2>/dev/null | jq -c . 2>/dev/null > "$CENTRAL_PROGRESS_FILE"; then
  echo "✅ Cleaned progress file ready: $CENTRAL_PROGRESS_FILE"
else
  echo "⚠️ Failed to fetch or clean progress.jsonl. Proceeding with empty progress state."
  > "$CENTRAL_PROGRESS_FILE"
fi

# Map of run_id,chunk_index to indicate completed .zip result
declare -A COMPLETED_MAP

while IFS= read -r line; do
  [[ -z "$line" || "$line" == "null" ]] && continue

  run_id=$(echo "$line" | jq -r '.run_id // empty')
  chunk_index=$(echo "$line" | jq -r '.chunk_index // empty')
  result_path=$(echo "$line" | jq -r '.result_path // empty')

  if [[ "$run_id" && "$chunk_index" && "$result_path" =~ \.zip$ ]]; then
    COMPLETED_MAP["$run_id,$chunk_index"]=1
  fi
done < "$CENTRAL_PROGRESS_FILE"

# Loop through all sessions
for CHUNK_PATH in $(gsutil ls gs://$BUCKET/users/*/chunks/ 2>/dev/null); do
  if [[ "$CHUNK_PATH" =~ users/([^/]+)/chunks/ ]]; then
    RUN_ID="${BASH_REMATCH[1]}"
    echo -e "\n🔍 Found session: $RUN_ID"

    NUM_CHUNKS=$(gsutil ls "gs://$BUCKET/users/$RUN_ID/chunks/" | grep -c 'chunk_')
    echo "📦 Found $NUM_CHUNKS chunk files for run_id=$RUN_ID"

    # Count how many chunks have zipped results
    COMPLETED=0
    for i in $(seq 1 "$NUM_CHUNKS"); do
      key="$RUN_ID,$i"
      if [[ "${COMPLETED_MAP[$key]+exists}" ]]; then
        ((COMPLETED++))
      fi
    done

    echo "✅ $COMPLETED of $NUM_CHUNKS chunks completed for $RUN_ID"

    if [[ "$COMPLETED" -eq "$NUM_CHUNKS" ]]; then
      echo "🎯 Session $RUN_ID fully completed. Skipping..."
      continue
    fi

    # Launch missing chunks
    for i in $(seq 1 "$NUM_CHUNKS"); do
      key="$RUN_ID,$i"
      if [[ "${COMPLETED_MAP[$key]+exists}" ]]; then
        echo "✅ Chunk $i already completed, skipping..."
        continue
      fi

      VM_NAME="scraper-vm-${RUN_ID}-${i}"
      echo "🚀 Launching $VM_NAME with run_id=$RUN_ID..."
      gcloud compute instances create "$VM_NAME" \
        --zone="$ZONE" \
        --source-instance-template="$TEMPLATE" \
        --project="$PROJECT" \
        --metadata=startup-script-url=gs://$BUCKET/startup.sh,run_id="$RUN_ID"
    done
  fi
done

rm -f "$CENTRAL_PROGRESS_FILE"
