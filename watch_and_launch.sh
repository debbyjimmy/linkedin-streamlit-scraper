#!/bin/bash

BUCKET="contact-scraper-bucket"
ZONE="us-central1-a"
TEMPLATE="scraper-template-v11"
PROJECT="contact-scraper-463913"

# Download central progress file once
CENTRAL_PROGRESS_FILE="/tmp/central_progress.jsonl"
gsutil cp "gs://$BUCKET/progress.jsonl" "$CENTRAL_PROGRESS_FILE" 2>/dev/null

# Convert progress log into associative array: run_id:completed_chunks
declare -A COMPLETED_MAP

if [[ -f "$CENTRAL_PROGRESS_FILE" ]]; then
  while IFS= read -r line; do
    run_id=$(echo "$line" | jq -r '.run_id')
    chunk_index=$(echo "$line" | jq -r '.chunk_index')
    if [[ "$run_id" != "null" && "$chunk_index" != "null" ]]; then
      COMPLETED_MAP["$run_id,$chunk_index"]=1
    fi
  done < "$CENTRAL_PROGRESS_FILE"
fi

# Loop through all session folders
for CHUNK_PATH in $(gsutil ls gs://$BUCKET/users/*/chunks/ 2>/dev/null); do
  if [[ "$CHUNK_PATH" =~ users/([^/]+)/chunks/ ]]; then
    RUN_ID="${BASH_REMATCH[1]}"
    echo "🔍 Found session: $RUN_ID"

    NUM_CHUNKS=$(gsutil ls gs://$BUCKET/users/$RUN_ID/chunks/ | grep -c 'chunk_')
    echo "📦 Found $NUM_CHUNKS chunk files for run_id=$RUN_ID"

    # Count completed from central progress
    COMPLETED=0
    for i in $(seq 1 $NUM_CHUNKS); do
      if [[ "${COMPLETED_MAP[$RUN_ID,$i]}" == "1" ]]; then
        ((COMPLETED++))
      fi
    done

    echo "✅ $COMPLETED of $NUM_CHUNKS chunks completed for $RUN_ID"

    if [[ "$COMPLETED" -eq "$NUM_CHUNKS" ]]; then
      echo "🎯 Session $RUN_ID fully completed. Skipping..."
      continue
    fi

    # Launch instances for missing chunks
    for i in $(seq 1 $NUM_CHUNKS); do
      if [[ "${COMPLETED_MAP[$RUN_ID,$i]}" == "1" ]]; then
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
