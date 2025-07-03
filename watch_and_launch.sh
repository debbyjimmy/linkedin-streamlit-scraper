#!/bin/bash

BUCKET="contact-scraper-bucket"
ZONE="us-central1-a"
TEMPLATE="scraper-template-v10"
PROJECT="contact-scraper-463913"

# Loop through all session folders
for CHUNK_PATH in $(gsutil ls gs://$BUCKET/users/*/chunks/ 2>/dev/null); do
  # Extract RUN_ID from path: users/<run_id>/chunks/
  if [[ "$CHUNK_PATH" =~ users/([^/]+)/chunks/ ]]; then
    RUN_ID="${BASH_REMATCH[1]}"
    echo "🔍 Found session: $RUN_ID"

    NUM_CHUNKS=$(gsutil ls gs://$BUCKET/users/$RUN_ID/chunks/ | grep -c 'chunk_')
    echo "📦 Found $NUM_CHUNKS chunk files for run_id=$RUN_ID"

    # Check progress.jsonl
    PROGRESS_FILE="/tmp/progress_${RUN_ID}.jsonl"
    gsutil cp "gs://$BUCKET/users/$RUN_ID/results/progress.jsonl" "$PROGRESS_FILE" 2>/dev/null

    if [[ -f "$PROGRESS_FILE" ]]; then
      COMPLETED=$(grep -c '"status": "completed"' "$PROGRESS_FILE")
      echo "✅ $COMPLETED of $NUM_CHUNKS chunks marked completed"

      if [[ "$COMPLETED" -eq "$NUM_CHUNKS" ]]; then
        echo "🎯 Session $RUN_ID fully completed. Skipping..."
        continue
      fi
    fi

    for i in $(seq 1 $NUM_CHUNKS); do
      VM_NAME="scraper-vm-${RUN_ID}-${i}"

      # Skip chunk if result already uploaded
      RESULT_PATH="gs://$BUCKET/users/$RUN_ID/results/scrape_results_${i}.zip"
      if gsutil -q stat "$RESULT_PATH"; then
        echo "✅ Chunk $i already completed, skipping..."
        continue
      fi

      echo "🚀 Launching $VM_NAME with run_id=$RUN_ID..."
      gcloud compute instances create "$VM_NAME" \
        --zone="$ZONE" \
        --source-instance-template="$TEMPLATE" \
        --project="$PROJECT" \
        --metadata=startup-script-url=gs://$BUCKET/startup.sh,run_id="$RUN_ID"
    done
  fi
done
