#!/bin/bash
exec > /var/log/startup-script.log 2>&1

echo "📦 Startup script begins..."

BUCKET="contact-scraper-bucket"

# Metadata
VM_NAME=$(curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/name)
CHUNK_INDEX=$(echo "$VM_NAME" | grep -o '[0-9]\+$')
ZONE=$(curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/zone | awk -F/ '{print $NF}')
PROJECT_ID=$(curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/project/project-id)

# Retry to fetch run_id metadata
MAX_RETRIES=10
RETRY_INTERVAL=2
for i in $(seq 1 $MAX_RETRIES); do
  RUN_ID_RAW=$(curl -s -H "Metadata-Flavor: Google" \
    http://metadata.google.internal/computeMetadata/v1/instance/attributes/run_id)

  if [[ -n "$RUN_ID_RAW" ]]; then
    break
  fi

  echo "⏳ Waiting for run_id metadata... attempt $i"
  sleep $RETRY_INTERVAL
done

if [[ -z "$RUN_ID_RAW" ]]; then
  echo "❌ run_id metadata not found after $MAX_RETRIES attempts. Aborting."
  exit 1
fi

# Clean and log the run_id
RUN_ID=$(echo "$RUN_ID_RAW" | tr -d '[:space:]')

echo "🖥️ VM Name: '$VM_NAME'"
echo "📄 Chunk Index: '$CHUNK_INDEX'"
echo "🧾 Sanitized Run ID: '$RUN_ID'"
export RUN_ID="$RUN_ID"

mkdir -p ~/workspace
cd ~/workspace || exit 1

echo "📥 Downloading files..."
gsutil cp "gs://$BUCKET/linkedin_scraper.py" . || exit 1
gsutil cp "gs://$BUCKET/config.json" . || exit 1
gsutil cp "gs://$BUCKET/users/$RUN_ID/chunks/chunk_${CHUNK_INDEX}.csv" input.csv

if [[ ! -f input.csv ]]; then
  echo "❌ Chunk file missing. Aborting."
  gsutil cp /var/log/startup-script.log "gs://$BUCKET/users/$RUN_ID/results/logs/error_${CHUNK_INDEX}.txt"
  exit 1
fi

echo "🚀 Running scraper..."
python3 linkedin_scraper.py --input input.csv --output result_${CHUNK_INDEX}.csv --batch-index "${CHUNK_INDEX}"

ZIP_FILE="scrape_results_${CHUNK_INDEX}.zip"

echo "📤 Uploading zipped results..."
if [[ -f "$ZIP_FILE" ]]; then
  gsutil cp "$ZIP_FILE" "gs://$BUCKET/users/$RUN_ID/results/"
else
  echo "⚠️ Zip file not found. No results to upload."
fi

echo "📝 Uploading log..."
gsutil cp /var/log/startup-script.log "gs://$BUCKET/users/$RUN_ID/results/logs/log_${CHUNK_INDEX}.txt"

# Append progress to central file
echo "🧾 Appending to central progress log..."
cat <<EOF > progress_tmp.json
{
  "run_id": "$RUN_ID",
  "chunk_index": $CHUNK_INDEX,
  "vm_name": "$VM_NAME",
  "status": "completed",
  "success_count": $(grep -c '"Success"' result_${CHUNK_INDEX}.csv 2>/dev/null || echo 0),
  "failure_count": $(grep -v '"Success"' result_${CHUNK_INDEX}.csv | wc -l),
  "start_time": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "end_time": "$(date -u +"%Y-%m-%dT%H:%M:%SZ")",
  "result_path": "gs://$BUCKET/users/$RUN_ID/results/$ZIP_FILE"
}
EOF

# Safely append to central progress.jsonl in root
CENTRAL_PROGRESS="gs://$BUCKET/progress.jsonl"
TMP_LINE="gs://$BUCKET/tmp/progress_chunk_${RUN_ID}_${CHUNK_INDEX}.jsonl"

gsutil cp progress_tmp.json "$TMP_LINE"

if gsutil -q stat "$CENTRAL_PROGRESS"; then
  gsutil compose "$CENTRAL_PROGRESS" "$TMP_LINE" "$CENTRAL_PROGRESS"
else
  gsutil mv "$TMP_LINE" "$CENTRAL_PROGRESS"
fi

# Cleanup
rm -f progress_tmp.json

echo "✅ Finished chunk ${CHUNK_INDEX}, deleting VM..."
gcloud compute instances delete "$VM_NAME" --zone="$ZONE" --quiet || sudo shutdown -h now
