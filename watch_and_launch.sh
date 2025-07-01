#!/bin/bash

BUCKET="contact-scraper-bucket"
ZONE="us-central1-a"
TEMPLATE="scraper-template-v4"
PROJECT="contact-scraper-463913"
RUN_ID="$1"  # UUID passed as first argument

if [[ -z "$RUN_ID" ]]; then
  echo "❌ No RUN_ID provided. Usage: ./watch_and_launch.sh <run_id>"
  exit 1
fi

CHUNK_PREFIX="users/$RUN_ID/chunks/"
NUM_CHUNKS=$(gsutil ls gs://$BUCKET/$CHUNK_PREFIX | grep -c 'chunk_')

echo "Found $NUM_CHUNKS chunk files for run_id=$RUN_ID"

for i in $(seq 1 $NUM_CHUNKS); do
  VM_NAME="scraper-vm-${i}"

  if gcloud compute instances describe "$VM_NAME" --zone "$ZONE" --project "$PROJECT" &>/dev/null; then
    echo "$VM_NAME already exists, skipping..."
    continue
  fi

  echo "Launching $VM_NAME with run_id=$RUN_ID..."
  gcloud compute instances create "$VM_NAME" \
    --zone="$ZONE" \
    --source-instance-template="$TEMPLATE" \
    --project="$PROJECT" \
    --metadata=run_id="$RUN_ID"
done
