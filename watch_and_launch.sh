#!/bin/bash

BUCKET="contact-scraper-bucket"
ZONE="us-central1-a"
TEMPLATE="scraper-template-v4"
PROJECT="contact-scraper-463913"

while true; do
  echo "⏳ Checking for chunk files..."
  NUM_CHUNKS=$(gsutil ls gs://$BUCKET/chunks/ | grep -c 'chunk_')

  for i in $(seq 1 $NUM_CHUNKS); do
    VM_NAME="scraper-vm-${i}"

    if gcloud compute instances describe "$VM_NAME" --zone "$ZONE" --project "$PROJECT" &>/dev/null; then
      echo "$VM_NAME already exists, skipping..."
      continue
    fi

    echo "🚀 Launching $VM_NAME..."
    gcloud compute instances create "$VM_NAME" \
      --zone="$ZONE" \
      --source-instance-template="$TEMPLATE" \
      --project="$PROJECT"
  done

  echo "✅ Check completed. Sleeping..."
  sleep 30
done
