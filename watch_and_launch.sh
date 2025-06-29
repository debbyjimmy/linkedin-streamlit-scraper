#!/bin/bash

BUCKET="contact-scraper-bucket"
ZONE="us-central1-a"
TEMPLATE="scraper-template-v4"
PROJECT="contact-scraper-463913"

# Get number of chunks from the bucket
NUM_CHUNKS=$(gsutil ls gs://$BUCKET/chunks/ | grep -c 'chunk_')

echo "Found $NUM_CHUNKS chunk files."

for i in $(seq 1 $NUM_CHUNKS); do
  VM_NAME="scraper-vm-${i}"

  # Check if VM already exists
  if gcloud compute instances describe "$VM_NAME" --zone "$ZONE" --project "$PROJECT" &>/dev/null; then
    echo "$VM_NAME already exists, skipping..."
    continue
  fi

  echo "Launching $VM_NAME..."
  gcloud compute instances create "$VM_NAME" \
    --zone="$ZONE" \
    --source-instance-template="$TEMPLATE" \
    --project="$PROJECT"
done
