#!/bin/bash
exec > /var/log/startup-script.log 2>&1

echo "📦 Startup script begins..."

BUCKET="contact-scraper-bucket"

# Metadata
VM_NAME=$(curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/name)
CHUNK_INDEX=$(echo $VM_NAME | grep -o '[0-9]\+$')
ZONE=$(curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/zone | awk -F/ '{print $NF}')
PROJECT_ID=$(curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/project/project-id)

echo "🖥️ VM Name: $VM_NAME"
echo "📄 Using chunk index: $CHUNK_INDEX"

# Workspace
mkdir -p ~/workspace
cd ~/workspace

# Download input files
echo "📥 Downloading files from bucket..."
gsutil cp gs://$BUCKET/linkedin_scraper.py .
gsutil cp gs://$BUCKET/config.json .
gsutil cp gs://$BUCKET/chunks/chunk_${CHUNK_INDEX}.csv input.csv

# Confirm contents
echo "📁 Workspace contents:"
ls -lh

# Run the scraper
echo "🚀 Running scraper..."
python3 linkedin_scraper.py --input input.csv --output result_${CHUNK_INDEX}.csv --batch-index ${CHUNK_INDEX}

# Upload results
echo "📤 Uploading output..."
gsutil cp result_${CHUNK_INDEX}.csv gs://$BUCKET/results/
gsutil cp failures_${CHUNK_INDEX}.csv gs://$BUCKET/results/ || echo "No failure file to upload."

# Upload log
echo "📝 Uploading log file..."
gsutil cp /var/log/startup-script.log gs://$BUCKET/results/logs/log_${CHUNK_INDEX}.txt

echo "✅ Finished chunk ${CHUNK_INDEX}, deleting VM..."

# Delete this VM once everything’s uploaded
VM_NAME=$(curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/name)
ZONE=$(curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/zone | awk -F/ '{print $NF}')

echo "🧹 Deleting self: $VM_NAME in $ZONE"
gcloud compute instances delete "$VM_NAME" --zone="$ZONE" --quiet

