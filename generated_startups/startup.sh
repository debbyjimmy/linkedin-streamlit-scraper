#!/bin/bash
exec > /var/log/startup-script.log 2>&1

echo "📦 Startup script begins..."

BUCKET="contact-scraper-bucket"

# Metadata
VM_NAME=$(curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/name)
CHUNK_INDEX=$(echo $VM_NAME | grep -o '[0-9]\+$')
ZONE=$(curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/zone | awk -F/ '{print $NF}')
PROJECT_ID=$(curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/project/project-id)
RUN_ID=$(curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/attributes/run_id)

echo "🖥️ VM Name: $VM_NAME"
echo "📄 Chunk Index: $CHUNK_INDEX"
echo "🧾 Run ID: $RUN_ID"

mkdir -p ~/workspace
cd ~/workspace

echo "📥 Downloading files..."
gsutil cp gs://$BUCKET/linkedin_scraper.py .
gsutil cp gs://$BUCKET/config.json .
gsutil cp gs://$BUCKET/users/$RUN_ID/chunks/chunk_${CHUNK_INDEX}.csv input.csv

if [[ ! -f input.csv ]]; then
  echo "❌ Chunk file missing. Aborting."
  gsutil cp /var/log/startup-script.log gs://$BUCKET/users/$RUN_ID/results/logs/error_${CHUNK_INDEX}.txt
  exit 1
fi

echo "🚀 Running scraper..."
python3 linkedin_scraper.py --input input.csv --output result_${CHUNK_INDEX}.csv --batch-index ${CHUNK_INDEX}

echo "📤 Uploading results..."
gsutil cp result_${CHUNK_INDEX}.csv gs://$BUCKET/users/$RUN_ID/results/
gsutil cp failures_${CHUNK_INDEX}.csv gs://$BUCKET/users/$RUN_ID/results/ || echo "No failure file."

echo "📝 Uploading log..."
gsutil cp /var/log/startup-script.log gs://$BUCKET/users/$RUN_ID/results/logs/log_${CHUNK_INDEX}.txt

echo "✅ Finished chunk ${CHUNK_INDEX}, deleting VM..."
gcloud compute instances delete "$VM_NAME" --zone="$ZONE" --quiet || sudo shutdown -h now
