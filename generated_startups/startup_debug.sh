#!/bin/bash

CHUNK_INDEX=$(curl -s "http://metadata.google.internal/computeMetadata/v1/instance/attributes/chunk-index" -H "Metadata-Flavor: Google")
BUCKET="contact-scraper-bucket"

cd /opt/scraper || exit 1

# Download fresh config and chunk
gsutil cp gs://$BUCKET/config.json .
gsutil cp gs://$BUCKET/chunks/chunk_${CHUNK_INDEX}.csv input.csv

# Run scraper
python3 linkedin_scraper.py --input input.csv --output /tmp/result_${CHUNK_INDEX}.csv --batch-index ${CHUNK_INDEX}

# Upload result
gsutil cp /tmp/result_${CHUNK_INDEX}.csv gs://$BUCKET/results/

