#!/bin/bash

CHUNK_INDEX=17  # This will be replaced per instance
BUCKET="contact-scraper-bucket"

cd /workspace
mkdir -p chunks

# Pull the scraper script, config, and assigned chunk
gsutil cp gs://$BUCKET/linkedin_scraper.py .
gsutil cp gs://$BUCKET/config.json .
gsutil cp gs://$BUCKET/chunks/chunk_${CHUNK_INDEX}.csv input.csv

# Run the scraper
python3 linkedin_scraper.py --input input.csv --output result_${CHUNK_INDEX}.csv --shutdown --batch-index ${CHUNK_INDEX}

# Upload the result to GCS
gsutil cp result_${CHUNK_INDEX}.csv gs://$BUCKET/results/
