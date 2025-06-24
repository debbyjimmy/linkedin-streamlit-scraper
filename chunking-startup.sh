#! /bin/bash

# SETTINGS
BUCKET_NAME="contact-scraper-bucket"
INPUT_FILE="uploads/linkedin_full.csv"
CHUNKS_DIR="chunks"
CHUNK_COUNT=20

# Install Python and pip
apt-get update
apt-get install -y python3 python3-pip

# Install pandas and gcs client
pip3 install pandas google-cloud-storage

# Create working directory
mkdir -p /workspace
cd /workspace

# Download input and script
gsutil cp gs://$BUCKET_NAME/$INPUT_FILE input.csv
gsutil cp gs://$BUCKET_NAME/split_csv_into_chunks.py .

# Run chunking
python3 split_csv_into_chunks.py --input input.csv --chunks $CHUNK_COUNT --output $CHUNKS_DIR

# Upload result chunks
gsutil -m cp $CHUNKS_DIR/chunk_*.csv gs://$BUCKET_NAME/chunks/

# Shutdown
shutdown -h now
