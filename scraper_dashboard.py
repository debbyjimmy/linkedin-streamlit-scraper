import streamlit as st
import pandas as pd
import os
import json
import time
from google.cloud import storage
from google.oauth2 import service_account
from googleapiclient import discovery

st.set_page_config(page_title="Contact Scraper Dashboard")
st.title("📇 Contact Scraper Dashboard")

# Initialize GCS client
if st.secrets.get("GCP_CREDENTIALS"):
    credentials_info = json.loads(st.secrets["GCP_CREDENTIALS"])
    credentials = service_account.Credentials.from_service_account_info(credentials_info)
    client = storage.Client(credentials=credentials, project=credentials_info.get("project_id"))
else:
    client = storage.Client()

bucket_name = st.secrets["BUCKET_NAME"]
bucket = client.bucket(bucket_name)

# Upload input CSV
uploaded_file = st.file_uploader("Upload full LinkedIn CSV to split and scrape", type=["csv"])
num_chunks = st.number_input("Number of chunks", min_value=1, max_value=100, value=4)

if uploaded_file:
    input_df = pd.read_csv(uploaded_file)
    st.write(f"✅ Dataframe loaded: {len(input_df)} rows")

    if st.button("Split and Upload Chunks"):
        st.info("🧹 Clearing previous chunks, logs, and merged results...")

        # Clear chunks
        chunk_blobs = list(bucket.list_blobs(prefix="chunks/"))
        for blob in chunk_blobs:
            blob.delete()

        # Clear logs
        log_blobs = list(bucket.list_blobs(prefix="results/logs/"))
        for blob in log_blobs:
            blob.delete()

        # Clear merged results
        for name in ["ALL_SUCCESS.csv", "ALL_FAILURES.csv"]:
            blob = bucket.blob(f"results/{name}")
            if blob.exists():
                blob.delete()

        st.write(f"🗑️ Cleared {len(chunk_blobs)} chunks, {len(log_blobs)} logs, and previous results.")

        st.info("📤 Splitting CSV and uploading new chunks...")
        chunk_size = -(-len(input_df) // num_chunks)
        os.makedirs("chunks", exist_ok=True)

        for i in range(num_chunks):
            start = i * chunk_size
            end = min((i + 1) * chunk_size, len(input_df))
            chunk_df = input_df.iloc[start:end]
            if chunk_df.empty:
                continue
            filename = f"chunk_{i + 1}.csv"
            chunk_df.to_csv(filename, index=False)
            blob = bucket.blob(f"chunks/{filename}")
            blob.upload_from_filename(filename)
            st.success(f"✅ Uploaded chunk: {filename} ({len(chunk_df)} rows)")

        st.balloons()

# Auto-refresh option
autorefresh = st.checkbox("🔄 Auto-refresh scraping progress", value=True)
if autorefresh:
    time.sleep(5)
    st.experimental_rerun()

# Scraping Progress Section
st.header("📊 Scraping Progress")
log_blobs = list(bucket.list_blobs(prefix="results/logs/"))
log_files = [blob for blob in log_blobs if blob.name.endswith(".txt")]
completed_chunks = len(log_files)
progress = int((completed_chunks / num_chunks) * 100)
st.progress(progress, text=f"{completed_chunks}/{num_chunks} chunks completed")

# Auto-merge results (this logic is backend-related, still included)
merge_success = False
if completed_chunks == num_chunks:
    if not os.path.exists("ALL_SUCCESS.csv") or not os.path.exists("ALL_FAILURES.csv"):
        os.system("python3 merge_results.py")
        for file_name in ["ALL_SUCCESS.csv", "ALL_FAILURES.csv"]:
            if os.path.exists(file_name):
                blob = bucket.blob(f"results/{file_name}")
                blob.upload_from_filename(file_name)
        merge_success = True
    else:
        merge_success = True

# Download buttons if merged files exist in bucket
st.header("📥 Download Merged Results")
merged_files = ["ALL_SUCCESS.csv", "ALL_FAILURES.csv"]
for file_name in merged_files:
    blob = bucket.blob(f"results/{file_name}")
    if blob.exists():
        blob.download_to_filename(file_name)
        with open(file_name, "rb") as f:
            st.download_button(f"⬇️ Download {file_name}", f, file_name=file_name)

st.markdown("---")
st.caption("Powered by GeoRAD Solutions.")
