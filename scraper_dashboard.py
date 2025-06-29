import streamlit as st
import pandas as pd
import os
import json
import time
from google.cloud import storage
from google.oauth2 import service_account

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

        # Clear old files
        for prefix in ["chunks/", "results/logs/", "results/ALL_SUCCESS.csv", "results/ALL_FAILURES.csv"]:
            blobs = list(bucket.list_blobs(prefix=prefix if prefix.endswith("/") else ""))
            for blob in blobs:
                blob.delete()

        st.info("📤 Splitting CSV and uploading new chunks...")
        chunk_size = -(-len(input_df) // num_chunks)
        os.makedirs("chunks", exist_ok=True)

        for i in range(num_chunks):
            start = i * chunk_size
            end = min((i + 1) * chunk_size, len(input_df))
            chunk_df = input_df.iloc[start:end]
            if not chunk_df.empty:
                filename = f"chunk_{i + 1}.csv"
                chunk_df.to_csv(filename, index=False)
                blob = bucket.blob(f"chunks/{filename}")
                blob.upload_from_filename(filename)
                st.success(f"✅ Uploaded chunk: {filename} ({len(chunk_df)} rows)")

        st.balloons()
        st.success("🚀 All chunks uploaded. Scraping has now started automatically.")

# --- Real-time progress monitor ---
st.header("📊 Scraping Progress")

progress_placeholder = st.empty()
status_text = st.empty()

def count_completed_chunks():
    log_blobs = list(bucket.list_blobs(prefix="results/logs/"))
    return len([blob for blob in log_blobs if blob.name.endswith(".txt")])

completed_chunks = 0
for _ in range(60):  # Check progress for up to 5 minutes
    completed_chunks = count_completed_chunks()
    progress = int((completed_chunks / num_chunks) * 100)
    progress_placeholder.progress(progress, text=f"{completed_chunks}/{num_chunks} chunks completed")

    if completed_chunks >= num_chunks:
        status_text.success("✅ All chunks processed.")
        break
    time.sleep(5)

# --- Auto-merge logic ---
def download_csv_files(prefix):
    blobs = list(bucket.list_blobs(prefix=prefix))
    files = []
    for blob in blobs:
        if blob.name.endswith(".csv") and ("result_" in blob.name or "failures_" in blob.name):
            local_path = f"/tmp/{os.path.basename(blob.name)}"
            blob.download_to_filename(local_path)
            files.append(local_path)
    return files

def merge_csvs(files, pattern):
    dfs = []
    for file in files:
        if pattern in os.path.basename(file):
            dfs.append(pd.read_csv(file))
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

def upload_to_bucket(local_path, dest_name):
    blob = bucket.blob(dest_name)
    blob.upload_from_filename(local_path)

merge_success = False
if completed_chunks == num_chunks:
    st.info("🔀 Merging results...")
    files = download_csv_files("results/")
    success_df = merge_csvs(files, "result_")
    failure_df = merge_csvs(files, "failures_")

    if not success_df.empty:
        success_path = "/tmp/ALL_SUCCESS.csv"
        success_df.to_csv(success_path, index=False)
        upload_to_bucket(success_path, "results/ALL_SUCCESS.csv")

    if not failure_df.empty:
        failure_path = "/tmp/ALL_FAILURES.csv"
        failure_df.to_csv(failure_path, index=False)
        upload_to_bucket(failure_path, "results/ALL_FAILURES.csv")

    merge_success = True

# --- Download section ---
if merge_success:
    st.success("🎉 Merge completed. You can now download your results:")
    for fname in ["ALL_SUCCESS.csv", "ALL_FAILURES.csv"]:
        blob = bucket.blob(f"results/{fname}")
        local_path = f"/tmp/{fname}"
        if blob.exists():
            blob.download_to_filename(local_path)
            with open(local_path, "rb") as f:
                st.download_button(f"⬇️ Download {fname}", f, file_name=fname)

st.markdown("---")
st.caption("Powered by DataSol LTD.")
