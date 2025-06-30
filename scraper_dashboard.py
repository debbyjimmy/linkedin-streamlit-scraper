import streamlit as st
import pandas as pd
import os
import json
from time import sleep
from google.cloud import storage
from google.oauth2 import service_account
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="Contact Scraper Dashboard")
st.title("📇 Contact Scraper Dashboard")

# 🔄 Auto-refresh every 5 seconds
st_autorefresh(interval=5000, limit=None, key="auto_refresh")

# --- GCS Client Setup ---
if st.secrets.get("GCP_CREDENTIALS"):
    credentials_info = json.loads(st.secrets["GCP_CREDENTIALS"])
    credentials = service_account.Credentials.from_service_account_info(credentials_info)
    client = storage.Client(credentials=credentials, project=credentials_info.get("project_id"))
else:
    client = storage.Client()

bucket_name = st.secrets["BUCKET_NAME"]
bucket = client.bucket(bucket_name)

# --- Upload + Split CSV ---
uploaded_file = st.file_uploader("Upload full LinkedIn CSV to split and scrape", type=["csv"])
num_chunks = st.number_input("Number of chunks", min_value=1, max_value=100, value=4)

if uploaded_file:
    input_df = pd.read_csv(uploaded_file)
    st.write(f"✅ Dataframe loaded: {len(input_df)} rows")

    if st.button("Split and Upload Chunks"):
        st.info("🧹 Clearing previous chunks, logs, and merged results...")

        # --- SAFE DELETE ---
        for blob in bucket.list_blobs(prefix="chunks/"):
            if blob.name.endswith(".csv") and "chunk_" in blob.name:
                blob.delete()

        for blob in bucket.list_blobs(prefix="results/logs/"):
            if blob.name.endswith(".txt") and "log_" in blob.name:
                blob.delete()

        for filename in ["results/ALL_SUCCESS.csv", "results/ALL_FAILURES.csv"]:
            blob = bucket.blob(filename)
            if blob.exists():
                blob.delete()

        # Upload new chunks
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

# --- Progress Monitoring ---
st.header("📊 Scraping Progress")

def count_completed_chunks():
    log_blobs = list(bucket.list_blobs(prefix="results/logs/"))
    return len([blob for blob in log_blobs if blob.name.endswith(".txt")])

completed_chunks = count_completed_chunks()
progress = int((completed_chunks / num_chunks) * 100) if num_chunks else 0
st.progress(progress, text=f"{completed_chunks}/{int(num_chunks)} chunks completed")

# --- Merge Results ---
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
    dfs = [pd.read_csv(file) for file in files if pattern in os.path.basename(file)]
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

def upload_to_bucket(local_path, dest_name):
    blob = bucket.blob(dest_name)
    blob.upload_from_filename(local_path)

# Check if results already exist
merge_success = False
all_success_blob = bucket.blob("results/ALL_SUCCESS.csv")
all_failures_blob = bucket.blob("results/ALL_FAILURES.csv")

if completed_chunks == num_chunks:
    if not (all_success_blob.exists() and all_failures_blob.exists()):
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

# --- Download Buttons ---
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
st.caption("Powered by eCore Services.")
