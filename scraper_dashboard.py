import streamlit as st
import pandas as pd
import os
import json
import time
import uuid
import subprocess
from google.cloud import storage
from google.oauth2 import service_account

st.set_page_config(page_title="Contact Scraper Dashboard")
st.title("📇 Contact Scraper Dashboard")

# --- GCS Client Setup ---
if st.secrets.get("GCP_CREDENTIALS"):
    credentials_info = json.loads(st.secrets["GCP_CREDENTIALS"])
    credentials = service_account.Credentials.from_service_account_info(credentials_info)
    client = storage.Client(credentials=credentials, project=credentials_info.get("project_id"))
else:
    client = storage.Client()

bucket_name = st.secrets["BUCKET_NAME"]
bucket = client.bucket(bucket_name)

# --- Generate Session UUID ---
run_id = st.session_state.get("run_id")
if not run_id:
    run_id = str(uuid.uuid4())[:8]
    st.session_state["run_id"] = run_id
st.markdown(f"**Session ID:** `{run_id}`")

# --- Upload + Split CSV ---
uploaded_file = st.file_uploader("Upload full LinkedIn CSV to split and scrape", type=["csv"])
num_chunks = 4

if uploaded_file:
    input_df = pd.read_csv(uploaded_file)
    st.write(f"✅ Dataframe loaded: {len(input_df)} rows")

    if st.button("Split, Upload & Launch Scraping"):
        st.info("🧹 Clearing previous session files...")

        # --- SAFE DELETE ---
        # Clear user-specific files
        for prefix in [
            f"users/{run_id}/chunks/",
            f"users/{run_id}/results/logs/",
            f"users/{run_id}/results/ALL_SUCCESS.csv",
            f"users/{run_id}/results/ALL_FAILURES.csv"
        ]:
            blobs = list(bucket.list_blobs(prefix=prefix if prefix.endswith("/") else ""))
            for blob in blobs:
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
                blob = bucket.blob(f"users/{run_id}/chunks/{filename}")
                blob.upload_from_filename(filename)
                st.success(f"✅ Uploaded chunk: {filename} ({len(chunk_df)} rows)")

        st.balloons()
        st.success("🚀 All chunks uploaded. Launching scraper...")

        # Launch controller watcher with run_id
        try:
            subprocess.run(["gcloud", "compute", "ssh", "controller-vm",
                            "--command", f"bash ~/watch_and_launch.sh {run_id}"], check=True)
            st.success("🧠 Watcher launched for scraping.")
        except Exception as e:
            st.error(f"❌ Failed to start watcher: {e}")

# --- Progress Monitoring with Auto Refresh ---
st.header("📊 Scraping Progress")
progress_placeholder = st.empty()
status_text = st.empty()

def count_completed_chunks():
    result_blobs = list(bucket.list_blobs(prefix=f"users/{run_id}/results/"))
    return len([b for b in result_blobs if b.name.endswith(".csv") and "result_" in b.name])

completed_chunks = 0
for _ in range(60):  # 5 minutes max (60 × 5s)
    completed_chunks = count_completed_chunks()
    progress = int((completed_chunks / num_chunks) * 100)
    progress_placeholder.progress(progress, text=f"{completed_chunks}/{num_chunks} chunks completed")

    if completed_chunks >= num_chunks:
        status_text.success("✅ All chunks processed.")
        break
    time.sleep(5)

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

merge_success = False
if completed_chunks == num_chunks:
    st.info("🔀 Merging results...")
    files = download_csv_files(f"users/{run_id}/results/")
    success_df = merge_csvs(files, "result_")
    failure_df = merge_csvs(files, "failures_")

    if not success_df.empty:
        success_path = "/tmp/ALL_SUCCESS.csv"
        success_df.to_csv(success_path, index=False)
        upload_to_bucket(success_path, f"users/{run_id}/results/ALL_SUCCESS.csv")

    if not failure_df.empty:
        failure_path = "/tmp/ALL_FAILURES.csv"
        failure_df.to_csv(failure_path, index=False)
        upload_to_bucket(failure_path, f"users/{run_id}/results/ALL_FAILURES.csv")

    merge_success = True

# --- Download Buttons ---
if merge_success:
    st.success("🎉 Merge completed. You can now download your results:")
    for fname in ["ALL_SUCCESS.csv", "ALL_FAILURES.csv"]:
        blob = bucket.blob(f"users/{run_id}/results/{fname}")
        local_path = f"/tmp/{fname}"
        if blob.exists():
            blob.download_to_filename(local_path)
            with open(local_path, "rb") as f:
                st.download_button(f"⬇️ Download {fname}", f, file_name=fname)

st.markdown("---")
st.caption(" Powered by eCore Services.")
