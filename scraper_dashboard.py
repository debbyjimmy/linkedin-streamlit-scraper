import streamlit as st
import pandas as pd
import os
import json
import time
import uuid
import shutil
import zipfile
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
num_chunks = 3

if uploaded_file:
    input_df = pd.read_csv(uploaded_file)
    st.write(f"✅ Dataframe loaded: {len(input_df)} rows")

    rows_per_chunk = -(-len(input_df) // num_chunks)
    est_time_per_chunk_min = rows_per_chunk / 200
    st.info(f"⏱️ Estimated processing time: ~{int(est_time_per_chunk_min)} minutes per chunk")

    if st.button("Split & Upload"):
        st.info("🧹 Clearing previous session files...")
        for prefix in [f"users/{run_id}/chunks/", f"users/{run_id}/results/"]:
            blobs = list(bucket.list_blobs(prefix=prefix))
            for blob in blobs:
                blob.delete()

        st.info("📤 Splitting CSV and uploading new chunks...")
        os.makedirs("chunks", exist_ok=True)
        for i in range(num_chunks):
            start = i * rows_per_chunk
            end = min((i + 1) * rows_per_chunk, len(input_df))
            chunk_df = input_df.iloc[start:end]
            if not chunk_df.empty:
                filename = f"chunk_{i + 1}.csv"
                path = os.path.join("chunks", filename)
                chunk_df.to_csv(path, index=False)
                blob = bucket.blob(f"users/{run_id}/chunks/{filename}")
                blob.upload_from_filename(path)
                st.success(f"✅ Uploaded chunk: {filename} ({len(chunk_df)} rows)")
                os.remove(path)
        shutil.rmtree("chunks", ignore_errors=True)
        st.balloons()
        st.success("🚀 All chunks uploaded. Scraping will start automatically.")

# --- Progress Monitoring ---
st.header("📊 Scraping Progress")
progress_placeholder = st.empty()
status_text = st.empty()

def fetch_progress_records():
    blob = bucket.blob(f"users/{run_id}/results/progress.jsonl")
    if not blob.exists():
        return []

    local_path = f"/tmp/progress_{run_id}.jsonl"
    try:
        blob.download_to_filename(local_path)
        with open(local_path, "r") as f:
            return [json.loads(line) for line in f if line.strip()]
    except Exception as e:
        st.warning(f"Error reading progress log: {e}")
        return []

def count_completed_chunks(records):
    return sum(1 for r in records if r.get("status") == "completed")

# Continuous wait loop until all chunks are marked "completed"
completed_chunks = 0
attempt = 0
while True:
    records = fetch_progress_records()
    completed_chunks = count_completed_chunks(records)
    progress = int((completed_chunks / num_chunks) * 100)
    progress_placeholder.progress(progress, text=f"{completed_chunks}/{num_chunks} chunks completed")

    if completed_chunks >= num_chunks:
        status_text.success("✅ All chunks completed.")
        break

    attempt += 1
    status_text.info(f"⏳ Waiting... (Attempt {attempt})")
    time.sleep(5)

# Optional: show completed records
if completed_chunks:
    with st.expander("📋 View completed chunk logs"):
        st.json([r for r in records if r.get("status") == "completed"])

# --- Merge Results ---
def extract_zip_to_tmp(zip_path):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall("/tmp")

def download_and_extract_zip_files(prefix):
    blobs = list(bucket.list_blobs(prefix=prefix))
    for blob in blobs:
        if blob.name.endswith(".zip") and "scrape_results_" in blob.name:
            local_path = f"/tmp/{os.path.basename(blob.name)}_
