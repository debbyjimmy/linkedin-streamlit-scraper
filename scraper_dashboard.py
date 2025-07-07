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
from streamlit_autorefresh import st_autorefresh

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
        st.session_state["monitoring_active"] = True

# --- Progress Monitoring ---
if st.session_state.get("monitoring_active", False):
    st_autorefresh(interval=5000, key="autorefresh")

    st.header("📊 Scraping Progress")
    progress_placeholder = st.empty()
    status_text = st.empty()

    def fetch_central_progress():
        blob = bucket.blob("progress.jsonl")
        if not blob.exists():
            return []
        local_path = "/tmp/central_progress.jsonl"
        try:
            blob.download_to_filename(local_path)
            records = []
            with open(local_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError as e:
                        print(f"⚠️ Skipping bad line: {e}")
            return records
        except Exception as e:
            st.warning(f"Error reading progress log: {e}")
            return []

    def filter_records_by_run_id(records, run_id):
        return [r for r in records if r.get("run_id") == run_id]

    all_records = fetch_central_progress()
    session_records = filter_records_by_run_id(all_records, run_id)

    seen_chunks = set()
    for record in session_records:
        if (
            record.get("result_path", "").endswith(".zip")
            and isinstance(record.get("chunk_index"), int)
        ):
            seen_chunks.add(record["chunk_index"])

    completed_chunks = len(seen_chunks)
    progress = int((completed_chunks / num_chunks) * 100)
    progress_placeholder.progress(progress, text=f"{completed_chunks}/{num_chunks} chunks completed")

    if completed_chunks >= num_chunks:
        status_text.success("✅ All chunks completed.")
        st.session_state["monitoring_active"] = False

        with st.expander("📋 View completed logs"):
            st.json(session_records)

        # --- Merge Results ---
        def extract_zip_to_tmp(zip_path):
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall("/tmp")

        def download_and_extract_zip_files(prefix):
            blobs = list(bucket.list_blobs(prefix=prefix))
            for blob in blobs:
                if blob.name.endswith(".zip") and "scrape_results_" in blob.name:
                    local_path = f"/tmp/{os.path.basename(blob.name)}"
                    blob.download_to_filename(local_path)
                    extract_zip_to_tmp(local_path)

        def merge_csvs(pattern):
            files = [os.path.join("/tmp", f) for f in os.listdir("/tmp") if f.endswith(".csv") and pattern in f]
            return pd.concat([pd.read_csv(f) for f in files], ignore_index=True) if files else pd.DataFrame()

        def upload_to_bucket(local_path, dest_name):
            blob = bucket.blob(dest_name)
            blob.upload_from_filename(local_path)

        st.info("🔀 Merging results...")
        download_and_extract_zip_files(f"users/{run_id}/results/")
        success_df = merge_csvs("result_")
        failure_df = merge_csvs("failures_")

        if not success_df.empty:
            success_path = "/tmp/ALL_SUCCESS.csv"
            success_df.to_csv(success_path, index=False)
            upload_to_bucket(success_path, f"users/{run_id}/results/ALL_SUCCESS.csv")

        if not failure_df.empty:
            failure_path = "/tmp/ALL_FAILURES.csv"
            failure_df.to_csv(failure_path, index=False)
            upload_to_bucket(failure_path, f"users/{run_id}/results/ALL_FAILURES.csv")

        st.success("🎉 Merge completed. You can now download your results:")
        for fname in ["ALL_SUCCESS.csv", "ALL_FAILURES.csv"]:
            blob = bucket.blob(f"users/{run_id}/results/{fname}")
            local_path = f"/tmp/{fname}"
            if blob.exists():
                blob.download_to_filename(local_path)
                with open(local_path, "rb") as f:
                    st.download_button(f"⬇️ Download {fname}", f, file_name=fname)

st.markdown("---")
st.caption("Powered by eCore Services.")
