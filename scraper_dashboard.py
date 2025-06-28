import streamlit as st
import pandas as pd
import subprocess
import os
import json
from google.cloud import storage
from google.oauth2 import service_account

st.set_page_config(page_title="Contact Scraper Dashboard")
st.title("📇 Contact Scraper Dashboard")

# Initialize GCS client
# If GCP_CREDENTIALS is provided via Streamlit secrets, use it; otherwise use Application Default Credentials
if st.secrets.get("GCP_CREDENTIALS"):
    credentials_info = json.loads(st.secrets["GCP_CREDENTIALS"])
    credentials = service_account.Credentials.from_service_account_info(credentials_info)
    client = storage.Client(credentials=credentials, project=credentials_info.get("project_id"))
else:
    client = storage.Client()

# Bucket name stored in secrets
bucket_name = st.secrets["BUCKET_NAME"]
bucket = client.bucket(bucket_name)

# Upload input CSV
uploaded_file = st.file_uploader("Upload full LinkedIn CSV to split and scrape", type=["csv"])
num_chunks = st.number_input("Number of chunks", min_value=1, max_value=100, value=20)

if uploaded_file:
    input_df = pd.read_csv(uploaded_file)
    st.write(f"Dataframe loaded: {len(input_df)} rows")

    if st.button("Split and Upload Chunks"):
        st.info("Clearing existing chunks in the bucket...")
        existing_blobs = list(bucket.list_blobs(prefix="chunks/"))
        for blob in existing_blobs:
            blob.delete()
        st.write(f"Cleared {len(existing_blobs)} existing chunk files from bucket/chunks/")

        st.info("Splitting CSV and uploading new chunks...")
        chunk_size = -(-len(input_df) // num_chunks)
        os.makedirs("chunks", exist_ok=True)

        for i in range(num_chunks):
            start = i * chunk_size
            end = min((i+1)*chunk_size, len(input_df))
            chunk_df = input_df.iloc[start:end]
            if chunk_df.empty:
                continue
            filename = f"chunk_{i+1}.csv"
            chunk_df.to_csv(filename, index=False)
            blob = bucket.blob(f"chunks/{filename}")
            blob.upload_from_filename(filename)
            st.success(f"Uploaded chunk: {filename} ({len(chunk_df)} rows)")
        st.balloons()

    if st.button("Launch Scraper VMs"):
        st.info("Launching VMs...")
        for i in range(1, num_chunks+1):
            vm_name = f"scraper-vm-{i}"
            cmd = [
                "gcloud", "compute", "instances", "create", vm_name,
                "--zone=us-central1-a",
                "--source-instance-template=scraper-template-v4"
            ]
            subprocess.Popen(cmd)
            st.write(f"Launching {vm_name}...")
        st.success("All VMs launched.")

# Monitor results
st.header("Results")
if st.button("Merge Results Now"):
    subprocess.run(["python3", "merge_results.py"], check=True)
    st.success("Merged results into ALL_SUCCESS.csv and ALL_FAILURES.csv")

st.markdown("---")
st.write("Powered by DaG Inc.")
