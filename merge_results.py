import pandas as pd
import os
from google.cloud import storage

BUCKET_NAME = "contact-scraper-bucket"
RESULTS_PREFIX = "results/"
MERGED_SUCCESS = "results/ALL_SUCCESS.csv"
MERGED_FAILURES = "results/ALL_FAILURES.csv"

def download_csv_files(blob_prefix):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs(prefix=blob_prefix)
    
    result_files = []
    for blob in blobs:
        if blob.name.endswith(".csv") and ("result_" in blob.name or "failures_" in blob.name):
            local_path = f"/tmp/{os.path.basename(blob.name)}"
            blob.download_to_filename(local_path)
            result_files.append(local_path)
    return result_files

def merge_csvs(files, pattern):
    dfs = []
    for file in files:
        if pattern in os.path.basename(file):
            df = pd.read_csv(file)
            dfs.append(df)
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    return pd.DataFrame()

def upload_file_to_bucket(local_path, destination_blob):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(destination_blob)
    blob.upload_from_filename(local_path)
    print(f"✅ Uploaded: {destination_blob}")

def main():
    print("📥 Downloading result and failure CSVs from bucket...")
    files = download_csv_files(RESULTS_PREFIX)

    success_df = merge_csvs(files, "result_")
    failure_df = merge_csvs(files, "failures_")

    if not success_df.empty:
        merged_success_path = "/tmp/ALL_SUCCESS.csv"
        success_df.to_csv(merged_success_path, index=False)
        upload_file_to_bucket(merged_success_path, MERGED_SUCCESS)

    if not failure_df.empty:
        merged_failures_path = "/tmp/ALL_FAILURES.csv"
        failure_df.to_csv(merged_failures_path, index=False)
        upload_file_to_bucket(merged_failures_path, MERGED_FAILURES)

if __name__ == "__main__":
    main()
