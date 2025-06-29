# main.py
from google.cloud import storage
from googleapiclient import discovery
import os

def launch_scraper_vms(event, context):
    bucket_name = event['bucket']
    file_name = event['name']

    if not file_name.startswith("chunks/") or not file_name.endswith(".csv"):
        print(f"Ignoring unrelated file: {file_name}")
        return

    # Set configs
    project = os.environ.get("GCP_PROJECT_ID")
    zone = os.environ.get("GCP_COMPUTE_ZONE", "us-central1-a")
    template = os.environ.get("INSTANCE_TEMPLATE_NAME")

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Get all chunks
    blobs = list(storage_client.list_blobs(bucket_name, prefix="chunks/"))
    chunk_files = sorted([b.name for b in blobs if b.name.endswith(".csv")])

    compute = discovery.build("compute", "v1")

    for i, chunk_file in enumerate(chunk_files):
        vm_name = f"scraper-vm-{i+1}"

        print(f"Launching {vm_name} for chunk: {chunk_file}")
        request_body = {
            "name": vm_name,
            "sourceInstanceTemplate": f"global/instanceTemplates/{template}"
        }

        try:
            response = compute.instances().insert(
                project=project,
                zone=zone,
                body=request_body
            ).execute()
            print(f"VM {vm_name} launch triggered.")
        except Exception as e:
            print(f"Error launching {vm_name}: {e}")
