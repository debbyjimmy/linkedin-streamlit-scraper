import streamlit as st
import pandas as pd
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import zipfile
import os

API_KEY = st.secrets["API_KEY"]
API_URL = "https://api.scrapin.io/enrichment/profile"
MAX_WORKERS = 10
BATCH_SIZE = 50
SLEEP_BETWEEN_BATCHES = 30  # seconds

def flatten_json(y, prefix='', out=None):
    if out is None:
        out = {}
    for k, v in y.items():
        new_key = f'{prefix}.{k}' if prefix else k
        if isinstance(v, dict):
            flatten_json(v, new_key, out)
        elif isinstance(v, list):
            for i, item in enumerate(v):
                if isinstance(item, dict):
                    flatten_json(item, f'{new_key}[{i}]', out)
                else:
                    out[f'{new_key}[{i}]'] = item
        else:
            out[new_key] = v
    return out

def should_keep_field(field):
    return (
        field.startswith("person.linkedin") or
        field.startswith("person.firstName") or
        field.startswith("person.lastName") or
        field.startswith("person.headline") or
        field.startswith("person.location") or
        field.startswith("person.summary") or
        field.startswith("person.positions") or
        field.startswith("company")
    )

def scrape_profile(url):
    try:
        response = requests.get(API_URL, params={"apikey": API_KEY, "linkedInUrl": url})
        result = response.json()
        if not result.get("success", False):
            return {"sourceUrl": url, "status": f"🚫 API Error: {result.get('message', 'Unknown error')}"}
        flat = flatten_json(result)
        filtered = {k: v for k, v in flat.items() if should_keep_field(k)}
        filtered["sourceUrl"] = url
        filtered["status"] = "✅ Success"
        return filtered
    except Exception as e:
        return {"sourceUrl": url, "status": f"🛑 Error: {str(e)}"}

def batch_scrape(linkedin_urls):
    all_results = []
    batch_files = []
    num_batches = (len(linkedin_urls) + BATCH_SIZE - 1) // BATCH_SIZE

    for i in range(num_batches):
        st.info(f"🚀 Processing batch {i + 1} of {num_batches}...")
        batch = linkedin_urls[i * BATCH_SIZE : (i + 1) * BATCH_SIZE]
        batch_results = []
        progress_bar = st.progress(0)
        status_text = st.empty()

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_url = {executor.submit(scrape_profile, url): url for url in batch}
            total = len(future_to_url)
            completed = 0

            for future in as_completed(future_to_url):
                batch_results.append(future.result())
                completed += 1
                progress = int((completed / total) * 100)
                progress_bar.progress(progress)
                status_text.text(f"Processed {completed} of {total} in batch {i + 1}...")

        batch_df = pd.DataFrame(batch_results)
        batch_filename = f"batch_{i+1}.csv"
        batch_df.to_csv(batch_filename, index=False)
        batch_files.append(batch_filename)
        all_results.extend(batch_results)
        st.success(f"✅ Batch {i + 1} saved as {batch_filename}")

        if i < num_batches - 1:
            st.info(f"⏳ Sleeping {SLEEP_BETWEEN_BATCHES}s before next batch...")
            time.sleep(SLEEP_BETWEEN_BATCHES)

    # Save full merged CSV
    merged_filename = "linkedin_scraped_all.csv"
    pd.DataFrame(all_results).to_csv(merged_filename, index=False)
    batch_files.append(merged_filename)

    # Create a ZIP archive
    zip_filename = "linkedin_result.zip"
    with zipfile.ZipFile(zip_filename, "w") as zipf:
        for file in batch_files:
            zipf.write(file)
            os.remove(file)  # Clean up after zipping

    return zip_filename, pd.DataFrame(all_results)

st.title("🔍 LinkedIn Profile Scraper (Batch)")
st.markdown("Upload a CSV file with LinkedIn profile URLs in the first column. Then click **Start Scraping**.")

uploaded_file = st.file_uploader("📁 Choose a CSV file", type=["csv"])

if uploaded_file:
    df = pd.read_csv(uploaded_file)
    if df.empty or df.columns[0] == "":
        st.error("⚠️ CSV must have LinkedIn URLs in the first column.")
    else:
        linkedin_urls = df.iloc[:, 0].dropna().tolist()
        st.success(f"✅ Uploaded {len(linkedin_urls)} LinkedIn URLs.")

        if st.button("▶️ Start Scraping"):
            with st.spinner("⏳ Scraping in batches..."):
                zip_path, output_df = batch_scrape(linkedin_urls)

            st.success("🎉 All batches completed and zipped!")

            # Single ZIP download
            with open(zip_path, "rb") as f:
                st.download_button(
                    label="⬇️ Download Result (ZIP)",
                    data=f.read(),
                    file_name=zip_path,
                    mime="application/zip"
                )

            st.subheader("📄 Preview (First 20 Rows)")
            st.dataframe(output_df.head(20))
