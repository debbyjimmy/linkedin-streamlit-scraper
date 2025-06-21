import streamlit as st
import pandas as pd
import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

API_KEY = st.secrets["API_KEY"]
API_URL = "https://api.scrapin.io/enrichment/profile"
MAX_WORKERS = 10

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
            results = []
            progress_bar = st.progress(0)
            status_text = st.empty()

            with st.spinner("⏳ Scraping profiles using parallel threads..."):
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    future_to_url = {executor.submit(scrape_profile, url): url for url in linkedin_urls}
                    total = len(future_to_url)
                    completed = 0

                    for future in as_completed(future_to_url):
                        results.append(future.result())
                        completed += 1
                        progress = int((completed / total) * 100)
                        progress_bar.progress(progress)
                        status_text.text(f"Processed {completed} of {total} profiles...")

            output_df = pd.DataFrame(results)
            st.success("✅ Scraping completed!")

            st.download_button(
                label="⬇️ Download Results as CSV",
                data=output_df.to_csv(index=False),
                file_name="linkedin_scraped.csv",
                mime="text/csv"
            )

            st.subheader("📄 Preview")
            st.dataframe(output_df.head(20))
