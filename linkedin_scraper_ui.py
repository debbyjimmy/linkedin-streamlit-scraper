import streamlit as st
import pandas as pd
import requests
import time

API_KEY = st.secrets["API_KEY"]
API_URL = "https://api.scrapin.io/enrichment/profile"
DELAY_SEC = 0.25

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

st.title("🔍 LinkedIn Profile Scraper (Batch)")
st.markdown("Upload a CSV file with LinkedIn profile URLs in the first column.")

uploaded_file = st.file_uploader("Choose a CSV file", type=["csv"])
if uploaded_file:
    df = pd.read_csv(uploaded_file)
    if df.empty or df.columns[0] == "":
        st.error("CSV must have LinkedIn URLs in the first column.")
    else:
        linkedin_urls = df.iloc[:, 0].dropna().tolist()
        results = []
        progress_bar = st.progress(0)
        status_text = st.empty()

        with st.spinner("Scraping profiles..."):
            for idx, url in enumerate(linkedin_urls):
                try:
                    response = requests.get(API_URL, params={"apikey": API_KEY, "linkedInUrl": url})
                    result = response.json()

                    # Check for API error or failure
                    if not result.get("success", False):
                        st.error(f"🚫 Scrapin API error: {result.get('message', 'Unknown error')}")
                        st.stop()

                    flat = flatten_json(result)
                    filtered = {k: v for k, v in flat.items() if should_keep_field(k)}
                    filtered["sourceUrl"] = url
                    filtered["status"] = "✅ Success"
                    results.append(filtered)

                except Exception as e:
                    results.append({"sourceUrl": url, "status": f"🛑 Error: {str(e)}"})
                time.sleep(DELAY_SEC)
                progress = int((idx + 1) / len(linkedin_urls) * 100)
                progress_bar.progress(progress)
                status_text.text(f"Processing {idx + 1} of {len(linkedin_urls)} profiles...")

        output_df = pd.DataFrame(results)
        st.success("✅ Scraping completed!")
        st.download_button("Download Results", data=output_df.to_csv(index=False),
                           file_name="linkedin_scraped.csv", mime="text/csv")
        st.dataframe(output_df.head(20))
