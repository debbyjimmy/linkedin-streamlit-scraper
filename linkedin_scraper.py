import argparse
import pandas as pd
import requests
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

API_URL = "https://api.scrapin.io/enrichment/profile"
MAX_WORKERS = 10

def load_config():
    with open("config.json", "r") as f:
        return json.load(f)

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

def scrape_profile(url, apikey):
    try:
        response = requests.get(API_URL, params={"apikey": apikey, "linkedInUrl": url})
        result = response.json()
        if not result.get("success", False):
            return {"sourceUrl": url, "status": f"API Error: {result.get('message', '')}"}
        flat = flatten_json(result)
        filtered = {k: v for k, v in flat.items() if should_keep_field(k)}
        filtered["sourceUrl"] = url
        filtered["status"] = "Success"
        return filtered
    except Exception as e:
        return {"sourceUrl": url, "status": f"Error: {str(e)}"}

def batch_scrape(input_file, output_file, shutdown=False, batch_index=None):
    config = load_config()
    apikey = config["API_KEY"]

    df = pd.read_csv(input_file)
    urls = df.iloc[:, 0].dropna().tolist()

    if batch_index:
        print(f"[Batch {batch_index}] Starting batch of {len(urls)} records")
    else:
        print(f"Starting batch of {len(urls)} records")

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(scrape_profile, url, apikey) for url in urls]
        for i, future in enumerate(as_completed(futures), 1):
            result = future.result()
            results.append(result)
            print(f"[{i}/{len(urls)}] {result['sourceUrl']} → {result['status']}")

    pd.DataFrame(results).to_csv(output_file, index=False)
    print(f"\n✅ Done. Output saved to {output_file}")

    if shutdown:
        print("Shutting down machine...")
        os.system("sudo shutdown -h now")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Path to input CSV")
    parser.add_argument("--output", required=True, help="Path to save output CSV")
    parser.add_argument("--shutdown", action="store_true", help="Shutdown machine after run")
    parser.add_argument("--batch-index", help="Optional index label for logging")

    args = parser.parse_args()
    batch_scrape(args.input, args.output, args.shutdown, args.batch_index)
