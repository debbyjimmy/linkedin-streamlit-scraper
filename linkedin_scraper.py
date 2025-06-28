import argparse
import pandas as pd
import requests
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

API_URL = "https://api.scrapin.io/enrichment/profile"
MAX_WORKERS = 10
MAX_RETRIES = 3
INITIAL_BACKOFF = 1

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

def scrape_profile(url, apikey, retries=MAX_RETRIES, backoff=INITIAL_BACKOFF):
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(API_URL, params={"apikey": apikey, "linkedInUrl": url})
            response.raise_for_status()

            result = response.json()
            if not result.get("success", False):
                error_msg = result.get("message") or result.get("error") or "Unknown API error"
                raise ValueError(f"API Error: {error_msg}")

            flat = flatten_json(result)
            filtered = {k: v for k, v in flat.items() if should_keep_field(k)}
            filtered["sourceUrl"] = url
            filtered["status"] = "Success"
            return filtered

        except (requests.exceptions.HTTPError, ValueError) as e:
            # Only retry for 5xx errors
            if isinstance(e, requests.exceptions.HTTPError):
                status = response.status_code
            else:
                status = 500

            if attempt == retries or not (500 <= status < 600):
                return {"sourceUrl": url, "status": f"{type(e).__name__}: {str(e)}"}
            else:
                time.sleep(backoff)
                backoff *= 2  # exponential backoff

        except Exception as e:
            return {"sourceUrl": url, "status": f"Error: {str(e)}"}

def batch_scrape(input_file, output_file, shutdown=False, batch_index=None):
    config = load_config()
    apikey = config["API_KEY"]

    df = pd.read_csv(input_file)
    urls = df.iloc[:, 0].dropna().tolist()

    batch_label = f"[Batch {batch_index}]" if batch_index else "[Batch]"
    print(f"{batch_label} Starting batch of {len(urls)} records")

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(scrape_profile, url, apikey) for url in urls]
        for i, future in enumerate(as_completed(futures), 1):
            result = future.result()
            results.append(result)
            print(f"[{i}/{len(urls)}] {result['sourceUrl']} → {result['status']}")

    # Save all results
    pd.DataFrame(results).to_csv(output_file, index=False)
    print(f"\n✅ Done. Output saved to {output_file}")

    # Save failed ones
    failures = [r for r in results if r["status"] != "Success"]
    if failures:
        failure_file = output_file.replace("result_", "failures_")
        pd.DataFrame(failures).to_csv(failure_file, index=False)
        print(f"⚠️  Saved {len(failures)} failures to {failure_file}")

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
