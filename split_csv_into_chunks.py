import pandas as pd
import os
import math
import argparse

def split_csv(input_file, num_chunks=20, output_folder="chunks"):
    os.makedirs(output_folder, exist_ok=True)

    df = pd.read_csv(input_file)
    total_records = len(df)

    if total_records == 0:
        print("⚠️ Input file is empty.")
        return []

    chunk_size = math.ceil(total_records / num_chunks)
    chunk_files = []

    print(f"📂 Splitting {total_records} records into {num_chunks} chunks...")

    for i in range(num_chunks):
        start_idx = i * chunk_size
        end_idx = min((i + 1) * chunk_size, total_records)
        chunk_df = df.iloc[start_idx:end_idx]

        if chunk_df.empty:
            continue  # Skip empty chunks

        filename = os.path.join(output_folder, f"chunk_{i+1}.csv")
        chunk_df.to_csv(filename, index=False)
        chunk_files.append(filename)
        print(f"✅ Chunk {i+1}: Saved {len(chunk_df)} records to {filename}")

    print(f"\n🎉 Done! {len(chunk_files)} chunk files created in '{output_folder}'.")

    return chunk_files

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Split a large CSV into N chunks")
    parser.add_argument("--input", required=True, help="Path to the input CSV file")
    parser.add_argument("--chunks", type=int, default=20, help="Number of chunks to split into (default: 20)")
    parser.add_argument("--output", default="chunks", help="Output folder (default: 'chunks')")

    args = parser.parse_args()

    split_csv(args.input, args.chunks, args.output)
