import pandas as pd

INPUT_FILE = "/home/iankenny/Desktop/LargeData/MergedData/merged_labeled.csv"
OUTPUT_FILE = "/home/iankenny/Desktop/LargeData/MergedData/merged_subset_figures.csv"

CHUNK_SIZE = 20000000

START_TIME = "2026-02-18 13:00:00"

dtypes = {
    "date": "string",
    "time": "string",
    "current": "float64",
    "source": "string",
    "destination": "string",
    "protocol": "string",
    "length": "float64",
    "info": "string",
    "label": "string"
}

first_write = True
start_found = False

for chunk in pd.read_csv(INPUT_FILE, chunksize=CHUNK_SIZE, dtype=dtypes, low_memory=False):

    chunk.columns = chunk.columns.str.strip().str.lower()
    timestamps = chunk["date"] + " " + chunk["time"]

    if not start_found:

        mask = timestamps >= START_TIME

        if mask.any():
            start_found = True
            filtered = chunk[mask]
        else:
            continue

    else:
        filtered = chunk

    if first_write:
        filtered.to_csv(OUTPUT_FILE, index=False)
        first_write = False
    else:
        filtered.to_csv(OUTPUT_FILE, mode="a", header=False, index=False)

print("Extraction complete. Saved to:", OUTPUT_FILE)
