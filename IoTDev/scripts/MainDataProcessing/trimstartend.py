import pandas as pd

MERGED_FILE = "/home/iankenny/Desktop/LargeData/MergedData/merged_interpolated.csv"
TRIMMED_FILE = "/home/iankenny/Desktop/LargeData/MergedData/merged_trimmed.csv"

CHUNK_SIZE = 20000000

START_TIME = "2026-02-17 13:32:33.000000"
END_TIME   = "2026-02-18 13:32:37.568726"

# define dtypes for safety
dtypes = {
    "date": "string",
    "time": "string",
    "current": "float64",
    "source": "string",
    "destination": "string",
    "protocol": "string",
    "length": "float64",
    "info": "string"
}

first_write = True

for chunk in pd.read_csv(MERGED_FILE, chunksize=CHUNK_SIZE, dtype=dtypes, low_memory=False):
    chunk.columns = chunk.columns.str.strip().str.lower()

    # create combined timestamp string
    timestamp_str = chunk["date"] + " " + chunk["time"]

    # filter rows within range
    mask = (timestamp_str >= START_TIME) & (timestamp_str <= END_TIME)
    filtered = chunk[mask]

    if not filtered.empty:
        if first_write:
            filtered.to_csv(TRIMMED_FILE, index=False)
            first_write = False
        else:
            filtered.to_csv(TRIMMED_FILE, index=False, mode="a", header=False)

print("Trimming complete. Saved to:", TRIMMED_FILE)
