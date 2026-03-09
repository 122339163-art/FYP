import pandas as pd
import numpy as np

MERGED_FILE = "/home/iankenny/Desktop/LargeData/MergedData/merged_interpolated.csv"
LABELS_FILE = "/home/iankenny/FYP/NetData/run01/feb17normalrun_labels_mastertime.csv"
OUTPUT_FILE = "/home/iankenny/Desktop/LargeData/MergedData/merged_labeled.csv"

CHUNK_SIZE = 10_000_000
DAYFIRST = False   # set False if your dates are YYYY-MM-DD


def parse_datetimes(date_series, time_series, dayfirst=False):
    return pd.to_datetime(
        date_series.astype(str).str.strip() + " " + time_series.astype(str).str.strip(),
        errors="coerce",
        dayfirst=dayfirst
    )


# ----------------------------
# 1. Read labels file
# ----------------------------
labels_df = pd.read_csv(LABELS_FILE, dtype=str, low_memory=False)
labels_df.columns = labels_df.columns.str.strip().str.lower()

required_cols = {"date", "time"}
missing = required_cols - set(labels_df.columns)
if missing:
    raise ValueError(f"Labels file missing required columns: {missing}")

labels_df["datetime"] = parse_datetimes(
    labels_df["date"],
    labels_df["time"],
    dayfirst=DAYFIRST
)

if labels_df["datetime"].isna().any():
    bad = labels_df[labels_df["datetime"].isna()]
    raise ValueError(f"Could not parse some label timestamps:\n{bad.head(10)}")

labels_df = labels_df.sort_values("datetime").reset_index(drop=True)

if len(labels_df) < 4:
    raise ValueError("Labels file is too short. Need at least program start, one interval pair, and program end.")

program_start = labels_df.iloc[0]["datetime"]
program_end = labels_df.iloc[-1]["datetime"]

print("Program window:", program_start, "->", program_end)

# ----------------------------
# 2. Build only camera/upload intervals
# ----------------------------
body = labels_df.iloc[1:-1].reset_index(drop=True)

if len(body) % 2 != 0:
    raise ValueError(
        f"Rows between first and last must come in start/stop pairs, found {len(body)} rows."
    )

intervals = []
pair_num = 0

for i in range(0, len(body), 2):
    start_dt = body.loc[i, "datetime"]
    end_dt = body.loc[i + 1, "datetime"]

    if end_dt < start_dt:
        raise ValueError(
            f"Invalid interval: end before start at body rows {i} and {i+1}: {start_dt} -> {end_dt}"
        )

    label_name = "camera operation" if pair_num % 2 == 0 else "uploading"
    intervals.append((start_dt, end_dt, label_name))
    pair_num += 1

print(f"Built {len(intervals)} intervals")
print("First few intervals:")
for interval in intervals[:6]:
    print(interval)

# ----------------------------
# 3. Process merged CSV in chunks
# ----------------------------
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
total_rows_written = 0

for chunk_idx, chunk in enumerate(
    pd.read_csv(MERGED_FILE, chunksize=CHUNK_SIZE, dtype=dtypes, low_memory=False),
    start=1
):
    chunk.columns = chunk.columns.str.strip().str.lower()

    chunk["datetime"] = parse_datetimes(
        chunk["date"],
        chunk["time"],
        dayfirst=DAYFIRST
    )

    if chunk["datetime"].isna().any():
        bad = chunk[chunk["datetime"].isna()]
        raise ValueError(
            f"Could not parse some merged timestamps in chunk {chunk_idx}:\n{bad.head(10)}"
        )

    # Keep only rows between program start and program end
    chunk = chunk[
        (chunk["datetime"] >= program_start) &
        (chunk["datetime"] <= program_end)
    ].copy()

    if len(chunk) == 0:
        continue

    dt_values = chunk["datetime"].to_numpy(dtype="datetime64[ns]")

    # Default everything to idle
    labels = np.full(len(chunk), "idle", dtype=object)

    # Apply camera/upload intervals
    for start_dt, end_dt, label_name in intervals:
        mask = (
            (dt_values >= np.datetime64(start_dt)) &
            (dt_values <= np.datetime64(end_dt))
        )
        labels[mask] = label_name

    chunk["label"] = labels
    chunk = chunk.drop(columns=["datetime"])

    if first_write:
        chunk.to_csv(OUTPUT_FILE, index=False)
        first_write = False
    else:
        chunk.to_csv(OUTPUT_FILE, mode="a", header=False, index=False)

    total_rows_written += len(chunk)
    print(f"Processed chunk {chunk_idx} | total rows written: {total_rows_written:,}")

print("Finished.")
print("Saved to:", OUTPUT_FILE)
