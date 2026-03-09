import pandas as pd
import numpy as np

MERGED_FILE = "/home/iankenny/Desktop/LargeData/MergedData/merged_interpolated.csv"
LABELS_FILE = "/home/iankenny/FYP/NetData/run01/feb17normalrun_labels_mastertime.csv"
OUTPUT_FILE = "/home/iankenny/Desktop/LargeData/MergedData/merged_labeled.csv"

CHUNK_SIZE = 10_000_000
DT_FORMAT = "%Y-%m-%d %H:%M:%S.%f"


def parse_dt(date_series, time_series):
    dt_str = date_series.astype(str).str.strip() + " " + time_series.astype(str).str.strip()
    return pd.to_datetime(dt_str, format=DT_FORMAT, errors="coerce")


# ----------------------------
# 1. Read labels file
# ----------------------------
labels_df = pd.read_csv(LABELS_FILE, dtype=str, low_memory=False)
labels_df.columns = labels_df.columns.str.strip().str.lower()

required_cols = {"date", "time"}
missing = required_cols - set(labels_df.columns)
if missing:
    raise ValueError(f"Labels file missing required columns: {missing}")

labels_df["datetime"] = parse_dt(labels_df["date"], labels_df["time"])

if labels_df["datetime"].isna().any():
    bad = labels_df[labels_df["datetime"].isna()]
    raise ValueError(
        "Could not parse some label timestamps. Example bad rows:\n"
        f"{bad[['date','time']].head(10)}"
    )

labels_df = labels_df.reset_index(drop=True)

if len(labels_df) < 6:
    raise ValueError(
        "Labels file is too short. Expected at least: "
        "program start, camera start, camera stop, upload start, upload stop, program stop."
    )

program_start = labels_df.iloc[0]["datetime"]
program_stop = labels_df.iloc[-1]["datetime"]

body = labels_df.iloc[1:-1].reset_index(drop=True)

if len(body) % 2 != 0:
    raise ValueError(
        f"Rows between program start and stop must come in pairs, found {len(body)} rows."
    )

# ----------------------------
# 2. Build intervals
# ----------------------------
intervals = []

pair_index = 0
for i in range(0, len(body), 2):
    start_dt = body.loc[i, "datetime"]
    stop_dt = body.loc[i + 1, "datetime"]

    if stop_dt < start_dt:
        raise ValueError(
            f"Interval stop before start at body rows {i} and {i+1}: {start_dt} -> {stop_dt}"
        )

    label_name = "camera operation" if pair_index % 2 == 0 else "uploading"
    intervals.append((np.datetime64(start_dt), np.datetime64(stop_dt), label_name))
    pair_index += 1

print(f"Program window: {program_start} -> {program_stop}")
print(f"Built {len(intervals)} intervals")
print("First 6 intervals:")
for x in intervals[:6]:
    print(x)

# ----------------------------
# 3. Process merged CSV
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

    chunk["datetime"] = parse_dt(chunk["date"], chunk["time"])

    if chunk["datetime"].isna().any():
        bad = chunk[chunk["datetime"].isna()]
        raise ValueError(
            f"Could not parse some merged timestamps in chunk {chunk_idx}. Example bad rows:\n"
            f"{bad[['date','time']].head(10)}"
        )

    # Trim to program window
    chunk = chunk[
        (chunk["datetime"] >= program_start) &
        (chunk["datetime"] <= program_stop)
    ].copy()

    if len(chunk) == 0:
        continue

    dt_values = chunk["datetime"].to_numpy(dtype="datetime64[ns]")
    labels = np.full(len(chunk), "idle", dtype=object)

    # Label by interval containment
    for start_dt, stop_dt, label_name in intervals:
        mask = (dt_values >= start_dt) & (dt_values <= stop_dt)
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
