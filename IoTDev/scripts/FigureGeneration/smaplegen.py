import pandas as pd
import numpy as np

INPUT_FILE = "/home/iankenny/Desktop/LargeData/MergedData/merged_labeled.csv"
OUTPUT_FILE = "/home/iankenny/Desktop/LargeData/MergedData/most_active_hour.csv"

CHUNK_SIZE = 10_000_000
RESAMPLE_BIN = "1s"
WINDOW_SECONDS = 3600

# Columns expected in merged labeled CSV
DATE_COL = "date"
TIME_COL = "time"
SOURCE_COL = "source"
DEST_COL = "destination"
PROTOCOL_COL = "protocol"
LENGTH_COL = "length"
INFO_COL = "info"


def normalise_columns(df):
    df.columns = df.columns.str.strip().str.lower()
    return df


def find_best_hour(input_file):
    """
    First pass:
    Build per-second packet counts across the whole file, then find the
    1-hour continuous window with the highest total packet activity.
    """
    sec_counts = {}

    for chunk in pd.read_csv(input_file, chunksize=CHUNK_SIZE, low_memory=False):
        chunk = normalise_columns(chunk)

        # Build datetime
        chunk["datetime"] = pd.to_datetime(
            chunk[DATE_COL].astype(str).str.strip() + " " + chunk[TIME_COL].astype(str).str.strip(),
            errors="coerce"
        )
        chunk = chunk.dropna(subset=["datetime"])

        # Packet row = any network field present
        packet_mask = chunk[[SOURCE_COL, DEST_COL, PROTOCOL_COL, LENGTH_COL, INFO_COL]].notna().any(axis=1)
        packet_rows = chunk.loc[packet_mask, ["datetime"]].copy()

        if packet_rows.empty:
            continue

        packet_rows["sec"] = packet_rows["datetime"].dt.floor(RESAMPLE_BIN)

        counts = packet_rows.groupby("sec").size()

        for ts, val in counts.items():
            sec_counts[ts] = sec_counts.get(ts, 0) + int(val)

    if not sec_counts:
        raise ValueError("No packet activity found in the file.")

    sec_series = pd.Series(sec_counts).sort_index()

    # Fill missing seconds so rolling window is continuous
    full_index = pd.date_range(sec_series.index.min(), sec_series.index.max(), freq=RESAMPLE_BIN)
    sec_series = sec_series.reindex(full_index, fill_value=0)

    rolling_activity = sec_series.rolling(f"{WINDOW_SECONDS}s").sum()

    best_end = rolling_activity.idxmax()
    best_start = best_end - pd.Timedelta(seconds=WINDOW_SECONDS) + pd.Timedelta(seconds=1)

    return best_start, best_end, rolling_activity.max()


def extract_window(input_file, output_file, start_time, end_time):
    """
    Second pass:
    Extract all rows whose timestamps fall within the chosen 1-hour window.
    """
    first_write = True
    total_rows = 0

    for chunk in pd.read_csv(input_file, chunksize=CHUNK_SIZE, low_memory=False):
        chunk = normalise_columns(chunk)

        chunk["datetime"] = pd.to_datetime(
            chunk[DATE_COL].astype(str).str.strip() + " " + chunk[TIME_COL].astype(str).str.strip(),
            errors="coerce"
        )
        chunk = chunk.dropna(subset=["datetime"])

        subset = chunk[(chunk["datetime"] >= start_time) & (chunk["datetime"] <= end_time)].copy()

        if subset.empty:
            continue

        subset = subset.drop(columns=["datetime"])

        subset.to_csv(output_file, mode="w" if first_write else "a", index=False, header=first_write)
        total_rows += len(subset)
        first_write = False

    return total_rows


if __name__ == "__main__":
    best_start, best_end, best_score = find_best_hour(INPUT_FILE)

    print(f"Most active hour found:")
    print(f"  Start: {best_start}")
    print(f"  End:   {best_end}")
    print(f"  Total packets in window: {int(best_score)}")

    rows_written = extract_window(INPUT_FILE, OUTPUT_FILE, best_start, best_end)

    print(f"\nSaved extracted window to: {OUTPUT_FILE}")
    print(f"Rows written: {rows_written}")
