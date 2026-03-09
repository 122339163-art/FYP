import pandas as pd

INPUT_FILE = "/home/iankenny/Desktop/LargeData/MergedData/merged_labeled.csv"
OUTPUT_FILE = "/home/iankenny/Desktop/LargeData/MergedData/most_active_hour.csv"

CHUNK_SIZE = 10_000_000

START_TIME = pd.Timestamp("2026-02-18 01:30:56")
END_TIME = pd.Timestamp("2026-02-18 02:30:55")


def normalise_columns(df):
    df.columns = df.columns.str.strip().str.lower()
    return df


def extract_window(input_file, output_file, start_time, end_time):
    first_write = True
    total_rows = 0

    for chunk in pd.read_csv(input_file, chunksize=CHUNK_SIZE, low_memory=False):
        chunk = normalise_columns(chunk)

        chunk["datetime"] = pd.to_datetime(
            chunk["date"].astype(str).str.strip() + " " + chunk["time"].astype(str).str.strip(),
            errors="coerce"
        )

        chunk = chunk.dropna(subset=["datetime"])

        subset = chunk[
            (chunk["datetime"] >= start_time) &
            (chunk["datetime"] <= end_time)
        ].copy()

        if subset.empty:
            continue

        subset = subset.drop(columns=["datetime"])

        subset.to_csv(
            output_file,
            mode="w" if first_write else "a",
            index=False,
            header=first_write
        )

        total_rows += len(subset)
        first_write = False

    return total_rows


if __name__ == "__main__":
    rows_written = extract_window(INPUT_FILE, OUTPUT_FILE, START_TIME, END_TIME)

    print(f"Extracted rows from {START_TIME} to {END_TIME}")
    print(f"Saved to: {OUTPUT_FILE}")
    print(f"Rows written: {rows_written}")
