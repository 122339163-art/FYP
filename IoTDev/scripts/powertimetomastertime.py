import pandas as pd
from datetime import datetime

# ---- CONFIG ----
input_file = "/media/iankenny/7BDD-A1FD/2026_02_17_13_32_30.csv"
output_file = "/home/iankenny/FYP/PowerData/2026_02_17_13_32_30_mastertime.csv"
master_start_str = "2026-02-17 13:32:30"  # full start datetime
chunk_size = 10_000_000  # adjust based on available RAM
# ----------------

# Parse master start datetime
master_start = datetime.strptime(master_start_str, "%Y-%m-%d %H:%M:%S")

# Create CSV reader with chunks and skip garbage rows
reader = pd.read_csv(
    input_file,
    skiprows=3,
    chunksize=chunk_size
)

first_chunk = True

for chunk in reader:

    # Rename column 2 (index 2) to "current average"
    cols = list(chunk.columns)
    cols[1] = "current average"
    chunk.columns = cols

    # Convert relative seconds (column 1) to absolute datetime
    absolute_dt = master_start + pd.to_timedelta(chunk.iloc[:, 1], unit="s")

    # Insert 'date' column immediately left of time column (index 1)
    chunk.insert(0, "date", absolute_dt.dt.strftime("%Y-%m-%d"))

    # Replace time column (now at index 1) with master time
    chunk.iloc[:, 1] = absolute_dt.dt.strftime("%H:%M:%S.%f")

    # Write chunk to output (append after first)
    chunk.to_csv(
        output_file,
        mode="w" if first_chunk else "a",
        header=first_chunk,
        index=False
    )

    first_chunk = False

print("Large CSV processing complete.")
