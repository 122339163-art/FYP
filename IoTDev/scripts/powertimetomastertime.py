import pandas as pd
from datetime import datetime

# ---- CONFIG ----
input_file = "/media/iankenny/7BDD-A1FD/2026_02_17_13_32_30.csv"
output_file = "/home/iankenny/FYP/PowerData/2026_02_17_13_32_30_mastertime.csv"
master_start_str = "2026-02-17 13:32:30"  # full start datetime
chunk_size = 5000000  # adjust based on available RAM
# ----------------

# Parse master start datetime
master_start = datetime.strptime(master_start_str, "%Y-%m-%d %H:%M:%S")

# Create CSV reader with chunks
# Skip first 4 rows (3 garbage + header)
reader = pd.read_csv(
    input_file,
    skiprows=4,
    header=None,
    names=["seconds", "current average"],
    dtype={"seconds": "float64", "current average": "float64"},
    chunksize=chunk_size,
    low_memory=False
)

first_chunk = True

for chunk in reader:

    # Ensure numeric seconds, drop any bad rows (should be clean after skiprows)
    chunk["seconds"] = pd.to_numeric(chunk["seconds"], errors="coerce")
    chunk = chunk.dropna(subset=["seconds"])

    # Compute absolute datetime
    absolute_dt = master_start + pd.to_timedelta(chunk["seconds"], unit="s")

    # Build final output DataFrame (exactly 3 columns)
    output_chunk = pd.DataFrame({
        "date": absolute_dt.dt.strftime("%Y-%m-%d"),
        "time": absolute_dt.dt.strftime("%H:%M:%S.%f"), 
        "current average": chunk["current average"]
    })

    # Write chunk to CSV
    output_chunk.to_csv(
        output_file,
        mode="w" if first_chunk else "a",
        header=first_chunk,
        index=False
    )

    first_chunk = False

print("Large CSV processing complete.")
