import pandas as pd

NETWORK_FILE = "/home/iankenny/FYP/NetData/run01/feb17normalrun_datasetdata_mastertime.csv"
POWER_FILE   = "/home/iankenny/Desktop/LargeData/PowerData/2026_02_17_13_32_30_mastertime.csv"
MERGED_FILE  = "/home/iankenny/Desktop/LargeData/MergedData/merged_interpolated.csv"

CHUNK_SIZE = 20000000

merged_dtypes = {
    "date": "string",
    "time": "string",
    "current": "float64",
    "source": "string",
    "destination": "string",
    "protocol": "string",
    "length": "float64",
    "info": "string"
}

print("\n--- VERIFYING MERGE ---\n")

# --------------------------------------------------
# Count POWER rows
# --------------------------------------------------

power_rows = 0
for chunk in pd.read_csv(
    POWER_FILE,
    chunksize=CHUNK_SIZE,
    dtype="string",
    low_memory=False
):
    # normalize column names
    chunk.columns = chunk.columns.str.strip().str.lower()
    power_rows += len(chunk)

print("Power CSV rows:", power_rows)


# --------------------------------------------------
# Count NETWORK TCP rows
# --------------------------------------------------

network_tcp_rows = 0
for chunk in pd.read_csv(
    NETWORK_FILE,
    chunksize=CHUNK_SIZE,
    dtype="string",
    low_memory=False
):
    chunk.columns = chunk.columns.str.strip().str.lower()
    network_tcp_rows += (chunk["protocol"].str.upper() == "TCP").sum()

print("Network TCP rows:", network_tcp_rows)


# --------------------------------------------------
# Scan MERGED file
# --------------------------------------------------

merged_rows = 0
merged_tcp_rows = 0
tcp_missing_metadata = 0

first_tcp_rows = []
last_tcp_rows = []

last_timestamp = None
out_of_order = 0

for chunk in pd.read_csv(
    MERGED_FILE,
    chunksize=CHUNK_SIZE,
    dtype=merged_dtypes,
    low_memory=False
):
    chunk.columns = chunk.columns.str.strip().str.lower()

    merged_rows += len(chunk)

    # timestamp as string for order check
    timestamps = chunk["date"] + " " + chunk["time"]
    if last_timestamp is not None:
        if timestamps.iloc[0] < last_timestamp:
            out_of_order += 1
    last_timestamp = timestamps.iloc[-1]

    tcp_chunk = chunk[chunk["protocol"].str.upper() == "TCP"]
    merged_tcp_rows += len(tcp_chunk)

    # Check TCP metadata integrity
    missing = (
        tcp_chunk["source"].isna() |
        tcp_chunk["destination"].isna() |
        tcp_chunk["length"].isna() |
        tcp_chunk["info"].isna()
    ).sum()
    tcp_missing_metadata += missing

    # capture first 10 TCP rows
    if len(first_tcp_rows) < 10:
        needed = 10 - len(first_tcp_rows)
        first_tcp_rows.extend(tcp_chunk.head(needed).to_dict("records"))

    # capture last 10 TCP rows
    if len(tcp_chunk) > 0:
        last_tcp_rows.extend(tcp_chunk.tail(10).to_dict("records"))
        last_tcp_rows = last_tcp_rows[-10:]


# --------------------------------------------------
# Results
# --------------------------------------------------

print("Merged CSV rows:", merged_rows)
if merged_rows >= power_rows:
    print("✅ Merged rows >= power rows")
else:
    print("❌ ERROR: merged rows < power rows")

print("Merged TCP rows:", merged_tcp_rows)
if merged_tcp_rows == network_tcp_rows:
    print("✅ All TCP packets preserved")
else:
    print("❌ TCP mismatch:", network_tcp_rows - merged_tcp_rows, "missing")

if tcp_missing_metadata == 0:
    print("✅ All TCP rows contain valid network metadata")
else:
    print("❌ TCP rows missing metadata:", tcp_missing_metadata)

# Print TCP samples
print("\n===== FIRST 10 TCP ROWS =====")
for r in first_tcp_rows:
    print(r)

print("\n===== LAST 10 TCP ROWS =====")
for r in last_tcp_rows:
    print(r)

# Timestamp ordering
if out_of_order == 0:
    print("\n✅ Timestamps appear sorted")
else:
    print("\n⚠️ Possible ordering issues:", out_of_order)

print("\n--- VERIFICATION COMPLETE ---\n")
