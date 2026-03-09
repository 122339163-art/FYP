import pandas as pd

# CONFIG
NETWORK_FILE = "/home/iankenny/FYP/NetData/run01/feb17normalrun_datasetdata_mastertime.csv"
POWER_FILE   = "/home/iankenny/Desktop/LargeData/PowerData/2026_02_17_13_32_30_mastertime.csv"
MERGED_FILE  = "/home/iankenny/Desktop/LargeData/MergedData/Unlabelled_merged.csv"
EXPECTED_COLS = ['date', 'time', 'current', 'source', 'destination', 'protocol', 'length', 'info']
CHUNK_SIZE   = 10000000  # Adjust for your RAM

# Correct dtypes
dtypes = {
    'date': 'string',
    'time': 'string',
    'current': 'float64',
    'source': 'string',
    'destination': 'string',
    'protocol': 'string',
    'length': 'float64',
    'info': 'string'
}

# ---- 1. Check row count ----
power_rows = sum(1 for _ in open(POWER_FILE)) - 1
merged_rows = 0
for chunk in pd.read_csv(MERGED_FILE, chunksize=CHUNK_SIZE, dtype=dtypes):
    merged_rows += len(chunk)

print(f"Power CSV rows:  {power_rows}")
print(f"Merged CSV rows: {merged_rows}")
if merged_rows == power_rows:
    print("✅ Row count matches power CSV.")
else:
    print(f"⚠️ Row count mismatch!")

# ---- 2. Check column count ----
first_chunk = pd.read_csv(MERGED_FILE, nrows=1, dtype=dtypes)
if len(first_chunk.columns) == 8:
    print("✅ Column count matches expected 8 columns.")
else:
    print(f"⚠️ Column count mismatch: {len(first_chunk.columns)}")

# ---- 3. Check TCP counts ----
# Network TCP rows
network = pd.read_csv(NETWORK_FILE)
network.columns = network.columns.str.strip().str.lower()
network_tcp_count = (network['protocol'].str.upper().str.strip() == 'TCP').sum()

# Merged TCP rows (chunked)
merged_tcp_count = 0
for chunk in pd.read_csv(MERGED_FILE, chunksize=CHUNK_SIZE, dtype=dtypes):
    merged_tcp_count += (chunk['protocol'].str.upper().str.strip() == 'TCP').sum()

print(f"Network TCP rows: {network_tcp_count}")
print(f"Merged TCP rows:  {merged_tcp_count}")
if merged_tcp_count == network_tcp_count:
    print("✅ TCP row count matches network CSV.")
else:
    print(f"⚠️ TCP row count mismatch!")

print("\n✅ Verification complete.")
