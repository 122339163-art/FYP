import pandas as pd

# CONFIG
NETWORK_FILE = "/home/iankenny/FYP/NetData/run01/feb17normalrun_datasetdata_mastertime.csv"
POWER_FILE   = "/home/iankenny/Desktop/LargeData/PowerData/2026_02_17_13_32_30_mastertime.csv"
MERGED_FILE  = "/home/iankenny/Desktop/LargeData/MergedData/Unlabelled_merged.csv"
EXPECTED_COLS = ['date', 'time', 'current', 'source', 'destination', 'protocol', 'length', 'info']
CHUNK_SIZE   = 10_000_000  # Adjust for your RAM

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

# Merged TCP rows and collect first/last 5 TCP rows (chunked)
merged_tcp_count = 0
first_tcp_rows = []
last_tcp_rows = []

for chunk in pd.read_csv(MERGED_FILE, chunksize=CHUNK_SIZE, dtype=dtypes):
    tcp_chunk = chunk[chunk['protocol'].str.upper().str.strip() == 'TCP']
    
    # TCP count
    merged_tcp_count += len(tcp_chunk)
    
    # Collect first 5 TCP rows
    if len(first_tcp_rows) < 5:
        rows_needed = 5 - len(first_tcp_rows)
        first_tcp_rows.extend(tcp_chunk.head(rows_needed).to_dict('records'))
    
    # Keep last 5 TCP rows
    last_tcp_rows.extend(tcp_chunk.to_dict('records'))
    if len(last_tcp_rows) > 5:
        last_tcp_rows = last_tcp_rows[-5:]

print(f"Network TCP rows: {network_tcp_count}")
print(f"Merged TCP rows:  {merged_tcp_count}")
if merged_tcp_count == network_tcp_count:
    print("✅ TCP row count matches network CSV.")
else:
    print(f"⚠️ TCP row count mismatch!")

# ---- 4. Print first/last 5 TCP rows ----
first_tcp_df = pd.DataFrame(first_tcp_rows)
last_tcp_df  = pd.DataFrame(last_tcp_rows)

print("\n===== First 5 TCP rows in merged CSV =====")
print(first_tcp_df)

print("\n===== Last 5 TCP rows in merged CSV =====")
print(last_tcp_df)

print("\n✅ Full merge verification complete.")
