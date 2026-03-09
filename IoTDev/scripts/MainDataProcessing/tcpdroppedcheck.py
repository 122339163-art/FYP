import pandas as pd

NETWORK_FILE = "/home/iankenny/FYP/NetData/run01/feb17normalrun_datasetdata_mastertime.csv"
MERGED_FILE  = "/home/iankenny/Desktop/LargeData/MergedData/merged_interpolated.csv"

CHUNK_SIZE = 20000000

net_tcp_count = 0
merged_tcp_count = 0

# streaming: count network TCP rows
for chunk in pd.read_csv(NETWORK_FILE, chunksize=CHUNK_SIZE, dtype=str, low_memory=False):
    chunk.columns = chunk.columns.str.strip().str.lower()
    net_tcp_count += (chunk["protocol"].str.upper() == "TCP").sum()

# streaming: count merged TCP rows
for chunk in pd.read_csv(MERGED_FILE, chunksize=CHUNK_SIZE, dtype=str, low_memory=False):
    chunk.columns = chunk.columns.str.strip().str.lower()
    merged_tcp_count += (chunk["protocol"].str.upper() == "TCP").sum()

print("Network TCP rows:", net_tcp_count)
print("Merged TCP rows:", merged_tcp_count)
print("Missing TCP rows:", net_tcp_count - merged_tcp_count)
