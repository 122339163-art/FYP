import pandas as pd
import numpy as np

NETWORK_FILE = "/home/iankenny/FYP/NetData/run01/feb17normalrun_datasetdata_mastertime.csv"
POWER_FILE   = "/home/iankenny/Desktop/LargeData/PowerData/2026_02_17_13_32_30_mastertime.csv"
OUTPUT_FILE  = "/home/iankenny/Desktop/LargeData/MergedData/Unlabelled_merged.csv"

CHUNK_SIZE = 5000000
TOLERANCE = 0.0006   # 0.6 ms

print("Loading network data...")

network = pd.read_csv(NETWORK_FILE)
network.columns = network.columns.str.strip().str.lower()

network['timestamp'] = pd.to_datetime(network['date'] + ' ' + network['time']).astype('int64') / 1e9
network = network.sort_values('timestamp').reset_index(drop=True)

# Ensure correct types
network[['source','destination','protocol','info']] = network[['source','destination','protocol','info']].astype(str)
network['length'] = pd.to_numeric(network['length'], errors='coerce').fillna(0)

network['used'] = False

print("Processing power file...")

chunk_iter = pd.read_csv(POWER_FILE, chunksize=CHUNK_SIZE)
first_chunk = True

for chunk in chunk_iter:

    chunk.columns = chunk.columns.str.strip().str.lower()

    if 'current average' in chunk.columns:
        chunk = chunk.rename(columns={'current average': 'current'})

    chunk['timestamp'] = pd.to_datetime(chunk['date'] + ' ' + chunk['time']).astype('int64') / 1e9
    chunk = chunk.sort_values('timestamp').reset_index(drop=True)

    power_times = chunk['timestamp'].values

    # Create correctly typed empty columns
    chunk['source'] = pd.Series("0", index=chunk.index, dtype="object")
    chunk['destination'] = pd.Series("0", index=chunk.index, dtype="object")
    chunk['protocol'] = pd.Series("0", index=chunk.index, dtype="object")
    chunk['length'] = pd.Series(0, index=chunk.index, dtype="float64")
    chunk['info'] = pd.Series("0", index=chunk.index, dtype="object")

    for i in range(len(network)):

        if network.loc[i, 'used']:
            continue

        pkt_time = network.loc[i, 'timestamp']

        idx = np.searchsorted(power_times, pkt_time)

        candidates = []

        if idx < len(power_times):
            candidates.append(idx)
        if idx > 0:
            candidates.append(idx-1)

        best = None
        best_diff = 1e9

        for c in candidates:
            diff = abs(power_times[c] - pkt_time)
            if diff < best_diff:
                best_diff = diff
                best = c

        if best is not None and best_diff <= TOLERANCE:

            chunk.loc[best, 'source'] = network.loc[i, 'source']
            chunk.loc[best, 'destination'] = network.loc[i, 'destination']
            chunk.loc[best, 'protocol'] = network.loc[i, 'protocol']
            chunk.loc[best, 'length'] = network.loc[i, 'length']
            chunk.loc[best, 'info'] = network.loc[i, 'info']

            network.loc[i, 'used'] = True

    merged_chunk = chunk[['date','time','current','source','destination','protocol','length','info']]

    if first_chunk:
        merged_chunk.to_csv(OUTPUT_FILE, index=False)
        first_chunk = False
    else:
        merged_chunk.to_csv(OUTPUT_FILE, mode='a', header=False, index=False)

    print(f"Processed chunk: {len(chunk)} rows")

print("Merge complete.")
