import pandas as pd

# CONFIG
NETWORK_FILE = "/home/iankenny/FYP/NetData/run01/feb17normalrun_datasetdata_mastertime.csv"      # 260k rows
POWER_FILE = "/home/iankenny/Desktop/LargeData/PowerData/2026_02_17_13_32_30_mastertime.csv"        # 430M rows
OUTPUT_FILE = "/home/iankenny/Desktop/LargeData/MergedData/Unlabelled_merged.csv"
CHUNK_SIZE = 10000000           # Adjust based on RAM
TOLERANCE = 0.001              

# ---- Load and prepare network CSV ----
network = pd.read_csv(NETWORK_FILE)
network.columns = network.columns.str.strip().str.lower()
network['timestamp'] = pd.to_datetime(network['date'] + ' ' + network['time']).astype('int64') / 1e9
network = network.sort_values('timestamp').reset_index(drop=True)
network['used'] = False

# ---- Process power CSV in chunks ----
chunk_iter = pd.read_csv(POWER_FILE, chunksize=CHUNK_SIZE)
first_chunk = True

for chunk in chunk_iter:
    # Normalize column names
    chunk.columns = chunk.columns.str.strip().str.lower()
    # Rename 'current average' -> 'current'
    if 'current average' in chunk.columns:
        chunk = chunk.rename(columns={'current average': 'current'})

    # Create timestamp
    chunk['timestamp'] = pd.to_datetime(chunk['date'] + ' ' + chunk['time']).astype('int64') / 1e9
    chunk = chunk.sort_values('timestamp').reset_index(drop=True)
    
    # Prepare lists to append network data
    appended_source = []
    appended_dest   = []
    appended_proto  = []
    appended_length = []
    appended_info   = []

    net_idx = 0
    for idx, row in chunk.iterrows():
        # Advance network pointer
        while net_idx < len(network) and network.loc[net_idx, 'timestamp'] + TOLERANCE < row['timestamp']:
            net_idx += 1

        if net_idx < len(network):
            net_time = network.loc[net_idx, 'timestamp']
            if abs(net_time - row['timestamp']) <= TOLERANCE and not network.loc[net_idx, 'used']:
                # Append network data
                appended_source.append(network.loc[net_idx, 'source'])
                appended_dest.append(network.loc[net_idx, 'destination'])
                appended_proto.append(network.loc[net_idx, 'protocol'])
                appended_length.append(network.loc[net_idx, 'length'])
                appended_info.append(network.loc[net_idx, 'info'])
                network.loc[net_idx, 'used'] = True
            else:
                appended_source.append(0)
                appended_dest.append(0)
                appended_proto.append("0")
                appended_length.append(0)
                appended_info.append("0")
        else:
            appended_source.append(0)
            appended_dest.append(0)
            appended_proto.append("0")
            appended_length.append(0)
            appended_info.append("0")

    # Add network columns
    chunk['source'] = appended_source
    chunk['destination'] = appended_dest
    chunk['protocol'] = appended_proto
    chunk['length'] = appended_length
    chunk['info'] = appended_info

    # Select final columns
    merged_chunk = chunk[['date', 'time', 'current', 'source', 'destination', 'protocol', 'length', 'info']]

    # Write/append
    if first_chunk:
        merged_chunk.to_csv(OUTPUT_FILE, index=False)
        first_chunk = False
    else:
        merged_chunk.to_csv(OUTPUT_FILE, mode='a', header=False, index=False)

    print(f"Processed chunk with {len(chunk)} rows")
