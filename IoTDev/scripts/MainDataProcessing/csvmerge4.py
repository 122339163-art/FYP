import pandas as pd

NETWORK_FILE = "/home/iankenny/FYP/NetData/run01/feb17normalrun_datasetdata_mastertime.csv"
POWER_FILE   = "/home/iankenny/Desktop/LargeData/PowerData/2026_02_17_13_32_30_mastertime.csv"
OUTPUT_FILE  = "/home/iankenny/Desktop/LargeData/MergedData/Union_merged.csv"

CHUNK_SIZE = 10000000

print("Loading network data...")

network = pd.read_csv(NETWORK_FILE)
network.columns = network.columns.str.strip().str.lower()

network['timestamp'] = pd.to_datetime(
    network['date'] + ' ' + network['time']
).astype('int64')

network = network.sort_values('timestamp').reset_index(drop=True)

network[['source','destination','protocol','info']] = \
    network[['source','destination','protocol','info']].astype(str)

network['length'] = pd.to_numeric(network['length'], errors='coerce').fillna(0)

net_idx = 0
net_len = len(network)

print("Streaming merge starting...")

power_iter = pd.read_csv(POWER_FILE, chunksize=CHUNK_SIZE)
first_write = True

prev_power_row = None

for power_chunk in power_iter:

    power_chunk.columns = power_chunk.columns.str.strip().str.lower()
    if 'current average' in power_chunk.columns:
        power_chunk = power_chunk.rename(columns={'current average': 'current'})

    power_chunk['timestamp'] = pd.to_datetime(
        power_chunk['date'] + ' ' + power_chunk['time']
    ).astype('int64')

    power_chunk = power_chunk.sort_values('timestamp').reset_index(drop=True)

    merged_rows = []

    for _, power_row in power_chunk.iterrows():

        power_time = power_row['timestamp']

        # Insert network packets that occur BEFORE this power row
        while net_idx < net_len and network.loc[net_idx, 'timestamp'] < power_time:

            pkt = network.loc[net_idx]

            if prev_power_row is not None:
                interpolated_current = (
                    prev_power_row['current'] + power_row['current']
                ) / 2
            else:
                interpolated_current = power_row['current']

            merged_rows.append({
                'date': pkt['date'],
                'time': pkt['time'],
                'current': interpolated_current,
                'source': pkt['source'],
                'destination': pkt['destination'],
                'protocol': pkt['protocol'],
                'length': pkt['length'],
                'info': pkt['info']
            })

            net_idx += 1

        # If exact timestamp match → merge into one row
        if net_idx < net_len and network.loc[net_idx, 'timestamp'] == power_time:

            pkt = network.loc[net_idx]

            merged_rows.append({
                'date': power_row['date'],
                'time': power_row['time'],
                'current': power_row['current'],
                'source': pkt['source'],
                'destination': pkt['destination'],
                'protocol': pkt['protocol'],
                'length': pkt['length'],
                'info': pkt['info']
            })

            net_idx += 1

        else:
            # Normal power row
            merged_rows.append({
                'date': power_row['date'],
                'time': power_row['time'],
                'current': power_row['current'],
                'source': "0",
                'destination': "0",
                'protocol': "0",
                'length': 0,
                'info': "0"
            })

        prev_power_row = power_row

    merged_df = pd.DataFrame(merged_rows)

    if first_write:
        merged_df.to_csv(OUTPUT_FILE, index=False)
        first_write = False
    else:
        merged_df.to_csv(OUTPUT_FILE, mode='a', header=False, index=False)

# After power exhausted, append remaining network packets
while net_idx < net_len:

    pkt = network.loc[net_idx]

    interpolated_current = prev_power_row['current']

    row = {
        'date': pkt['date'],
        'time': pkt['time'],
        'current': interpolated_current,
        'source': pkt['source'],
        'destination': pkt['destination'],
        'protocol': pkt['protocol'],
        'length': pkt['length'],
        'info': pkt['info']
    }

    pd.DataFrame([row]).to_csv(OUTPUT_FILE, mode='a', header=False, index=False)

    net_idx += 1

print("Union merge complete.")
