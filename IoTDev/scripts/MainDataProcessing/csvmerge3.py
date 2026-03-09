import pandas as pd

NETWORK_FILE = "/home/iankenny/FYP/NetData/run01/feb17normalrun_datasetdata_mastertime.csv"
POWER_FILE   = "/home/iankenny/Desktop/LargeData/PowerData/2026_02_17_13_32_30_mastertime.csv"
OUTPUT_FILE  = "/home/iankenny/Desktop/LargeData/MergedData/Unlabelled_merged.csv"

WINDOW_SIZE = 20000000
STEP_SIZE   = 5000000
TOLERANCE   = 0.000408   # 0.408ms(2*T)

print("Loading network data...")

network = pd.read_csv(NETWORK_FILE)
network.columns = network.columns.str.strip().str.lower()

network['timestamp'] = pd.to_datetime(network['date'] + ' ' + network['time']).astype('int64') / 1e9
network = network.sort_values('timestamp').reset_index(drop=True)

network[['source','destination','protocol','info']] = network[['source','destination','protocol','info']].astype(str)
network['length'] = pd.to_numeric(network['length'], errors='coerce').fillna(0)

net_idx = 0

print("Loading initial power window...")

power_iter = pd.read_csv(POWER_FILE, chunksize=STEP_SIZE)

# Load first 20M rows
power_buffer = pd.concat([next(power_iter) for _ in range(WINDOW_SIZE // STEP_SIZE)])
power_buffer.columns = power_buffer.columns.str.strip().str.lower()

if 'current average' in power_buffer.columns:
    power_buffer = power_buffer.rename(columns={'current average': 'current'})

power_buffer['timestamp'] = pd.to_datetime(power_buffer['date'] + ' ' + power_buffer['time']).astype('int64') / 1e9
power_buffer = power_buffer.reset_index(drop=True)

first_write = True

while True:

    power_buffer['source'] = "0"
    power_buffer['destination'] = "0"
    power_buffer['protocol'] = "0"
    power_buffer['length'] = 0
    power_buffer['info'] = "0"

    for i in range(len(power_buffer)):

        if net_idx >= len(network):
            break

        power_time = power_buffer.loc[i, 'timestamp']
        pkt_time   = network.loc[net_idx, 'timestamp']

        diff = power_time - pkt_time

        if abs(diff) <= TOLERANCE:

            power_buffer.loc[i, 'source'] = network.loc[net_idx, 'source']
            power_buffer.loc[i, 'destination'] = network.loc[net_idx, 'destination']
            power_buffer.loc[i, 'protocol'] = network.loc[net_idx, 'protocol']
            power_buffer.loc[i, 'length'] = network.loc[net_idx, 'length']
            power_buffer.loc[i, 'info'] = network.loc[net_idx, 'info']

            net_idx += 1

        elif power_time < pkt_time:
            continue
        else:
            net_idx += 1

    write_chunk = power_buffer.iloc[:STEP_SIZE][['date','time','current','source','destination','protocol','length','info']]

    if first_write:
        write_chunk.to_csv(OUTPUT_FILE, index=False)
        first_write = False
    else:
        write_chunk.to_csv(OUTPUT_FILE, mode='a', header=False, index=False)

    print("Wrote 5M rows")

    # Slide window
    power_buffer = power_buffer.iloc[STEP_SIZE:].reset_index(drop=True)

    try:
        new_chunk = next(power_iter)
    except StopIteration:
        # Write remaining rows
        remaining = power_buffer[['date','time','current','source','destination','protocol','length','info']]
        remaining.to_csv(OUTPUT_FILE, mode='a', header=False, index=False)
        break

    new_chunk.columns = new_chunk.columns.str.strip().str.lower()

    if 'current average' in new_chunk.columns:
        new_chunk = new_chunk.rename(columns={'current average': 'current'})

    new_chunk['timestamp'] = pd.to_datetime(new_chunk['date'] + ' ' + new_chunk['time']).astype('int64') / 1e9

    power_buffer = pd.concat([power_buffer, new_chunk]).reset_index(drop=True)

print("Merge complete.")
