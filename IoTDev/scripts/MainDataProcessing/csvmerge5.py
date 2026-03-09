import pandas as pd

NETWORK_FILE = "/home/iankenny/FYP/NetData/run01/feb17normalrun_datasetdata_mastertime.csv"
POWER_FILE   = "/home/iankenny/Desktop/LargeData/PowerData/2026_02_17_13_32_30_mastertime.csv"
OUTPUT_FILE  = "/home/iankenny/Desktop/LargeData/MergedData/merged_interpolated.csv"

CHUNK_SIZE = 20000000

print("Loading network data...")

network = pd.read_csv(NETWORK_FILE)
network.columns = network.columns.str.strip().str.lower()

network["timestamp"] = pd.to_datetime(
    network["date"] + " " + network["time"]
).astype("int64") / 1e9

network = network.sort_values("timestamp").reset_index(drop=True)

network[["source","destination","protocol","info"]] = \
    network[["source","destination","protocol","info"]].astype(str)

network["length"] = pd.to_numeric(network["length"], errors="coerce").fillna(0)

net_idx = 0
total_packets = len(network)

print(f"Network packets loaded: {total_packets}")

power_iter = pd.read_csv(POWER_FILE, chunksize=CHUNK_SIZE)

first_write = True

prev_time = None
prev_current = None
prev_date = None
prev_clock = None

for chunk in power_iter:

    chunk.columns = chunk.columns.str.strip().str.lower()

    if "current average" in chunk.columns:
        chunk = chunk.rename(columns={"current average": "current"})

    chunk["timestamp"] = pd.to_datetime(
        chunk["date"] + " " + chunk["time"]
    ).astype("int64") / 1e9

    output_rows = []

    for _, row in chunk.iterrows():

        power_time = row["timestamp"]
        power_current = row["current"]

        while net_idx < total_packets:

            pkt_time = network.loc[net_idx, "timestamp"]

            if prev_time is not None and prev_time < pkt_time < power_time:

                interp_current = (prev_current + power_current) / 2

                pkt = network.loc[net_idx]

                output_rows.append({
                    "date": pkt["date"],
                    "time": pkt["time"],
                    "current": interp_current,
                    "source": pkt["source"],
                    "destination": pkt["destination"],
                    "protocol": pkt["protocol"],
                    "length": pkt["length"],
                    "info": pkt["info"]
                })

                net_idx += 1

            elif pkt_time == power_time:

                pkt = network.loc[net_idx]

                output_rows.append({
                    "date": row["date"],
                    "time": row["time"],
                    "current": power_current,
                    "source": pkt["source"],
                    "destination": pkt["destination"],
                    "protocol": pkt["protocol"],
                    "length": pkt["length"],
                    "info": pkt["info"]
                })

                net_idx += 1
                break

            else:
                break

        if net_idx >= total_packets or network.loc[net_idx, "timestamp"] != power_time:

            output_rows.append({
                "date": row["date"],
                "time": row["time"],
                "current": power_current,
                "source": "0",
                "destination": "0",
                "protocol": "0",
                "length": 0,
                "info": "0"
            })

        prev_time = power_time
        prev_current = power_current
        prev_date = row["date"]
        prev_clock = row["time"]

    out_df = pd.DataFrame(output_rows)

    if first_write:
        out_df.to_csv(OUTPUT_FILE, index=False)
        first_write = False
    else:
        out_df.to_csv(OUTPUT_FILE, mode="a", header=False, index=False)

    print(f"Processed power rows, packets merged: {net_idx}")

print("Merge complete.")
print(f"Total packets inserted: {net_idx}")
