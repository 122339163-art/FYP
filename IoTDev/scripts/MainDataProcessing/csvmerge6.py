import csv
import math
from collections import defaultdict
from datetime import datetime
import pandas as pd

NETWORK_FILE = "/home/iankenny/FYP/NetData/run01/feb17normalrun_datasetdata_mastertime.csv"
POWER_FILE   = "/home/iankenny/Desktop/LargeData/PowerData/2026_02_17_13_32_30_mastertime.csv"
OUTPUT_FILE  = "/home/iankenny/Desktop/LargeData/MergedData/merged_interpolated.csv"

# 0.204 ms = 204,000 ns
SAMPLE_INTERVAL_NS = 204_000
# 0.408 ms = 408,000 ns
TOLERANCE_NS = 408_000


def parse_timestamp_ns(date_str: str, time_str: str) -> int:
    """
    Parse date + time to integer nanoseconds since epoch.
    Uses pandas for robustness with fractional seconds.
    """
    return int(pd.Timestamp(f"{date_str} {time_str}").value)


def normalise_columns(cols):
    return [c.strip().lower() for c in cols]


def get_power_header_and_first_timestamp(power_file):
    """
    Read the power header and first data row only.
    """
    with open(power_file, "r", newline="") as f:
        reader = csv.reader(f)
        raw_header = next(reader)
        header = normalise_columns(raw_header)

        date_idx = header.index("date")
        time_idx = header.index("time")

        if "current average" in header:
            current_idx = header.index("current average")
        elif "current" in header:
            current_idx = header.index("current")
        else:
            raise ValueError("Could not find 'current average' or 'current' column in power file.")

        first_row = next(reader)
        first_ts_ns = parse_timestamp_ns(first_row[date_idx], first_row[time_idx])

    return raw_header, header, date_idx, time_idx, current_idx, first_ts_ns


def load_network_packets(network_file, first_power_ts_ns):
    """
    Load network file into memory, sort by time, and prepare packet records.
    """
    print("Loading network packets...")
    net = pd.read_csv(network_file)
    net.columns = net.columns.str.strip().str.lower()

    required = ["date", "time", "source", "destination", "protocol", "length", "info"]
    for col in required:
        if col not in net.columns:
            raise ValueError(f"Missing required network column: {col}")

    ts = pd.to_datetime(net["date"].astype(str) + " " + net["time"].astype(str))
    net["timestamp_ns"] = ts.astype("int64")

    net = net.sort_values("timestamp_ns", kind="mergesort").reset_index(drop=True)

    net["source"] = net["source"].astype(str)
    net["destination"] = net["destination"].astype(str)
    net["protocol"] = net["protocol"].astype(str)
    net["info"] = net["info"].astype(str)
    net["length"] = pd.to_numeric(net["length"], errors="coerce").fillna(0).astype(int)

    packets = []
    for row in net.itertuples(index=False):
        pkt = {
            "date": str(row.date),
            "time": str(row.time),
            "timestamp_ns": int(row.timestamp_ns),
            "source": row.source,
            "destination": row.destination,
            "protocol": row.protocol,
            "length": int(row.length),
            "info": row.info,
        }
        packets.append(pkt)

    print(f"Loaded {len(packets):,} packets")
    return packets


def assign_packets_to_power_rows(packets, first_power_ts_ns):
    """
    First pass:
    Assign each packet to the nearest unoccupied power row within tolerance.

    If no free row is available within tolerance, keep it for insertion later.
    """
    print("Assigning packets to nearest available power rows...")

    # row_index -> packet
    assigned = {}
    # lower_power_row_index -> list[packet]
    inserted = defaultdict(list)

    assigned_count = 0
    inserted_count = 0

    for pkt in packets:
        pkt_ts = pkt["timestamp_ns"]

        # Approximate nearest sample index
        approx_idx = int(round((pkt_ts - first_power_ts_ns) / SAMPLE_INTERVAL_NS))

        best_idx = None
        best_delta = None

        # Search local neighborhood; tolerance is 2 sample intervals
        for idx in range(max(0, approx_idx - 2), approx_idx + 3):
            sample_ts = first_power_ts_ns + idx * SAMPLE_INTERVAL_NS
            delta = abs(pkt_ts - sample_ts)

            if delta <= TOLERANCE_NS and idx not in assigned:
                if best_delta is None or delta < best_delta or (delta == best_delta and idx < best_idx):
                    best_idx = idx
                    best_delta = delta

        if best_idx is not None:
            assigned[best_idx] = pkt
            assigned_count += 1
        else:
            # Packet needs a new inserted row.
            # Put it between lower_idx and lower_idx + 1.
            lower_idx = math.floor((pkt_ts - first_power_ts_ns) / SAMPLE_INTERVAL_NS)
            if lower_idx < 0:
                lower_idx = 0
            inserted[lower_idx].append(pkt)
            inserted_count += 1

    # Keep inserted rows ordered by packet timestamp within each interval
    for lower_idx in inserted:
        inserted[lower_idx].sort(key=lambda p: p["timestamp_ns"])

    print(f"Packets assigned to existing power rows: {assigned_count:,}")
    print(f"Packets requiring inserted rows:     {inserted_count:,}")
    print(f"Total packets accounted for:         {assigned_count + inserted_count:,}")

    return assigned, inserted


def write_power_row(writer, power_row, assigned_packet=None):
    """
    Write one output row for a power sample.
    """
    date_str, time_str, current_str = power_row

    if assigned_packet is None:
        writer.writerow([
            date_str,
            time_str,
            current_str,
            "",
            "",
            "",
            "",
            "",
        ])
    else:
        writer.writerow([
            date_str,
            time_str,
            current_str,
            assigned_packet["source"],
            assigned_packet["destination"],
            assigned_packet["protocol"],
            assigned_packet["length"],
            assigned_packet["info"],
        ])


def write_inserted_rows_between(writer, packets, prev_current, next_current, prev_sample_ts_ns):
    """
    Write inserted packet rows between two adjacent power rows, using linear interpolation.

    The packet timestamps can fall anywhere in this interval. Interpolation ratio is
    based on the fixed sample interval.
    """
    if not packets:
        return

    delta_current = next_current - prev_current

    for pkt in packets:
        ratio = (pkt["timestamp_ns"] - prev_sample_ts_ns) / SAMPLE_INTERVAL_NS
        # Clamp in case of slight drift / rounding beyond the interval
        ratio = max(0.0, min(1.0, ratio))
        interp_current = prev_current + ratio * delta_current

        writer.writerow([
            pkt["date"],
            pkt["time"],
            f"{interp_current:.12g}",
            pkt["source"],
            pkt["destination"],
            pkt["protocol"],
            pkt["length"],
            pkt["info"],
        ])


def stream_merge(power_file, output_file, assigned, inserted):
    """
    Stream through the power CSV once and write the merged output.

    Output order:
      power row i
      inserted rows between i and i+1
      power row i+1
      ...
    """
    print("Streaming power file and writing merged output...")

    with open(power_file, "r", newline="") as pf, open(output_file, "w", newline="") as outf:
        reader = csv.reader(pf)
        writer = csv.writer(outf)

        raw_header = next(reader)
        power_header = normalise_columns(raw_header)

        date_idx = power_header.index("date")
        time_idx = power_header.index("time")

        if "current average" in power_header:
            current_idx = power_header.index("current average")
        elif "current" in power_header:
            current_idx = power_header.index("current")
        else:
            raise ValueError("Could not find 'current average' or 'current' column in power file.")

        writer.writerow([
            "date", "time", "current", "source", "destination", "protocol", "length", "info"
        ])

        prev_row = None
        prev_current = None
        prev_idx = None
        row_idx = 0

        power_rows_written = 0
        inserted_rows_written = 0

        for raw_row in reader:
            date_str = raw_row[date_idx]
            time_str = raw_row[time_idx]
            current_val = float(raw_row[current_idx])

            curr_row = (date_str, time_str, raw_row[current_idx])

            if prev_row is None:
                prev_row = curr_row
                prev_current = current_val
                prev_idx = row_idx
                row_idx += 1
                continue

            # Now we have prev and current, so we can safely write prev
            write_power_row(writer, prev_row, assigned.get(prev_idx))
            power_rows_written += 1

            # Write inserted rows between prev_idx and prev_idx + 1
            if prev_idx in inserted:
                prev_sample_ts_ns = None
                # Reconstruct sample timestamp using fixed interval.
                # We only need relative time from index 0, so row index is enough.
                # first power timestamp was used during planning and is embedded in packet positions.
                # We recover it from the first packet mapping indirectly by passing it externally would
                # be cleaner, but we avoid a second global here by storing it later.
                raise RuntimeError("Internal error: first power timestamp was not attached.")

            prev_row = curr_row
            prev_current = current_val
            prev_idx = row_idx
            row_idx += 1

        # Write the final power row
        if prev_row is not None:
            write_power_row(writer, prev_row, assigned.get(prev_idx))
            power_rows_written += 1

        print(f"Power rows written:   {power_rows_written:,}")
        print(f"Inserted rows written:{inserted_rows_written:,}")


def stream_merge_with_interpolation(power_file, output_file, assigned, inserted, first_power_ts_ns):
    """
    Same as stream_merge, but includes interpolation timestamp support.
    """
    print("Streaming power file and writing merged output...")

    with open(power_file, "r", newline="") as pf, open(output_file, "w", newline="") as outf:
        reader = csv.reader(pf)
        writer = csv.writer(outf)

        raw_header = next(reader)
        power_header = normalise_columns(raw_header)

        date_idx = power_header.index("date")
        time_idx = power_header.index("time")

        if "current average" in power_header:
            current_idx = power_header.index("current average")
        elif "current" in power_header:
            current_idx = power_header.index("current")
        else:
            raise ValueError("Could not find 'current average' or 'current' column in power file.")

        writer.writerow([
            "date", "time", "current", "source", "destination", "protocol", "length", "info"
        ])

        prev_row = None
        prev_current = None
        prev_idx = None
        row_idx = 0

        power_rows_written = 0
        inserted_rows_written = 0

        for raw_row in reader:
            date_str = raw_row[date_idx]
            time_str = raw_row[time_idx]
            current_val = float(raw_row[current_idx])

            curr_row = (date_str, time_str, raw_row[current_idx])

            if prev_row is None:
                prev_row = curr_row
                prev_current = current_val
                prev_idx = row_idx
                row_idx += 1
                continue

            # Write the previous real power row
            write_power_row(writer, prev_row, assigned.get(prev_idx))
            power_rows_written += 1

            # Write any inserted packet rows between prev_idx and prev_idx + 1
            between_packets = inserted.get(prev_idx, [])
            if between_packets:
                prev_sample_ts_ns = first_power_ts_ns + prev_idx * SAMPLE_INTERVAL_NS
                write_inserted_rows_between(
                    writer=writer,
                    packets=between_packets,
                    prev_current=prev_current,
                    next_current=current_val,
                    prev_sample_ts_ns=prev_sample_ts_ns,
                )
                inserted_rows_written += len(between_packets)

            prev_row = curr_row
            prev_current = current_val
            prev_idx = row_idx
            row_idx += 1

            if row_idx % 5_000_000 == 0:
                print(
                    f"Processed power rows: {row_idx:,} | "
                    f"power written: {power_rows_written:,} | "
                    f"inserted written: {inserted_rows_written:,}"
                )

        # Final power row
        if prev_row is not None:
            write_power_row(writer, prev_row, assigned.get(prev_idx))
            power_rows_written += 1

        # Rare case: packets after the final power sample
        # Fall back to last observed current.
        tail_packets = inserted.get(prev_idx, [])
        if tail_packets:
            for pkt in tail_packets:
                writer.writerow([
                    pkt["date"],
                    pkt["time"],
                    f"{prev_current:.12g}",
                    pkt["source"],
                    pkt["destination"],
                    pkt["protocol"],
                    pkt["length"],
                    pkt["info"],
                ])
            inserted_rows_written += len(tail_packets)

        print(f"Power rows written:    {power_rows_written:,}")
        print(f"Inserted rows written: {inserted_rows_written:,}")
        print(f"Total output rows:     {power_rows_written + inserted_rows_written:,}")


def main():
    _, _, _, _, _, first_power_ts_ns = get_power_header_and_first_timestamp(POWER_FILE)

    packets = load_network_packets(NETWORK_FILE, first_power_ts_ns)
    assigned, inserted = assign_packets_to_power_rows(packets, first_power_ts_ns)

    total_packets = len(packets)
    accounted_for = len(assigned) + sum(len(v) for v in inserted.values())
    if accounted_for != total_packets:
        raise RuntimeError(
            f"Packet accounting error: expected {total_packets}, got {accounted_for}"
        )

    stream_merge_with_interpolation(
        power_file=POWER_FILE,
        output_file=OUTPUT_FILE,
        assigned=assigned,
        inserted=inserted,
        first_power_ts_ns=first_power_ts_ns,
    )

    print("Merge complete.")
    print(f"Output written to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
