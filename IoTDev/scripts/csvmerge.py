import csv
from datetime import datetime
import math

# ====== CONFIGURATION ======
PACKET_FILE = "/home/iankenny/FYP/NetData/run01/feb17normalrun_datasetdata_mastertime.csv"
CURRENT_FILE = "/home/iankenny/Desktop/LargeData/PowerData/2026_02_17_13_32_30_mastertime.csv"
OUTPUT_FILE = "/home/iankenny/Desktop/LargeData/MergedData/Unlabelled_merged.csv"

ROUND_INTERVAL = 0.000204  # seconds
TIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
# ============================


def to_epoch(date_str, time_str):
    dt = datetime.strptime(f"{date_str} {time_str}", TIME_FORMAT)
    return dt.timestamp()


def round_to_interval(epoch_time):
    return round(epoch_time / ROUND_INTERVAL) * ROUND_INTERVAL


def main():
    with open(PACKET_FILE, "r", newline="") as f_packets, \
         open(CURRENT_FILE, "r", newline="") as f_current, \
         open(OUTPUT_FILE, "w", newline="") as f_out:

        packets_reader = csv.reader(f_packets)
        current_reader = csv.reader(f_current)
        writer = csv.writer(f_out)

        # Skip headers
        next(packets_reader)
        next(current_reader)

        # Write output header
        writer.writerow([
            "date", "time", "current",
            "source", "destination", "protocol", "length", "info"
        ])

        # Initialize first packet

        packet_row = next(packets_reader, None)

        if not packet_row:
            raise RuntimeError("Packet file is empty")

        packet_epoch = round_to_interval(
            to_epoch(packet_row[0], packet_row[1])
        )

        next_packet = next(packets_reader, None)

        if next_packet:
            next_packet_epoch = round_to_interval(
                to_epoch(next_packet[0], next_packet[1])
            )
        else:
            next_packet_epoch = None

        for current_row in current_reader:

            current_epoch = round_to_interval(
                to_epoch(current_row[0], current_row[1])
            )

            # Advance packet pointer if next packet is closer
            while next_packet_epoch is not None:

                current_diff = abs(current_epoch - packet_epoch)
                next_diff = abs(current_epoch - next_packet_epoch)

                if next_diff < current_diff:
                    packet_row = next_packet
                    packet_epoch = next_packet_epoch

                    next_packet = next(packets_reader, None)
                    if next_packet:
                        next_packet_epoch = round_to_interval(
                            to_epoch(next_packet[0], next_packet[1])
                        )
                    else:
                        next_packet_epoch = None
                else:
                    break

            writer.writerow([
                current_row[0],
                current_row[1],
                current_row[2],
                packet_row[2],
                packet_row[3],
                packet_row[4],
                packet_row[5],
                packet_row[6]
            ])

    print("Merge complete.")


if __name__ == "__main__":
    main()
