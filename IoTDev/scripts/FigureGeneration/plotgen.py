import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# ---- INPUT CSV ----
INPUT_CSV = "/home/iankenny/Desktop/LargeData/MergedData/most_active_hour.csv"

# ---- SAVE FIGURES IN SCRIPT DIRECTORY ----
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = SCRIPT_DIR

print("Loading data...")
df = pd.read_csv(INPUT_CSV)

df.columns = [c.lower().strip() for c in df.columns]

# Combine date + time
df["timestamp"] = pd.to_datetime(df["date"] + " " + df["time"])

# Ensure numeric columns
if "current" in df.columns:
    df["current"] = pd.to_numeric(df["current"], errors="coerce")

if "length" in df.columns:
    df["length"] = pd.to_numeric(df["length"], errors="coerce")

# Detect packet rows
packet_cols = ["source", "destination", "protocol", "length", "info"]
existing_packet_cols = [c for c in packet_cols if c in df.columns]

if existing_packet_cols:
    df["is_packet"] = False
    for col in existing_packet_cols:
        df["is_packet"] |= df[col].notna()
else:
    df["is_packet"] = False

df = df.sort_values("timestamp")

print("Generating figures...")

# -----------------------------
# Current vs Time
# -----------------------------
if "current" in df.columns:

    plot_df = df.dropna(subset=["current"])

    if len(plot_df) > 200000:
        step = len(plot_df) // 200000
        plot_df = plot_df.iloc[::step]

    plt.figure(figsize=(14,5))
    plt.plot(plot_df["timestamp"], plot_df["current"])
    plt.xlabel("Time")
    plt.ylabel("Current")
    plt.title("Current vs Time")
    plt.tight_layout()

    path = os.path.join(OUTPUT_DIR, "current_vs_time.png")
    plt.savefig(path, dpi=200)
    plt.close()
    print("Saved:", path)

# -----------------------------
# Packet rate vs Time
# -----------------------------
packet_df = df[df["is_packet"]]

if not packet_df.empty:

    packet_df = packet_df.set_index("timestamp")
    packet_rate = packet_df.resample("1s").size()

    plt.figure(figsize=(14,5))
    plt.plot(packet_rate.index, packet_rate.values)
    plt.xlabel("Time")
    plt.ylabel("Packets / second")
    plt.title("Packet Rate vs Time")
    plt.tight_layout()

    path = os.path.join(OUTPUT_DIR, "packet_rate_vs_time.png")
    plt.savefig(path, dpi=200)
    plt.close()
    print("Saved:", path)

# -----------------------------
# Current + Packet Rate Overlay
# -----------------------------
if "current" in df.columns and not packet_df.empty:

    current_resampled = (
        df.set_index("timestamp")["current"]
        .resample("1s")
        .mean()
    )

    packet_rate = packet_df.resample("1s").size()

    merged = pd.concat([current_resampled, packet_rate], axis=1)
    merged.columns = ["current", "packet_rate"]

    merged = merged.dropna()

    plt.figure(figsize=(14,5))

    ax1 = plt.gca()
    ax1.plot(merged.index, merged["current"])
    ax1.set_ylabel("Current")

    ax2 = ax1.twinx()
    ax2.plot(merged.index, merged["packet_rate"])
    ax2.set_ylabel("Packets/sec")

    plt.title("Current and Packet Rate Overlay")
    plt.tight_layout()

    path = os.path.join(OUTPUT_DIR, "current_packet_overlay.png")
    plt.savefig(path, dpi=200)
    plt.close()
    print("Saved:", path)

# -----------------------------
# Packet Length Histogram
# -----------------------------
if "length" in df.columns:

    lengths = df["length"].dropna()

    if not lengths.empty:

        plt.figure(figsize=(10,5))
        plt.hist(lengths, bins=50)
        plt.xlabel("Packet Length")
        plt.ylabel("Count")
        plt.title("Packet Length Distribution")
        plt.tight_layout()

        path = os.path.join(OUTPUT_DIR, "packet_length_distribution.png")
        plt.savefig(path, dpi=200)
        plt.close()
        print("Saved:", path)

# -----------------------------
# Current by Label
# -----------------------------
if "label" in df.columns and "current" in df.columns:

    subset = df.dropna(subset=["label","current"])

    labels = subset["label"].unique()
    data = [subset[subset["label"] == l]["current"] for l in labels]

    plt.figure(figsize=(10,5))
    plt.boxplot(data, tick_labels=labels, showfliers=False)
    plt.xlabel("Label")
    plt.ylabel("Current")
    plt.title("Current Distribution by Label")
    plt.tight_layout()

    path = os.path.join(OUTPUT_DIR, "current_by_label.png")
    plt.savefig(path, dpi=200)
    plt.close()
    print("Saved:", path)

print("Finished.")
