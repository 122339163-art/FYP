#!/bin/bash
#
# Smart Camera Experiment Controller
#
# WHAT THIS SCRIPT DOES:
# ---------------------
# 1. Starts capturing network traffic (UDP + TCP) into a PCAP file
# 2. Starts a TCP server to receive video backups
# 3. Launches the smart camera program on the RB3 Gen 2 over SSH
# 4. Runs for a user-specified amount of time
# 5. Stops everything cleanly
#
# WHY THIS EXISTS:
# ----------------
# This allows fully automated, repeatable data collection for
# multimodal datasets (power + network + video artifacts).
#

set -e

############################################
# USER CONFIGURATION
############################################

# RB3 Gen 2 connection details
RB3_USER="root"
RB3_HOST="192.0.2.10"            # IP of RB3 Gen 2
RB3_BINARY="/root/smart_camera"  # Path to camera program on RB3

# Network capture settings
CAPTURE_INTERFACE="eth0"
PCAP_FILE="experiment_$(date +%Y%m%d_%H%M%S).pcap"

# TCP backup receiver settings
BACKUP_PORT=10000
BACKUP_DIR="./backups"

# How long to run (example: 4h 10m 15s)
RUN_HOURS=4
RUN_MINUTES=10
RUN_SECONDS=15

############################################
# DERIVED VALUES
############################################

TOTAL_SECONDS=$((RUN_HOURS*3600 + RUN_MINUTES*60 + RUN_SECONDS))

############################################
# SAFETY CHECKS
############################################

if [ "$TOTAL_SECONDS" -le 0 ]; then
    echo "ERROR: Total run time must be greater than zero."
    exit 1
fi

mkdir -p "$BACKUP_DIR"

############################################
# START PACKET CAPTURE
############################################

echo "[+] Starting network capture -> $PCAP_FILE"

sudo tcpdump \
    -i "$CAPTURE_INTERFACE" \
    -w "$PCAP_FILE" \
    tcp or udp \
    >/dev/null 2>&1 &

TCPDUMP_PID=$!

############################################
# START TCP BACKUP RECEIVER
############################################

echo "[+] Starting TCP backup receiver on port $BACKUP_PORT"

(
    while true; do
        TIMESTAMP=$(date +%Y%m%d_%H%M%S)
        OUTFILE="$BACKUP_DIR/backup_$TIMESTAMP.raw"

        echo "[*] Waiting for backup connection..."
        nc -l -p "$BACKUP_PORT" > "$OUTFILE"

        echo "[*] Backup received -> $OUTFILE"
    done
) &

BACKUP_PID=$!

############################################
# START SMART CAMERA ON RB3
############################################

echo "[+] Launching smart camera on RB3 Gen 2"

ssh "$RB3_USER@$RB3_HOST" "
    nohup $RB3_BINARY >/dev/null 2>&1 &
    echo \$! > /tmp/smartcam.pid
"

############################################
# WAIT FOR EXPERIMENT DURATION
############################################

echo "[+] Experiment running for $RUN_HOURS h $RUN_MINUTES m $RUN_SECONDS s"
sleep "$TOTAL_SECONDS"

############################################
# STOP SMART CAMERA
############################################

echo "[+] Stopping smart camera on RB3"

ssh "$RB3_USER@$RB3_HOST" "
    if [ -f /tmp/smartcam.pid ]; then
        kill \$(cat /tmp/smartcam.pid)
        rm /tmp/smartcam.pid
    fi
"

############################################
# STOP BACKUP RECEIVER
############################################

echo "[+] Stopping TCP backup receiver"
kill "$BACKUP_PID" 2>/dev/null || true

############################################
# STOP PACKET CAPTURE
############################################

echo "[+] Stopping packet capture"
sudo kill "$TCPDUMP_PID"
wait "$TCPDUMP_PID" 2>/dev/null || true

############################################
# FINAL STATUS
############################################

echo "========================================"
echo "Experiment complete."
echo "PCAP file:    $PCAP_FILE"
echo "Backup files: $BACKUP_DIR/"
echo "========================================"

