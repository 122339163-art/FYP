#!/bin/bash
#
# Smart Camera Experiment Controller
# ----------------------------------
# Responsibilities:
# - Start tcpdump for monitoring
# - Prune backup folder to max 2 recent videos
# - Launch smartcam on RB3 after 5s delay
# - Stop smartcam 10s before experiment ends
# - Cleanly stop monitoring after experiment duration
#

set -euo pipefail

############################################
# USER CONFIGURATION
############################################

# RB3 camera details
RB3_USER="root"
RB3_HOST="192.0.2.10"
RB3_BINARY="/root/smart_camera"

# Monitoring settings
CAPTURE_INTERFACE="eth0"
PCAP_FILE="experiment_$(date +%Y%m%d_%H%M%S).pcap"

BACKUP_DIR="./backups"
MAX_BACKUPS=3
KEEP_BACKUPS=2

# Experiment duration
RUN_HOURS=4
RUN_MINUTES=10
RUN_SECONDS=15

############################################
# DERIVED VALUES
############################################

TOTAL_SECONDS=$((RUN_HOURS*3600 + RUN_MINUTES*60 + RUN_SECONDS))
START_TIME=$(date +%s)
PRERUN_BUFFER=5        # seconds before starting smartcam
POSTRUN_BUFFER=10      # seconds to stop smartcam before experiment end

mkdir -p "$BACKUP_DIR"

############################################
# CLEANUP FUNCTION
############################################

cleanup() {
    echo "[+] Cleaning up..."
    # Kill tcpdump
    if [[ -n "${TCPDUMP_PID-}" ]]; then
        sudo kill -INT "$TCPDUMP_PID" 2>/dev/null || true
        wait "$TCPDUMP_PID" 2>/dev/null || true
    fi
    # Stop smartcam on RB3 safely
    if [[ -n "${SMARTCAM_PID_FILE-}" ]]; then
        ssh "$RB3_USER@$RB3_HOST" "
            if [ -f $SMARTCAM_PID_FILE ]; then
                kill \$(cat $SMARTCAM_PID_FILE) 2>/dev/null || true
                rm -f $SMARTCAM_PID_FILE
            fi
        "
    fi
}

trap cleanup EXIT SIGINT SIGTERM

############################################
# BACKUP FOLDER MANAGEMENT
############################################

prune_backups() {
    files=($(ls -1t "$BACKUP_DIR"/* 2>/dev/null || true))
    if [ ${#files[@]} -gt $MAX_BACKUPS ]; then
        for f in "${files[@]:$KEEP_BACKUPS}"; do
            rm -f "$f"
            echo "[*] Pruned old backup: $f"
        done
    fi
}

############################################
# START TCPDUMP
############################################

echo "[+] Starting network capture -> $PCAP_FILE"
sudo tcpdump -i "$CAPTURE_INTERFACE" -w "$PCAP_FILE" -U tcp or udp >/dev/null 2>&1 &
TCPDUMP_PID=$!

############################################
# START EXPERIMENT LOOP
############################################

echo "[+] Monitoring for $RUN_HOURS h $RUN_MINUTES m $RUN_SECONDS s"

SMARTCAM_PID_FILE="/tmp/smartcam.pid"
SMARTCAM_STARTED=false

while true; do
    CURRENT_TIME=$(date +%s)
    ELAPSED=$(( CURRENT_TIME - START_TIME ))

    # Exit condition
    if [ "$ELAPSED" -ge "$TOTAL_SECONDS" ]; then
        echo "[+] Experiment duration reached"
        break
    fi

    # Start smartcam after PRERUN_BUFFER
    if [ "$ELAPSED" -ge "$PRERUN_BUFFER" ] && [ "$SMARTCAM_STARTED" = false ]; then
        echo "[+] Launching smartcam on RB3"
        ssh "$RB3_USER@$RB3_HOST" "
            nohup $RB3_BINARY >/dev/null 2>&1 &
            echo \$! > $SMARTCAM_PID_FILE
        "
        SMARTCAM_STARTED=true
    fi

    # Stop smartcam POSTRUN_BUFFER before experiment ends
    if [ "$ELAPSED" -ge $(( TOTAL_SECONDS - POSTRUN_BUFFER )) ] && [ "$SMARTCAM_STARTED" = true ]; then
        echo "[+] Stopping smartcam on RB3 for idle buffer"
        ssh "$RB3_USER@$RB3_HOST" "
            if [ -f $SMARTCAM_PID_FILE ]; then
                kill \$(cat $SMARTCAM_PID_FILE) 2>/dev/null || true
                rm -f $SMARTCAM_PID_FILE
            fi
        "
        SMARTCAM_STARTED=false
    fi

    # Periodically prune backups
    prune_backups

    sleep 5
done

############################################
# FINAL STATUS
############################################

echo "========================================"
echo "Experiment complete."
echo "PCAP file:    $PCAP_FILE"
echo "Backup files: $BACKUP_DIR/"
echo "========================================"

