/*
 * SmartCam traffic profile simulator (for lab/testing use only)
 * - UDP-based synthetic stream to a server
 * - Simulates base stream bitrate, keepalive messages, randomized motion events
 *
 * Build:
 *   gcc -O2 -std=c11 -o smartcam_sim main.c
 *
 * Usage:
 *   ./smartcam_sim [options]
 *
 * Options (examples):
 *   -a <addr>    Server IPv4 address (default: 192.0.2.1)
 *   -p <port>    Server UDP port (default: 9000)
 *   -b <mbps>    Base stream bitrate in Mbps (default: 2.5)
 *   -m <mbps>    Motion/burst bitrate in Mbps (default: 5.0)
 *   -k <sec>     Keepalive interval in seconds (default: 30)
 *   -s <bytes>   UDP payload size in bytes (default: 1200)
 *   -i <sec>     Min interval between motion events (default: 20)
 *   -x <sec>     Max interval between motion events (default: 180)
 *   -h           Show this help and exit
 *
 * Notes:
 * - Intended to simulate network patterns for lab/testing. 
 */

#define _POSIX_C_SOURCE 200809L /* Enable POSIX features like clock_gettime, nanosleep, etc. */

#include <stdio.h>      // Standard I/O functions (printf, fprintf)
#include <stdlib.h>     // Standard library functions (malloc, free, rand, atoi, strtod)
#include <string.h>     // String manipulation functions (memcpy, memset, strncpy)
#include <time.h>       // Time functions (time, nanosleep, clock_gettime)
#include <stdint.h>     // Fixed-width integer types (uint64_t, uint32_t)
#include <unistd.h>     // POSIX API (close, getopt)
#include <errno.h>      // Error codes (EINTR)
#include <signal.h>     // Signal handling (SIGINT)
#include <getopt.h>     // Command-line argument parsing (getopt)

#include <sys/types.h>  // Basic system data types (used by socket)
#include <sys/socket.h> // Socket API (socket, sendto)
#include <arpa/inet.h>  // Internet address conversions (inet_pton, htons)
#include <netinet/in.h> // sockaddr_in structure

volatile sig_atomic_t stop = 0; // Global flag for clean shutdown via signal
static void handle_sigint(int sig) { (void)sig; stop = 1; } 
// Signal handler for SIGINT (Ctrl+C). Sets `stop` to 1 to exit main loop safely.

/* Return monotonic time in milliseconds (good for intervals) */
static inline uint64_t now_ms(void) {
    struct timespec t;
    if (clock_gettime(CLOCK_MONOTONIC, &t) != 0) return 0; // Get monotonic clock
    return (uint64_t)t.tv_sec * 1000ULL + (uint64_t)(t.tv_nsec / 1000000L); 
    // Convert seconds+nano to milliseconds
}

/* Sleep best-effort for given milliseconds (handles interrupts) */
static void msleep(uint64_t ms) {
    struct timespec req, rem;
    req.tv_sec = (time_t)(ms / 1000);                   // Seconds part
    req.tv_nsec = (long)((ms % 1000) * 1000000);        // Nanoseconds part
    while (nanosleep(&req, &rem) == -1 && errno == EINTR) req = rem; 
    // Repeat if interrupted by signal
}

/* Send UDP packet (ignore individual send errors) */
static void udp_send(int sock, const struct sockaddr_in *dst, const void *buf, size_t len) {
    sendto(sock, buf, len, 0, (const struct sockaddr*)dst, sizeof(*dst));
    // Fire-and-forget UDP send
}

/* Generate pseudo-random next motion event interval in seconds (uniform) */
static int next_motion_interval_s(int min_s, int max_s) {
    if (max_s <= min_s) return min_s;                   // Edge case
    return min_s + rand() % (max_s - min_s + 1);       // Random number between min and max inclusive
}

/* Motion event duration: 10-30s (configurable easily here) */
static int motion_duration_s(void) {
    return 10 + rand() % 21; // Random duration between 10 and 30 seconds
}

/* Convert Mbps to bytes-per-second (double for fractional pps) */
static inline double mbps_to_Bps(double mbps) {
    return (mbps * 1000000.0) / 8.0; // Convert megabits/sec to bytes/sec
}

static void print_usage(const char *prog) {
    fprintf(stderr,
            "Usage: %s [options]\n"
            "  -a <addr>    Server IPv4 address (default: 192.0.2.1)\n"
            "  -p <port>    Server UDP port (default: 9000)\n"
            "  -b <mbps>    Base stream bitrate in Mbps (default: 2.5)\n"
            "  -m <mbps>    Motion/burst bitrate in Mbps (default: 5.0)\n"
            "  -k <sec>     Keepalive interval in seconds (default: 30)\n"
            "  -s <bytes>   UDP payload size in bytes (default: 1200)\n"
            "  -i <sec>     Min interval between motion events (default: 600)\n"
            "  -x <sec>     Max interval between motion events (default: 7200)\n"
            "  -h           Show this help and exit\n",
            prog); // Prints CLI usage information
}

int main(int argc, char **argv) {
    /* Default parameters */
    char server_ip[64] = "192.0.2.1"; /* Default IP (example/reserved) */
    int server_port = 9000;           // Default UDP port
    double base_stream_mbps = 2.5;    // Default base stream bitrate
    double motion_burst_mbps = 5.0;   // Default motion/burst bitrate
    int keepalive_interval_s = 30;    // Default keepalive interval
    size_t packet_size = 1200;        // Default UDP payload size
    int min_motion_interval_s = 600;   // Minimum interval between motion events
    int max_motion_interval_s = 7200;  // Maximum interval between motion events

    int opt;
    while ((opt = getopt(argc, argv, "a:p:b:m:k:s:i:x:h")) != -1) {
        switch (opt) {
        case 'a':
            strncpy(server_ip, optarg, sizeof(server_ip) - 1); // Copy user-supplied server IP
            server_ip[sizeof(server_ip) - 1] = '\0';           // Ensure null-terminated
            break;
        case 'p':
            server_port = atoi(optarg);                        // Convert port to integer
            if (server_port <= 0 || server_port > 65535) {
                fprintf(stderr, "Invalid port: %s\n", optarg);
                return 1;
            }
            break;
        case 'b':
            base_stream_mbps = strtod(optarg, NULL);          // Convert string to double
            if (base_stream_mbps <= 0) { fprintf(stderr, "Invalid base bitrate\n"); return 1; }
            break;
        case 'm':
            motion_burst_mbps = strtod(optarg, NULL);         // Convert string to double
            if (motion_burst_mbps <= 0) { fprintf(stderr, "Invalid motion bitrate\n"); return 1; }
            break;
        case 'k':
            keepalive_interval_s = atoi(optarg);              // Convert string to int
            if (keepalive_interval_s <= 0) { fprintf(stderr, "Invalid keepalive interval\n"); return 1; }
            break;
        case 's':
            packet_size = (size_t)atoi(optarg);              // Convert to size_t
            if (packet_size < 64) { fprintf(stderr, "Packet size too small\n"); return 1; }
            break;
        case 'i':
            min_motion_interval_s = atoi(optarg);
            if (min_motion_interval_s < 0) { fprintf(stderr, "Invalid min motion interval\n"); return 1; }
            break;
        case 'x':
            max_motion_interval_s = atoi(optarg);
            if (max_motion_interval_s < min_motion_interval_s) {
                fprintf(stderr, "Max interval must be >= min interval\n");
                return 1;
            }
            break;
        case 'h':
        default:
            print_usage(argv[0]); // Show help if unknown option
            return 0;
        }
    }

    /* Install SIGINT handler for clean shutdown */
    signal(SIGINT, handle_sigint); // Ctrl+C sets stop=1

    /* Seed PRNG (simple) */
    srand((unsigned)time(NULL) ^ (unsigned)getpid()); // Seed random number generator

    /* Derived values for pacing (packets per second) */
    const double base_Bps = mbps_to_Bps(base_stream_mbps);  // Base bytes/sec
    const double motion_Bps = mbps_to_Bps(motion_burst_mbps);// Motion bytes/sec
    const double base_pps = base_Bps / (double)packet_size; // Packets/sec for base stream
    const double motion_pps = motion_Bps / (double)packet_size; // Packets/sec for motion burst

    /* Setup UDP socket and destination address */
    int sock = socket(AF_INET, SOCK_DGRAM, 0); // Create UDP socket
    if (sock < 0) { perror("socket"); return 1; }

    struct sockaddr_in dst;
    memset(&dst, 0, sizeof(dst)); // Zero-out structure
    dst.sin_family = AF_INET;     // IPv4
    dst.sin_port = htons((uint16_t)server_port); // Convert port to network byte order
    if (inet_pton(AF_INET, server_ip, &dst.sin_addr) != 1) {
        fprintf(stderr, "Invalid server IP: %s\n", server_ip);
        close(sock);
        return 1;
    }

    /* Streaming state and timing */
    int streaming_mode = 1; /* streaming on by default; could be toggled externally */
    uint64_t last_keepalive_ms = now_ms(); // Last keepalive send time
    uint64_t next_motion_ms = now_ms() + (uint64_t)next_motion_interval_s(min_motion_interval_s, max_motion_interval_s) * 1000ULL;
    // Schedule first motion event
    int in_motion = 0;       // Flag indicating if currently in motion
    uint64_t motion_end_ms = 0; // End time for current motion

    uint64_t seq = 0; // Packet sequence number
    unsigned char *payload = malloc(packet_size); // Allocate buffer for UDP packet
    if (!payload) { fprintf(stderr, "Out of memory\n"); close(sock); return 1; }

    const char keepalive_msg[] = "{\"type\":\"keepalive\"}"; // Keepalive JSON message

    fprintf(stderr,
            "Starting simulation -> server=%s:%d base=%.2fMbps motion=%.2fMbps keepalive=%ds pkt=%zuB motion_interval=%ds..%ds\n",
            server_ip, server_port, base_stream_mbps, motion_burst_mbps,
            keepalive_interval_s, packet_size, min_motion_interval_s, max_motion_interval_s);

    /* Accumulator approach: keep fractional packets between ticks */
    uint64_t last_send_ms = now_ms();
    double send_accumulator = 0.0;

    while (!stop) { // Main simulation loop
        uint64_t now = now_ms(); // Current time in ms

        /* Keepalive: send a small control/heartbeat periodically */
        if (now - last_keepalive_ms >= (uint64_t)keepalive_interval_s * 1000ULL) {
            udp_send(sock, &dst, keepalive_msg, sizeof(keepalive_msg) - 1); // Send keepalive packet
            last_keepalive_ms = now;
        }

        /* Motion event scheduling: when due, enter motion state and send metadata */
        if (!in_motion && now >= next_motion_ms) {
            in_motion = 1;                       // Enter motion state
            int dur = motion_duration_s();       // Determine random motion duration
            motion_end_ms = now + (uint64_t)dur * 1000ULL; // Set motion end timestamp
            next_motion_ms = motion_end_ms + (uint64_t)next_motion_interval_s(min_motion_interval_s, max_motion_interval_s) * 1000ULL;
            // Schedule next motion after this one ends

            char meta[256];                      // Buffer for motion metadata
            int n = snprintf(meta, sizeof(meta),
                             "{\"type\":\"motion_event\",\"start_ms\":%llu,\"duration_s\":%d}",
                             (unsigned long long)now, dur); // Format JSON
            if (n > 0) udp_send(sock, &dst, meta, (size_t)n); // Send motion metadata
            fprintf(stderr, "[event] motion start t=%llu dur=%ds\n", (unsigned long long)now, dur);
        }

        /* End motion when time elapses */
        if (in_motion && now >= motion_end_ms) {
            in_motion = 0; // Exit motion state
            fprintf(stderr, "[event] motion end t=%llu\n", (unsigned long long)now);
        }

        /* Determine current target packets-per-second */
        double target_pps = streaming_mode ? (in_motion ? motion_pps : base_pps) : 0.0;
        // Use motion PPS if in motion, else base PPS

        /* Throttle by elapsed time (ms) to calculate how many packets to send now */
        uint64_t elapsed_ms = now - last_send_ms;
        if (elapsed_ms == 0) {
            msleep(1); // Prevent division by zero and busy spin
            continue;
        }
        last_send_ms = now;

        double want_packets = (target_pps * (double)elapsed_ms) / 1000.0; // Fractional packets for this interval
        send_accumulator += want_packets;
        int to_send = (int)send_accumulator; // Number of whole packets to send
        send_accumulator -= to_send;          // Keep leftover fractional packets

        /* Send the computed number of packets */
        for (int i = 0; i < to_send && !stop; ++i) {
            uint32_t s = (uint32_t)seq++; // Packet sequence number
            /* simple header (sequence) + filler bytes for plausible payload */
            memcpy(payload, &s, sizeof(s)); // Copy sequence number at start
            for (size_t p = sizeof(s); p < packet_size; ++p) {
                payload[p] = (unsigned char)((s + p) & 0xFF); // Fill rest of packet deterministically
            }
            udp_send(sock, &dst, payload, packet_size); // Send UDP packet
        }

        /* Short sleep to avoid busy spin; loop will wake frequently to keep pacing */
        msleep(5);
    }

    free(payload); // Release allocated memory
    close(sock);   // Close UDP socket
    fprintf(stderr, "Simulation stopped cleanly.\n");
    return 0;
}
