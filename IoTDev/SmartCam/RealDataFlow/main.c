#define _POSIX_C_SOURCE 200809L  // Enable modern POSIX features for clock_gettime and nanosleep

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <time.h>
#include <string.h>
#include <errno.h>

#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <linux/videodev2.h>

/* ============================================================
   GLOBALS
   ============================================================ */

volatile sig_atomic_t stop_requested = 0;  // Flag set by signal handler to safely stop program

// Signal handler to catch CTRL+C and request a stop
static void handle_sigint(int sig)
{
    (void)sig;          // Unused parameter
    stop_requested = 1;  // Set stop flag
}

// Return current time in milliseconds
static uint64_t now_ms(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);  // Monotonic clock avoids jumps from system time changes
    return (uint64_t)t.tv_sec * 1000ULL +
           (uint64_t)t.tv_nsec / 1000000ULL;
}

// Sleep for a given number of milliseconds
static void msleep(uint64_t ms)
{
    struct timespec ts = { ms / 1000, (ms % 1000) * 1000000 };
    nanosleep(&ts, NULL);  // High-precision sleep
}

/* ============================================================
   UDP EVENT SENDER
   ============================================================ */

#define LABEL_PORT 9000           // Port to send lightweight event labels
#define SYNC_PORT  9001           // Port to send aggressive sync events
#define LABEL_DST  "10.0.0.1"    // Destination IP for UDP events

// Send a simple JSON label over UDP
static void send_label(const char *label)
{
    int sock = socket(AF_INET, SOCK_DGRAM, 0); // Create UDP socket

    struct sockaddr_in dst = {0};
    dst.sin_family = AF_INET;
    dst.sin_port   = htons(LABEL_PORT);
    inet_pton(AF_INET, LABEL_DST, &dst.sin_addr); // Convert IP string to binary

    char msg[128];
    snprintf(msg, sizeof(msg),
             "{\"event\":\"%s\",\"t_ms\":%llu}",  // Format JSON with label and timestamp
             label, (unsigned long long)now_ms());

    sendto(sock, msg, strlen(msg), 0, (struct sockaddr *)&dst, sizeof(dst)); // Send UDP packet

    close(sock); // Close socket
}

// Aggressive sync event: CPU + UDP burst to mark power activity
static void send_aggressive_sync(void)
{
    int sock = socket(AF_INET, SOCK_DGRAM, 0); // UDP socket for sync

    struct sockaddr_in dst = {0};
    dst.sin_family = AF_INET;
    dst.sin_port   = htons(SYNC_PORT);
    inet_pton(AF_INET, LABEL_DST, &dst.sin_addr);

    const char msg[] = "{\"event\":\"SYNC\"}";

    send_label("SYNC_START"); // Mark start of sync

    uint64_t end = now_ms() + 1500; // Run for 1.5 seconds

    while (now_ms() < end) {
        for (volatile int i = 0; i < 50000; i++); // Burn CPU cycles intentionally
        sendto(sock, msg, sizeof(msg) - 1, 0, (struct sockaddr *)&dst, sizeof(dst)); // Send sync packet
        msleep(5); // Small delay to control activity
    }

    send_label("SYNC_END"); // Mark end of sync

    close(sock); // Close socket
}

/* ============================================================
   CAMERA (V4L2)
   ============================================================ */

#define CAMERA_DEVICE "/dev/video0" // Default camera device
#define CAMERA_BUFFERS 4            // Number of memory-mapped buffers
#define VIDEO_FILE "/tmp/capture.raw"

struct cam_buf { void *addr; size_t len; }; // Represents a memory-mapped buffer

static int cam_fd = -1;                    // File descriptor for camera device
static struct cam_buf buffers[CAMERA_BUFFERS]; // Array of camera buffers

// Initialize camera for capture
static void camera_init(void)
{
    cam_fd = open(CAMERA_DEVICE, O_RDWR); // Open camera device
    if (cam_fd < 0) exit(1);             // Exit if cannot open

    struct v4l2_format fmt = {0};
    fmt.type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
    fmt.fmt.pix.width = 640;
    fmt.fmt.pix.height = 480;
    fmt.fmt.pix.pixelformat = V4L2_PIX_FMT_YUYV;
    ioctl(cam_fd, VIDIOC_S_FMT, &fmt);   // Set video format

    struct v4l2_requestbuffers req = {0};
    req.count = CAMERA_BUFFERS;
    req.type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
    req.memory = V4L2_MEMORY_MMAP;
    ioctl(cam_fd, VIDIOC_REQBUFS, &req); // Request buffers

    for (int i = 0; i < CAMERA_BUFFERS; i++) {
        struct v4l2_buffer buf = {0};
        buf.type = req.type;
        buf.memory = req.memory;
        buf.index = i;
        ioctl(cam_fd, VIDIOC_QUERYBUF, &buf); // Query buffer info

        buffers[i].len = buf.length;
        buffers[i].addr = mmap(NULL, buf.length,
                               PROT_READ | PROT_WRITE,
                               MAP_SHARED, cam_fd, buf.m.offset); // Map buffer to memory
        ioctl(cam_fd, VIDIOC_QBUF, &buf); // Queue buffer for capture
    }

    enum v4l2_buf_type type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
    ioctl(cam_fd, VIDIOC_STREAMON, &type); // Start streaming
}

// Shutdown camera and clean up
static void camera_shutdown(void)
{
    enum v4l2_buf_type type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
    ioctl(cam_fd, VIDIOC_STREAMOFF, &type); // Stop streaming

    for (int i = 0; i < CAMERA_BUFFERS; i++)
        munmap(buffers[i].addr, buffers[i].len); // Unmap memory

    close(cam_fd); // Close device
}

/* ============================================================
   TCP UPLOAD
   ============================================================ */

// Upload file over TCP with event labels
static void upload_file(const char *path)
{
    send_label("UPLOAD_START"); // Mark start of upload

    int fd = open(path, O_RDONLY);
    if (fd < 0) return; // Exit if file cannot be opened

    int sock = socket(AF_INET, SOCK_STREAM, 0); // TCP socket

    struct sockaddr_in dst = {0};
    dst.sin_family = AF_INET;
    dst.sin_port = htons(10000);
    inet_pton(AF_INET, "10.0.0.1", &dst.sin_addr); // Destination server

    if (connect(sock, (struct sockaddr *)&dst, sizeof(dst)) < 0) goto out; // Exit on failure

    char buf[2048];
    ssize_t n;

    while ((n = read(fd, buf, sizeof(buf))) > 0) { // Read file chunks
        send(sock, buf, n, 0); // Send over TCP
        msleep(2 + rand() % 5); // Add small jitter for realistic traffic
    }

out:
    close(fd);   // Close file
    close(sock); // Close socket

    send_label("UPLOAD_END"); // Mark end of upload
}

/* ============================================================
   MAIN
   ============================================================ */

int main(void)
{
    signal(SIGINT, handle_sigint); // Handle CTRL+C
    srand(time(NULL));             // Seed random numbers

    msleep(2000);                  // Wait 2 seconds before starting
    send_aggressive_sync();        // Initial aggressive sync
    msleep(2000);                  // Wait 2 seconds

    uint64_t next_sync = now_ms() + (30 + rand() % 10) * 60 * 1000; // Next periodic sync 30–40 min later

    while (!stop_requested) {

        msleep((10 + rand() % 30) * 1000); // Random idle 10–30s

        send_label("CAPTURE_START");       // Mark capture start
        camera_init();                     // Initialize camera

        int out = open(VIDEO_FILE, O_CREAT | O_TRUNC | O_WRONLY, 0644);
        uint64_t end = now_ms() + (3000 + rand() % 4000); // Capture 3–7s

        while (now_ms() < end && !stop_requested) {
            struct v4l2_buffer buf = {0};
            buf.type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
            buf.memory = V4L2_MEMORY_MMAP;
            ioctl(cam_fd, VIDIOC_DQBUF, &buf); // Dequeue frame
            write(out, buffers[buf.index].addr, buf.bytesused); // Write frame to file
            ioctl(cam_fd, VIDIOC_QBUF, &buf); // Requeue buffer
        }

        close(out);            // Close file
        camera_shutdown();     // Shutdown camera
        send_label("CAPTURE_END");// Mark capture end

        upload_file(VIDEO_FILE); // Upload captured file
        unlink(VIDEO_FILE);      // Delete file

        if (now_ms() > next_sync) { // Periodic sync
            msleep(3000);
            send_aggressive_sync();
            msleep(3000);
            next_sync = now_ms() + (30 + rand() % 10) * 60 * 1000; // Schedule next
        }
    }

    return 0;
}
