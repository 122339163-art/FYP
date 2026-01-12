/*
 * RB3 Gen 2 Smart Camera – Capture + Backup Simulator
 *
 * WHAT THIS PROGRAM IS:
 * --------------------
 * This program acts as a real smart security camera.
 *
 * It turns on the camera hardware inside the RB3 device,
 * captures real video frames, stores them temporarily, and then
 * sends those frames to my computer as if they were being
 * backed up to the cloud.
 *
 * WHY THIS EXISTS:
 * ----------------
 * The goal is to create realistic electrical power usage and
 * realistic network traffic patterns so that those patterns
 * can later be studied and used to train intrusion-detection
 * or anomaly-detection systems.
 *
 * NETWORK TRAFFIC POLICY:
 * -----------------------
 * - UDP is used ONLY for short synchronization markers
 * - TCP is used ONLY for backing up stored video
 *
 * There is NO background streaming and NO constant chatter.
 */

#define _POSIX_C_SOURCE 200809L

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <time.h>
#include <string.h>
#include <errno.h>

/*
 * These headers give access to low-level operating system features:
 * - Talking to hardware
 * - Mapping memory
 * - Sending data over the network
 */
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <arpa/inet.h>

/*
 * This header defines how Linux cameras are controlled.
 * It is the standard interface used by almost all Linux cameras.
 */
#include <linux/videodev2.h>

/* ============================================================
   GLOBAL SHUTDOWN FLAG
   ============================================================ */

/*
 * This variable acts like a shared “stop sign” for the program.
 *
 * When the user presses Ctrl+C, this flag is set.
 * Every loop checks it and exits cleanly when requested.
 *
 * This prevents corrupted files, stuck hardware, or crashes.
 */
volatile sig_atomic_t stop_requested = 0;

/*
 * This function runs automatically when Ctrl+C is pressed.
 * It does not stop the program immediately.
 * It simply requests a clean shutdown.
 */
static void handle_sigint(int sig)
{
    (void)sig;
    stop_requested = 1;
}

/* ============================================================
   TIME HELPERS
   ============================================================ */

/*
 * Returns the current time in milliseconds.
 *
 * IMPORTANT:
 * This uses a special clock that only moves forward.
 * It cannot jump backward if the system time changes.
 *
 * This makes it safe for measuring durations.
 */
static uint64_t now_ms(void)
{
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (uint64_t)t.tv_sec * 1000ULL +
           (uint64_t)t.tv_nsec / 1000000ULL;
}

/*
 * Pauses the program for a specific number of milliseconds.
 *
 * This prevents the program from running too fast and
 * consuming unnecessary CPU power.
 */
static void msleep(uint64_t ms)
{
    struct timespec ts = { ms / 1000, (ms % 1000) * 1000000 };
    nanosleep(&ts, NULL);
}

/* ============================================================
   UDP SYNC MARKER
   ============================================================ */

/*
 * This function sends a short burst of UDP packets.
 *
 * WHY THIS EXISTS:
 * ----------------
 * When collecting power measurements and network traffic,
 * we need a clear way to line up the two timelines.
 *
 * This UDP burst acts like a movie clapperboard:
 * - It produces a visible spike in network traffic
 * - It also produces a visible spike in power usage
 *
 * By finding this spike in both datasets, we can align them.
 *
 * UDP is used here because:
 * - It is lightweight
 * - We do not care if packets are lost
 * - The burst itself is the signal
 */
static void send_sync_marker(void)
{
    int sock = socket(AF_INET, SOCK_DGRAM, 0);

    struct sockaddr_in dst = {0};
    dst.sin_family = AF_INET;
    dst.sin_port   = htons(9000);
    inet_pton(AF_INET, "192.0.2.1", &dst.sin_addr);

    const char msg[] = "{\"type\":\"SYNC\"}";

    /*
     * The burst lasts exactly 3 seconds.
     * During this time we send packets as fast as possible.
     */
    uint64_t end = now_ms() + 3000;

    while (now_ms() < end) {
        sendto(sock, msg, sizeof(msg) - 1, 0,
               (struct sockaddr *)&dst, sizeof(dst));
    }

    close(sock);
}

/* ============================================================
   CAMERA (V4L2)
   ============================================================ */

/*
 * This is the Linux device file representing the camera.
 * On the RB3 Gen 2, the built-in camera usually appears here.
 */
#define CAMERA_DEVICE "/dev/video0"

/*
 * These buffers are shared between the camera hardware and
 * the program. The camera writes frames into them using DMA.
 */
#define CAMERA_BUFFERS 4

/*
 * Temporary file where captured video is stored.
 * This mimics local storage before cloud backup.
 */
#define VIDEO_FILE "/tmp/capture.raw"

/*
 * Each buffer represents a chunk of memory that holds
 * one captured video frame.
 */
struct cam_buf {
    void *addr;
    size_t len;
};

static int cam_fd;
static struct cam_buf buffers[CAMERA_BUFFERS];

/*
 * Initializes the camera hardware.
 *
 * WHAT THIS DOES INTERNALLY:
 * -------------------------
 * - Powers up the camera sensor
 * - Configures resolution and format
 * - Allocates memory for video frames
 * - Starts the camera streaming
 *
 * This step alone causes noticeable power activity.
 */
static void camera_init(void)
{
    cam_fd = open(CAMERA_DEVICE, O_RDWR);
    if (cam_fd < 0) {
        perror("camera");
        exit(1);
    }

    /*
     * Define the video format.
     * 640x480 is chosen for stability and predictability.
     */
    struct v4l2_format fmt = {0};
    fmt.type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
    fmt.fmt.pix.width = 640;
    fmt.fmt.pix.height = 480;
    fmt.fmt.pix.pixelformat = V4L2_PIX_FMT_YUYV;

    ioctl(cam_fd, VIDIOC_S_FMT, &fmt);

    /*
     * Request memory buffers from the kernel.
     * These will be shared with the camera hardware.
     */
    struct v4l2_requestbuffers req = {0};
    req.count = CAMERA_BUFFERS;
    req.type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
    req.memory = V4L2_MEMORY_MMAP;

    ioctl(cam_fd, VIDIOC_REQBUFS, &req);

    /*
     * Map each buffer into the program’s address space.
     */
    for (int i = 0; i < CAMERA_BUFFERS; i++) {
        struct v4l2_buffer buf = {0};
        buf.type = req.type;
        buf.memory = req.memory;
        buf.index = i;

        ioctl(cam_fd, VIDIOC_QUERYBUF, &buf);

        buffers[i].len = buf.length;
        buffers[i].addr = mmap(NULL, buf.length,
                               PROT_READ | PROT_WRITE,
                               MAP_SHARED, cam_fd, buf.m.offset);

        /*
         * Give the buffer to the camera so it can be filled.
         */
        ioctl(cam_fd, VIDIOC_QBUF, &buf);
    }

    /*
     * Start video capture.
     * From this point on, the camera is active.
     */
    enum v4l2_buf_type type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
    ioctl(cam_fd, VIDIOC_STREAMON, &type);
}

/*
 * Shuts the camera down cleanly.
 *
 * This ensures:
 * - Hardware is powered down properly
 * - Memory is released
 * - The system is left in a clean state
 */
static void camera_shutdown(void)
{
    enum v4l2_buf_type type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
    ioctl(cam_fd, VIDIOC_STREAMOFF, &type);

    for (int i = 0; i < CAMERA_BUFFERS; i++)
        munmap(buffers[i].addr, buffers[i].len);

    close(cam_fd);
}

/* ============================================================
   TCP BACKUP UPLOAD
   ============================================================ */

/*
 * Sends the captured video file to another computer.
 *
 * WHY TCP IS USED:
 * ----------------
 * This represents realistic backup behavior:
 * - Data must arrive intact
 * - Order matters
 * - Retries are automatic
 *
 * This mirrors how real cameras upload footage.
 */
static void upload_file(const char *path)
{
    int fd = open(path, O_RDONLY);
    if (fd < 0) return;

    int sock = socket(AF_INET, SOCK_STREAM, 0);

    struct sockaddr_in dst = {0};
    dst.sin_family = AF_INET;
    dst.sin_port = htons(10000);
    inet_pton(AF_INET, "192.0.2.2", &dst.sin_addr);

    connect(sock, (struct sockaddr *)&dst, sizeof(dst));

    /*
     * Read the file in chunks and send it.
     * This creates sustained network traffic.
     */
    char buf[4096];
    ssize_t n;

    while ((n = read(fd, buf, sizeof(buf))) > 0)
        send(sock, buf, n, 0);

    close(fd);
    close(sock);
}

/* ============================================================
   MAIN PROGRAM
   ============================================================ */

int main(void)
{
    signal(SIGINT, handle_sigint);
    srand(time(NULL));

    /*
     * Initial synchronization marker.
     * Used to align datasets from the very start.
     */
    send_sync_marker();

    /*
     * Turn on and configure the camera.
     */
    camera_init();

    /*
     * Main experiment loop.
     * This repeats until the user stops the program.
     */
    while (!stop_requested) {

        /*
         * Idle period.
         * This simulates times when no motion is detected.
         */
        msleep((10 + rand() % 30) * 1000);

        /*
         * Capture phase.
         * Video is recorded locally for a fixed duration.
         */
        int out = open(VIDEO_FILE, O_CREAT | O_TRUNC | O_WRONLY, 0644);
        uint64_t end = now_ms() + 5000;

        while (now_ms() < end && !stop_requested) {

            struct v4l2_buffer buf = {0};
            buf.type = V4L2_BUF_TYPE_VIDEO_CAPTURE;
            buf.memory = V4L2_MEMORY_MMAP;

            /*
             * Retrieve one video frame from the camera.
             */
            ioctl(cam_fd, VIDIOC_DQBUF, &buf);

            /*
             * Store the raw frame to disk.
             * This mimics local storage behavior.
             */
            write(out, buffers[buf.index].addr, buf.bytesused);

            /*
             * Give the buffer back to the camera.
             */
            ioctl(cam_fd, VIDIOC_QBUF, &buf);
        }

        close(out);

        /*
         * Backup phase.
         * The stored video is uploaded to another computer.
         */
        upload_file(VIDEO_FILE);

        /*
         * Remove the local copy after backup.
         */
        unlink(VIDEO_FILE);

        /*
         * Optional re-synchronization marker.
         * Useful for long-running experiments.
         */
        send_sync_marker();
    }

    /*
     * Shut everything down cleanly.
     */
    camera_shutdown();
    return 0;
}
