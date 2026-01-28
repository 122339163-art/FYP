/*
 *  BUILD INSTRUCTIONS (ARM / AARCH64)
 *  ---------------------------------
 *  If cross-compiling from an x86_64 Ubuntu host:
 *
 *      aarch64-linux-gnu-gcc -O2 -Wall -o iot_cam_emulator main.c
 *
 *  Alternatively, compile natively on the RB3:
 *
 *      gcc -O2 -Wall -o iot_cam_emulator main.c
 *
 *
 *  RUN INSTRUCTIONS
 *  ----------------
 *  ./iot_cam_emulator <host_ip> <host_port> \
 *                     <idle_min_minutes> <idle_max_minutes> \
 *                     <capture_min_seconds> <capture_max_seconds>
 *
 *  Example:
 *      ./iot_cam_emulator 192.168.10.1 9000 1 5 3 10
 *
 */

#define _POSIX_C_SOURCE 200809L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <signal.h>
#include <errno.h>
#include <sys/socket.h>
#include <arpa/inet.h>

#define SYNC_PORT_OFFSET 1

// Utility: Generate ISO-8601 UTC timestamp with millisecond precision
void get_iso_timestamp(char *buffer, size_t len)
{
    struct timespec ts;
    struct tm tm;

    clock_gettime(CLOCK_REALTIME, &ts);
    gmtime_r(&ts.tv_sec, &tm);

    snprintf(buffer, len,
             "%04d-%02d-%02dT%02d:%02d:%02d.%03ldZ",
             tm.tm_year + 1900,
             tm.tm_mon + 1,
             tm.tm_mday,
             tm.tm_hour,
             tm.tm_min,
             tm.tm_sec,
             ts.tv_nsec / 1000000);
}

// Send UDP JSON packet (sync + labels)
int send_udp_json(const char *host_ip, int port, const char *json)
{
    int sock;
    struct sockaddr_in addr;

    sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0)
        return -1;

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, host_ip, &addr.sin_addr);

    sendto(sock, json, strlen(json), 0,
           (struct sockaddr *)&addr, sizeof(addr));

    close(sock);
    return 0;
}

// Send START_SYNC JSON packet (UDP)
int send_start_sync(const char *host_ip, int sync_port)
{
    char timestamp[64];
    char json[256];

    get_iso_timestamp(timestamp, sizeof(timestamp));

    snprintf(json, sizeof(json),
             "{ \"type\": \"START_SYNC\", "
             "\"timestamp\": \"%s\", "
             "\"device\": \"RB3_Gen2\" }\n",
             timestamp);

    return send_udp_json(host_ip, sync_port, json);
}

// Send LABEL JSON packet (UDP)
void send_label(const char *host_ip, int sync_port, const char *event)
{
    char timestamp[64];
    char json[256];

    get_iso_timestamp(timestamp, sizeof(timestamp));

    snprintf(json, sizeof(json),
             "{ \"type\": \"LABEL\", "
             "\"event\": \"%s\", "
             "\"timestamp\": \"%s\", "
             "\"device\": \"RB3_Gen2\" }\n",
             event, timestamp);

    send_udp_json(host_ip, sync_port, json);
}

// Generate a random integer within an inclusive range
int rand_range(int min, int max)
{
    return min + rand() % (max - min + 1);
}

// Capture video using GStreamer (V4L2 camera)
void capture_video(int duration_sec, const char *filename)
{
    char cmd[512];

    snprintf(cmd, sizeof(cmd),
        "gst-launch-1.0 -e "
        "v4l2src device=/dev/video0 ! "
        "video/x-raw,width=1280,height=720,framerate=30/1 ! "
        "x264enc tune=zerolatency ! "
        "mp4mux ! filesink location=%s",
        filename);

    alarm(duration_sec);
    system(cmd);
    alarm(0);
}

// Upload a file to the host over TCP
void upload_file(const char *host_ip, int port, const char *filename)
{
    FILE *fp = fopen(filename, "rb");
    if (!fp)
        return;

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr;

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, host_ip, &addr.sin_addr);

    if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0)
    {
        fclose(fp);
        close(sock);
        return;
    }

    char buffer[4096];
    size_t bytes;

    while ((bytes = fread(buffer, 1, sizeof(buffer), fp)) > 0)
        send(sock, buffer, bytes, 0);

    fclose(fp);
    close(sock);
}

// Main
int main(int argc, char *argv[])
{
    if (argc != 7)
    {
        fprintf(stderr,
            "Usage: %s <host_ip> <host_port> "
            "<idle_min_minutes> <idle_max_minutes> "
            "<capture_min_seconds> <capture_max_seconds>\n",
            argv[0]);
        return 1;
    }

    const char *host_ip = argv[1];
    int port = atoi(argv[2]);
    int sync_port = port + SYNC_PORT_OFFSET;

    int idle_min = atoi(argv[3]);
    int idle_max = atoi(argv[4]);
    int cap_min = atoi(argv[5]);
    int cap_max = atoi(argv[6]);

    srand(time(NULL));

    send_start_sync(host_ip, sync_port);

    while (1)
    {
        int idle_minutes = rand_range(idle_min, idle_max);
        sleep(idle_minutes * 60);

        int capture_seconds = rand_range(cap_min, cap_max);
        char filename[128];

        snprintf(filename, sizeof(filename),
                 "/data/capture_%ld.mp4", time(NULL));

        send_label(host_ip, sync_port, "CAMERA_OPERATION_START");
        capture_video(capture_seconds, filename);
        send_label(host_ip, sync_port, "CAMERA_OPERATION_END");

        send_label(host_ip, sync_port, "BACKUP_OPERATION_START");
        upload_file(host_ip, port, filename);
        send_label(host_ip, sync_port, "BACKUP_OPERATION_END");
    }

    return 0;
}
