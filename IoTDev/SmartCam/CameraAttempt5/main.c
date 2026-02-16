#define _POSIX_C_SOURCE 200809L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <signal.h>
#include <errno.h>
#include <stdint.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <sys/wait.h>

#define SYNC_PORT_OFFSET 1
#define OUTPUT_DIR "/home/root/temp"

static volatile sig_atomic_t keep_running = 1;

/* ------------------- Utility ------------------- */

static uint64_t htobe64_u(uint64_t host_64) {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    return ((uint64_t)htonl(host_64 & 0xFFFFFFFF) << 32) | htonl(host_64 >> 32);
#else
    return host_64;
#endif
}

void handle_signal(int sig) {
    (void)sig;
    keep_running = 0;
}

void get_iso_timestamp(char *buffer, size_t len) {
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

int rand_range(int min, int max) {
    if (min >= max)
        return min;
    return min + rand() % (max - min + 1);
}

/* ------------------- UDP JSON ------------------- */

int send_udp_json(const char *host_ip, int port, const char *json) {
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (sock < 0)
        return -1;

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

    if (inet_pton(AF_INET, host_ip, &addr.sin_addr) != 1) {
        close(sock);
        return -1;
    }

    ssize_t sent = sendto(sock, json, strlen(json), 0,
                          (struct sockaddr *)&addr, sizeof(addr));

    close(sock);
    return (sent < 0) ? -1 : 0;
}

void send_label(const char *host_ip, int port, const char *event) {
    char ts[64];
    char json[256];

    get_iso_timestamp(ts, sizeof(ts));

    snprintf(json, sizeof(json),
             "{ \"type\":\"LABEL\", \"event\":\"%s\", \"timestamp\":\"%s\" }\n",
             event, ts);

    send_udp_json(host_ip, port, json);
}

/* ------------------- Camera Capture ------------------- */

int capture_video(int duration_sec, const char *filename) {
    int camera_index = 1;

    pid_t pid = fork();
    if (pid < 0) {
        perror("fork failed");
        return -1;
    }

    if (pid == 0) {
        /* Child process */
        execlp("gst-multi-camera-example",
               "gst-multi-camera-example",
               "-o", "1",
               (char *)NULL);

        perror("execlp failed");
        exit(EXIT_FAILURE);
    }

    /* Parent */
    printf("Camera running for %d seconds...\n", duration_sec);
    sleep(duration_sec);

    printf("Stopping camera (SIGINT)...\n");
    kill(pid, SIGINT);

    int status;
    waitpid(pid, &status, 0);

    if (!WIFEXITED(status)) {
        fprintf(stderr, "Camera process did not exit cleanly\n");
        return -1;
    }

    /* Copy output file */
    char copy_cmd[512];
    snprintf(copy_cmd, sizeof(copy_cmd),
             "cp /opt/cam%d_vid.mp4 %s",
             camera_index, filename);

    if (system(copy_cmd) != 0) {
        fprintf(stderr, "Failed to copy video file\n");
        return -1;
    }

    return 0;
}

/* ------------------- File Upload ------------------- */

int upload_file(const char *host_ip, int port, const char *filename) {
    struct stat st;
    if (stat(filename, &st) != 0)
        return -1;

    FILE *fp = fopen(filename, "rb");
    if (!fp)
        return -1;

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0)
        goto fail;

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

    if (inet_pton(AF_INET, host_ip, &addr.sin_addr) != 1)
        goto fail;

    if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0)
        goto fail;

    uint64_t size_net = htobe64_u((uint64_t)st.st_size);
    uint16_t name_len = htons(strlen(filename));

    send(sock, &size_net, sizeof(size_net), 0);
    send(sock, &name_len, sizeof(name_len), 0);
    send(sock, filename, strlen(filename), 0);

    char buf[4096];
    size_t n;

    while ((n = fread(buf, 1, sizeof(buf), fp)) > 0) {
        send(sock, buf, n, 0);
    }

    fclose(fp);
    close(sock);
    return 0;

fail:
    if (sock >= 0)
        close(sock);
    fclose(fp);
    return -1;
}

/* ------------------- Main ------------------- */

int main(int argc, char *argv[]) {

    if (argc != 7) {
        fprintf(stderr,
                "Usage: %s <host_ip> <host_port> <idle_min_m> <idle_max_m> <cap_min_s> <cap_max_s>\n",
                argv[0]);
        return 1;
    }

    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);

    const char *host_ip = argv[1];
    int port = (int)strtol(argv[2], NULL, 10);
    int sync_port = port + SYNC_PORT_OFFSET;

    int idle_min = (int)strtol(argv[3], NULL, 10);
    int idle_max = (int)strtol(argv[4], NULL, 10);
    int cap_min  = (int)strtol(argv[5], NULL, 10);
    int cap_max  = (int)strtol(argv[6], NULL, 10);

    srand(time(NULL));

    send_label(host_ip, sync_port, "START_SYNC");

    while (keep_running) {

        int idle_time = rand_range(idle_min, idle_max) * 60;
        printf("Idling for %d seconds...\n", idle_time);
        sleep(idle_time);

        char filename[256];
        snprintf(filename, sizeof(filename),
                 OUTPUT_DIR "/capture_%ld.mp4", time(NULL));

        int cap_time = rand_range(cap_min, cap_max);

        send_label(host_ip, sync_port, "CAMERA_START");

        if (capture_video(cap_time, filename) != 0) {
            send_label(host_ip, sync_port, "CAMERA_FAILED");
            continue;
        }

        send_label(host_ip, sync_port, "CAMERA_END");

        send_label(host_ip, sync_port, "UPLOAD_START");
        upload_file(host_ip, port, filename);
        send_label(host_ip, sync_port, "UPLOAD_END");
    }

    send_label(host_ip, sync_port, "SHUTDOWN");
    return 0;
}
