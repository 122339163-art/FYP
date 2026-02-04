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


#define SYNC_PORT_OFFSET 1
#define VIDEO_DEVICE "/dev/video0"
#define OUTPUT_DIR   "/home/root/temp"

static volatile sig_atomic_t keep_running = 1;

static uint64_t htobe64(uint64_t host_64) {
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    return ((uint64_t)htonl(host_64 & 0xFFFFFFFF) << 32) | htonl(host_64 >> 32);
#else
    return host_64;
#endif
}



void handle_signal(int sig) { (void)sig; keep_running = 0; }

void get_iso_timestamp(char *buffer, size_t len) {
    struct timespec ts;
    struct tm tm;
    clock_gettime(CLOCK_REALTIME, &ts);
    gmtime_r(&ts.tv_sec, &tm);
    snprintf(buffer, len, "%04d-%02d-%02dT%02d:%02d:%02d.%03ldZ",
             tm.tm_year+1900, tm.tm_mon+1, tm.tm_mday,
             tm.tm_hour, tm.tm_min, tm.tm_sec, ts.tv_nsec/1000000);
}

int send_udp_json(const char *host_ip, int port, const char *json) {
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    if(sock < 0) return -1;

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if(inet_pton(AF_INET, host_ip, &addr.sin_addr) != 1) { close(sock); return -1; }

    ssize_t sent = sendto(sock, json, strlen(json), 0, (struct sockaddr*)&addr, sizeof(addr));
    close(sock);
    return (sent < 0) ? -1 : 0;
}

void send_label(const char *host_ip, int port, const char *event) {
    char ts[64], json[256];
    get_iso_timestamp(ts, sizeof(ts));
    snprintf(json, sizeof(json), "{ \"type\":\"LABEL\", \"event\":\"%s\", \"timestamp\":\"%s\" }\n", event, ts);
    send_udp_json(host_ip, port, json);
}

int rand_range(int min, int max) { return (min>max)?min:(min + rand() % (max-min+1)); }

int capture_video(int duration_sec, const char *filename)
{

    int camera_index = 1;

    char cmd[512];
    snprintf(cmd, sizeof(cmd),
             "timeout %d gst-multi-camera-example -o %d && cp /opt/cam%d_vid.mp4 %s",
             duration_sec, camera_index, camera_index, filename);


    int ret = system(cmd);

    if (ret != 0) {
        fprintf(stderr, "Error: Video capture failed (camera %d)\n", camera_index);
        return -1;
    }

    return 0;
}

int upload_file(const char *host_ip, int port, const char *filename) {
    struct stat st;
    if(stat(filename, &st)!=0) return -1;
    FILE *fp = fopen(filename, "rb"); if(!fp) return -1;

    int sock = socket(AF_INET, SOCK_STREAM, 0); if(sock<0) goto fail;
    struct sockaddr_in addr = {0}; addr.sin_family = AF_INET; addr.sin_port = htons(port);
    if(inet_pton(AF_INET, host_ip, &addr.sin_addr)!=1) goto fail;
    if(connect(sock,(struct sockaddr*)&addr,sizeof(addr))<0) goto fail;

    /* ---- HEADER ---- */
    uint64_t size_net = htobe64((uint64_t)st.st_size);
    uint16_t name_len = htons(strlen(filename));
    send(sock,&size_net,sizeof(size_net),0);
    send(sock,&name_len,sizeof(name_len),0);
    send(sock,filename,strlen(filename),0);

    /* ---- PAYLOAD ---- */
    char buf[4096]; size_t n;
    while((n=fread(buf,1,sizeof(buf),fp))>0) send(sock,buf,n,0);

    fclose(fp); close(sock); return 0;

fail:
    if(sock>=0) close(sock); fclose(fp); return -1;
}

int main(int argc,char*argv[]) {
    if(argc!=7){
        fprintf(stderr,"Usage: %s <host_ip> <host_port> <idle_min_m> <idle_max_m> <cap_min_s> <cap_max_s>\n",argv[0]);
        return 1;
    }

    signal(SIGINT,handle_signal); signal(SIGTERM,handle_signal);

    const char *host_ip = argv[1]; int port = atoi(argv[2]); int sync_port = port+SYNC_PORT_OFFSET;
    int idle_min = atoi(argv[3]); int idle_max = atoi(argv[4]);
    int cap_min = atoi(argv[5]); int cap_max = atoi(argv[6]);
    srand(time(NULL));

    send_label(host_ip,sync_port,"START_SYNC");

    while(keep_running){
        sleep(rand_range(idle_min,idle_max)*60);

        char filename[256];
        snprintf(filename,sizeof(filename), OUTPUT_DIR "/capture_%ld.mp4", time(NULL));

        int cap_time = rand_range(cap_min,cap_max);

        send_label(host_ip,sync_port,"CAMERA_START");
        if(!capture_video(cap_time,filename)){
            send_label(host_ip,sync_port,"CAMERA_FAILED");
            continue;
        }
        send_label(host_ip,sync_port,"CAMERA_END");

        send_label(host_ip,sync_port,"UPLOAD_START");
        upload_file(host_ip,port,filename);
        send_label(host_ip,sync_port,"UPLOAD_END");
    }

    send_label(host_ip,sync_port,"SHUTDOWN");
    return 0;
}
