#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define WIDTH 2000
#define HEIGHT 400
#define HEADER 80
#define BYTES (HEADER + (size_t)WIDTH * HEIGHT)
static volatile sig_atomic_t running = 1;
static volatile sig_atomic_t publishing = 1;
static void stop(int sig) { (void)sig; running = 0; }
static void toggle_publish(int sig) { (void)sig; publishing = !publishing; }
static uint64_t now_ns(void) {
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (uint64_t)t.tv_sec * 1000000000u + (uint64_t)t.tv_nsec;
}
static void publish(unsigned char *shared, uint64_t sequence, uint64_t timestamp) {
    uint32_t *gen = (uint32_t *)(void *)(shared + 8);
    uint32_t value = __atomic_load_n(gen, __ATOMIC_RELAXED);
    if (value & 1u) ++value;
    __atomic_store_n(gen, value + 1u, __ATOMIC_RELEASE);
    memcpy(shared + 16, &sequence, 8);
    memcpy(shared + 24, &timestamp, 8);
    for (int y = 0; y < HEIGHT; ++y)
        for (int x = 0; x < WIDTH; ++x) {
            int panel = x / 400, local = x % 400;
            shared[HEADER + (size_t)y * WIDTH + x] = (unsigned char)
                ((local >= (int)(sequence * 7 % 400) && local < (int)(sequence * 7 % 400) + 20) ?
                255 : 25 + panel * 42 + (y / 20) % 20);
        }
    __atomic_store_n(gen, value + 2u, __ATOMIC_RELEASE);
}
int main(int argc, char **argv) {
    if (argc != 3 && (argc != 4 || strcmp(argv[3], "--prepublish"))) {
        fprintf(stderr, "Usage: fake_streamer SHARED_PATH FPS [--prepublish]\n"); return 1;
    }
    char *end;
    long fps = strtol(argv[2], &end, 10);
    if (!*argv[2] || *end || fps < 1 || fps > 120) return 1;
    signal(SIGTERM, stop); signal(SIGINT, stop); signal(SIGUSR1, toggle_publish);
    setvbuf(stdout, NULL, _IOLBF, 0);
    unsigned char *shared = NULL;
    while (running && !shared) {
        int fd = open(argv[1], O_RDWR | O_CLOEXEC);
        if (fd >= 0) {
            struct stat s;
            if (fstat(fd, &s) == 0 && s.st_size >= (off_t)BYTES) {
                void *p = mmap(NULL, BYTES, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
                if (p != MAP_FAILED) shared = p;
            }
            close(fd);
        }
        if (!shared) usleep(100000);
    }
    if (!shared) return 0;
    printf("SHARED_READY\n");
    uint64_t sequence = 0;
    if (argc == 4) {
        uint64_t now = now_ns();
        publish(shared, ++sequence, now > UINT64_C(3600000000000) ? now - UINT64_C(3600000000000) : 1);
        printf("PREPUBLISHED sequence=%llu\n", (unsigned long long)sequence);
    }
    int was_active = 0;
    while (running) {
        uint64_t now = now_ns();
        int active = __atomic_load_n((uint64_t *)(void *)(shared + 64), __ATOMIC_ACQUIRE) > now;
        if (active != was_active) { printf("LEASE_%s at=%llu\n", active ? "ACTIVE" : "IDLE", (unsigned long long)now); was_active = active; }
        if (active && publishing) {
            publish(shared, ++sequence, now);
        }
        usleep((useconds_t)(1000000 / fps));
    }
    munmap(shared, BYTES);
    return 0;
}
