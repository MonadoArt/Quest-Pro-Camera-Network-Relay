#define _GNU_SOURCE
#include <arpa/inet.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <pthread.h>
#include <stdarg.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include "turbojpeg.h"

#define WIDTH 2000
#define HEIGHT 400
#define PANEL 400
#define PIXELS ((size_t)WIDTH * HEIGHT)
#define HEADER 80
#define BYTES (HEADER + PIXELS)
#define STREAMS 9
#define MAX_CLIENTS 16
#define LEASE_NS UINT64_C(2000000000)
#define VERSION "0.4.0"
#define LOG_LIMIT (1024 * 1024)

typedef struct Image {
    unsigned char *data;
    size_t length;
    unsigned refs;
    uint64_t serial;
    uint64_t timestamp_ns;
} Image;
typedef struct {
    Image *latest;
    unsigned subscribers;
    uint64_t encodes;
    double encode_ms;
    uint64_t jpeg_bytes;
} Stream;
typedef struct {
    int fd;
    char ip[INET_ADDRSTRLEN];
} Client;

static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t changed = PTHREAD_COND_INITIALIZER;
static Stream streams[STREAMS];
static volatile sig_atomic_t stopping;
static unsigned char *shared;
static uint64_t started_ns, frames_read, last_sequence;
static uint64_t lease_start_ns, lease_active_since_ns, newest_frame_timestamp_ns;
static uint64_t last_lease_written, stale_skipped, dropped_frames;
static double age_sum_ms, age_max_ms;
static uint64_t age_samples;
typedef struct {
    uint64_t at_ns, frames_read, stale_skipped, dropped_frames, age_samples;
    uint64_t encodes[STREAMS], jpeg_bytes[STREAMS];
    double age_sum_ms, age_max_ms, encode_ms[STREAMS];
    struct rusage usage;
} Snapshot;
static Snapshot baseline;
static struct rusage start_usage;
static unsigned active_clients;
static int listen_fd = -1;
static int test_pattern, quality = 85, max_fps = 72, max_fps_explicit, port = 27280, run_as = 2000;
static int log_fd = -1;
static const char *bind_address = "0.0.0.0";
static const char *shared_path = "/data/local/tmp/questpro-live-v8-shared.bin";
static const char *pid_path = "/data/local/tmp/qpro-camd.pid";
static const char *log_path = "/data/local/tmp/qpro-camd.log";
static const int stream_first_camera[STREAMS] = { 0, 1, 2, 3, 4, 0, 0, 2, 2 };
static const int stream_width[STREAMS] = { PANEL, PANEL, PANEL, PANEL, PANEL, WIDTH, 800, 1200, 800 };

static uint64_t now_ns(void) {
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (uint64_t)t.tv_sec * 1000000000u + (uint64_t)t.tv_nsec;
}
static double cpu_seconds(const struct rusage *usage) {
    return usage->ru_utime.tv_sec + usage->ru_utime.tv_usec / 1e6 +
           usage->ru_stime.tv_sec + usage->ru_stime.tv_usec / 1e6;
}
static void deadline_after(struct timespec *deadline, uint64_t ns) {
    clock_gettime(CLOCK_REALTIME, deadline);
    deadline->tv_sec += (time_t)(ns / 1000000000ULL);
    deadline->tv_nsec += (long)(ns % 1000000000ULL);
    if (deadline->tv_nsec >= 1000000000L) { ++deadline->tv_sec; deadline->tv_nsec -= 1000000000L; }
}
static void snapshot(Snapshot *s, uint64_t now) {
    s->at_ns = now;
    s->frames_read = frames_read;
    s->stale_skipped = stale_skipped;
    s->dropped_frames = dropped_frames;
    s->age_samples = age_samples;
    s->age_sum_ms = age_sum_ms;
    s->age_max_ms = age_max_ms;
    for (int i = 0; i < STREAMS; ++i) {
        s->encodes[i] = streams[i].encodes;
        s->jpeg_bytes[i] = streams[i].jpeg_bytes;
        s->encode_ms[i] = streams[i].encode_ms;
    }
    getrusage(RUSAGE_SELF, &s->usage);
}
static int read_sysfs(const char *path, long *value) {
    FILE *f = fopen(path, "r");
    if (!f) return 0;
    int ok = fscanf(f, "%ld", value) == 1;
    fclose(f);
    return ok;
}
static long rss_kb(void) {
    FILE *f = fopen("/proc/self/statm", "r");
    unsigned long total, resident;
    if (!f) return 0;
    int ok = fscanf(f, "%lu %lu", &total, &resident) == 2;
    fclose(f);
    long page = sysconf(_SC_PAGESIZE);
    return ok && page > 0 ? (long)(resident * (unsigned long)page / 1024) : 0;
}
static void append(char *body, size_t size, size_t *used, const char *format, ...) {
    if (*used >= size) return;
    va_list args;
    va_start(args, format);
    int n = vsnprintf(body + *used, size - *used, format, args);
    va_end(args);
    if (n > 0) *used += (size_t)n;
}
static void append_sysfs(char *body, size_t size, size_t *used, const char *name, const char *path) {
    long value;
    if (read_sysfs(path, &value)) append(body, size, used, "\"%s\":%ld", name, value);
    else append(body, size, used, "\"%s\":null", name);
}
static void nap(long ns) {
    struct timespec t = { ns / 1000000000L, ns % 1000000000L };
    nanosleep(&t, NULL);
}
static void lease(uint64_t until) {
    if (!shared) return;
    uint64_t *value = (uint64_t *)(void *)(shared + 64);
    uint64_t current = __atomic_load_n(value, __ATOMIC_ACQUIRE);
    if (!until) {
        while (current == last_lease_written && current != 0) {
            if (__atomic_compare_exchange_n(value, &current, 0, 0, __ATOMIC_RELEASE, __ATOMIC_ACQUIRE)) {
                last_lease_written = 0;
                return;
            }
        }
        last_lease_written = 0;
        return;
    }
    for (;;) {
        uint64_t desired = current > until ? current : until;
        if (desired == current) return;
        if (__atomic_compare_exchange_n(value, &current, desired, 0, __ATOMIC_RELEASE, __ATOMIC_ACQUIRE)) {
            last_lease_written = desired;
            return;
        }
    }
}
static void on_signal(int number) {
    (void)number;
    stopping = 1;
    lease(0);
}
static int process_alive(pid_t pid) {
    char path[64], line[256];
    snprintf(path, sizeof(path), "/proc/%ld/stat", (long)pid);
    FILE *file = fopen(path, "r");
    if (!file) return kill(pid, 0) == 0;
    int alive = 1;
    if (fgets(line, sizeof(line), file)) {
        char *end = strrchr(line, ')');
        if (end && end[1] == ' ' && (end[2] == 'Z' || end[2] == 'X')) alive = 0;
    }
    fclose(file);
    return alive;
}
static void put32(unsigned char *p, size_t offset, uint32_t value) { memcpy(p + offset, &value, 4); }
static void image_release(Image *image) {
    if (image && --image->refs == 0) {
        tj3Free(image->data);
        free(image);
    }
}
static int parse_number(const char *value, int low, int high, int *out) {
    char *end;
    long v;
    if (!*value) return 0;
    errno = 0;
    v = strtol(value, &end, 10);
    if (errno || *end || v < low || v > high) return 0;
    *out = (int)v;
    return 1;
}
static int args(int argc, char **argv, int *daemonize, int *stop) {
    for (int i = 1; i < argc; ++i) {
        const char *arg = argv[i];
        if (!strcmp(arg, "--daemonize")) *daemonize = 1;
        else if (!strcmp(arg, "--stop")) *stop = 1;
        else if (!strcmp(arg, "--test-pattern")) test_pattern = 1;
        else if (!strcmp(arg, "--version")) { puts("qpro-camd " VERSION); exit(0); }
        else if (i + 1 >= argc) return 0;
        else if (!strcmp(arg, "--port")) { if (!parse_number(argv[++i], 1, 65535, &port)) return 0; }
        else if (!strcmp(arg, "--max-fps")) { if (!parse_number(argv[++i], 0, 120, &max_fps)) return 0; max_fps_explicit = 1; }
        else if (!strcmp(arg, "--quality")) { if (!parse_number(argv[++i], 1, 100, &quality)) return 0; }
        else if (!strcmp(arg, "--bind")) bind_address = argv[++i];
        else if (!strcmp(arg, "--shared-path")) shared_path = argv[++i];
        else if (!strcmp(arg, "--pid-path")) pid_path = argv[++i];
        else if (!strcmp(arg, "--log")) log_path = argv[++i];
        else if (!strcmp(arg, "--run-as")) { if (!parse_number(argv[++i], 0, 2147483647, &run_as)) return 0; }
        else return 0;
    }
    return 1;
}
static int stop_previous(int require_running) {
    FILE *file = fopen(pid_path, "r");
    long value = 0;
    if (file) { if (fscanf(file, "%ld", &value) != 1) value = 0; fclose(file); }
    if (value > 1 && value != (long)getpid()) {
        char path[64], command[256];
        snprintf(path, sizeof(path), "/proc/%ld/cmdline", value);
        int fd = open(path, O_RDONLY | O_CLOEXEC);
        ssize_t n = fd < 0 ? -1 : read(fd, command, sizeof(command) - 1);
        if (fd >= 0) close(fd);
        if (n > 0) {
            command[n] = 0;
            const char *base = strrchr(command, '/');
            base = base ? base + 1 : command;
            if (!strcmp(base, "qpro-camd") || !strcmp(base, "libqprocamd.so")) {
                if (kill((pid_t)value, SIGTERM) != 0) return -1;
                for (int i = 0; i < 100; ++i) {
                    if (!process_alive((pid_t)value)) break;
                    nap(50000000);
                }
                if (process_alive((pid_t)value)) return -1;
                return 1;
            }
        }
    }
    return require_running ? 0 : 1;
}
static void warn_relay(void) {
    DIR *dir = opendir("/proc");
    if (!dir) return;
    struct dirent *entry;
    while ((entry = readdir(dir))) {
        if (strspn(entry->d_name, "0123456789") != strlen(entry->d_name)) continue;
        char path[512], command[256];
        snprintf(path, sizeof(path), "/proc/%s/cmdline", entry->d_name);
        int fd = open(path, O_RDONLY | O_CLOEXEC);
        if (fd < 0) continue;
        ssize_t n = read(fd, command, sizeof(command) - 1);
        close(fd);
        if (n <= 0) continue;
        command[n] = 0;
        const char *base = strrchr(command, '/');
        base = base ? base + 1 : command;
        if (!strncmp(base, "questpro-camera-relay", 21)) {
            fprintf(stderr, "WARNING questpro-camera-relay running pid=%s\n", entry->d_name);
            break;
        }
    }
    closedir(dir);
}
static int open_shared(void) {
    int fd = open(shared_path, O_CREAT | O_RDWR | O_CLOEXEC, 0666);
    if (fd < 0) return 0;
    struct stat s;
    if (fstat(fd, &s) != 0 || (s.st_size != (off_t)BYTES && ftruncate(fd, (off_t)BYTES) != 0) || fchmod(fd, 0666) != 0) {
        close(fd); return 0;
    }
    shared = mmap(NULL, BYTES, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (shared == MAP_FAILED) { shared = NULL; return 0; }
    memcpy(shared, "QPSHARV7", 8);
    put32(shared, 32, WIDTH); put32(shared, 36, HEIGHT); put32(shared, 40, WIDTH);
    put32(shared, 44, 1); put32(shared, 48, PIXELS); put32(shared, 52, 0x1f);
    uint32_t existing_fps = __atomic_load_n((uint32_t *)(void *)(shared + 72), __ATOMIC_ACQUIRE);
    if (max_fps_explicit || existing_fps == 0) put32(shared, 72, (uint32_t)max_fps);
    else fprintf(stderr, "MAX_FPS using_shared=%u\n", existing_fps);
    lease(0);
    return 1;
}
static int copy_frame(unsigned char *frame, uint32_t *last_gen, uint64_t *sequence, uint64_t *timestamp) {
    const uint32_t *gen = (const uint32_t *)(const void *)(shared + 8);
    uint32_t before = __atomic_load_n(gen, __ATOMIC_ACQUIRE);
    if ((before & 1u) || before == *last_gen) return 0;
    memcpy(sequence, shared + 16, 8);
    memcpy(timestamp, shared + 24, 8);
    memcpy(frame, shared + HEADER, PIXELS);
    uint32_t after = __atomic_load_n(gen, __ATOMIC_ACQUIRE);
    if (before != after || (after & 1u)) return 0;
    *last_gen = after;
    return 1;
}
static void make_pattern(unsigned char *frame, uint64_t sequence) {
    for (int y = 0; y < HEIGHT; ++y)
        for (int x = 0; x < WIDTH; ++x) {
            int panel = x / PANEL, local = x % PANEL;
            frame[(size_t)y * WIDTH + x] = (unsigned char)((local >= (int)(sequence * 7 % 400) &&
                local < (int)(sequence * 7 % 400) + 20) ? 255 : 25 + panel * 42 + (y / 20) % 20);
        }
}
static void *reader_main(void *unused) {
    (void)unused;
    unsigned char *frame = malloc(PIXELS);
    tjhandle encoder = tj3Init(TJINIT_COMPRESS);
    if (!frame || !encoder || tj3Set(encoder, TJPARAM_QUALITY, quality) < 0 ||
        tj3Set(encoder, TJPARAM_SUBSAMP, TJSAMP_GRAY) < 0) {
        fprintf(stderr, "READER_INIT_FAILED\n"); stopping = 1; free(frame);
        if (encoder) tj3Destroy(encoder);
        return NULL;
    }
    uint32_t last_gen = 0;
    uint64_t synthetic_sequence = 0, last_pattern = 0, last_lease = 0, last_stats = started_ns, last_stats_emit = started_ns;
    uint64_t previous_timestamp = 0;
    char last_idle_stats[1024] = "";
    double period_ns = 0;
    while (!stopping) {
        uint64_t now = now_ns();
        unsigned subscribers[STREAMS], total = 0;
        uint64_t lease_start;
        pthread_mutex_lock(&lock);
        for (int i = 0; i < STREAMS; ++i) { subscribers[i] = streams[i].subscribers; total += subscribers[i]; }
        lease_start = lease_start_ns;
        if (shared && (now - last_lease >= 250000000 || !last_lease)) {
            lease(total ? now + LEASE_NS : 0);
            last_lease = now;
        }
        pthread_mutex_unlock(&lock);
        if (now - last_stats >= 10000000000ULL) {
            uint64_t torn = shared ? __atomic_load_n((uint64_t *)(void *)(shared + 56), __ATOMIC_ACQUIRE) : 0;
            pthread_mutex_lock(&lock);
            Snapshot current;
            snapshot(&current, now);
            double seconds = (double)(now - baseline.at_ns) / 1e9;
            double cpu = 100.0 * (cpu_seconds(&current.usage) - cpu_seconds(&baseline.usage)) / seconds;
            uint64_t output_frames = 0, output_bytes = 0;
            double encode_ms = 0;
            for (int i = 0; i < STREAMS; ++i) {
                output_frames += current.encodes[i] - baseline.encodes[i];
                output_bytes += current.jpeg_bytes[i] - baseline.jpeg_bytes[i];
                encode_ms += current.encode_ms[i] - baseline.encode_ms[i];
            }
            char stats_line[1024];
            snprintf(stats_line, sizeof(stats_line), "STATS clients=%u frames=%llu sequence=%llu torn=%llu cpu=%.2f%% rss_kb=%ld read_fps=%.2f stale=%llu dropped=%llu age_ms=%.2f/%.2f out_fps=%.2f jpeg_bytes=%.1f bytes_s=%.0f encode_ms=%.2f voluntary_cs_s=%.2f involuntary_cs_s=%.2f",
                   total, (unsigned long long)frames_read, (unsigned long long)last_sequence,
                   (unsigned long long)torn, cpu, rss_kb(),
                   (double)(current.frames_read - baseline.frames_read) / seconds,
                   (unsigned long long)(current.stale_skipped - baseline.stale_skipped),
                   (unsigned long long)(current.dropped_frames - baseline.dropped_frames),
                   current.age_samples > baseline.age_samples ? (current.age_sum_ms - baseline.age_sum_ms) / (current.age_samples - baseline.age_samples) : 0,
                   current.age_max_ms, (double)output_frames / seconds,
                   output_frames ? (double)output_bytes / output_frames : 0,
                   (double)output_bytes / seconds,
                   output_frames ? encode_ms / output_frames : 0,
                   (double)(current.usage.ru_nvcsw - baseline.usage.ru_nvcsw) / seconds,
                   (double)(current.usage.ru_nivcsw - baseline.usage.ru_nivcsw) / seconds);
            if (total || strcmp(stats_line, last_idle_stats) || now - last_stats_emit >= 60000000000ULL) {
                printf("%s\n", stats_line);
                if (!total) { snprintf(last_idle_stats, sizeof(last_idle_stats), "%s", stats_line); last_stats_emit = now; }
            }
            if (log_fd >= 0) {
                struct stat log_stat;
                if (fstat(log_fd, &log_stat) == 0 && log_stat.st_size > LOG_LIMIT && ftruncate(log_fd, 0) != 0)
                    fprintf(stderr, "LOG_TRUNCATE_FAILED error=%s\n", strerror(errno));
            }
            baseline = current;
            age_max_ms = 0;
            pthread_mutex_unlock(&lock);
            last_stats = now;
        }
        if (!total) {
            pthread_mutex_lock(&lock);
            unsigned active = 0;
            for (int i = 0; i < STREAMS; ++i) active += streams[i].subscribers;
            if (!active && !stopping) {
                struct timespec deadline;
                uint64_t remaining = last_stats + 10000000000ULL - now_ns();
                deadline_after(&deadline, remaining);
                pthread_cond_timedwait(&changed, &lock, &deadline);
            }
            pthread_mutex_unlock(&lock);
            previous_timestamp = 0;
            period_ns = 0;
            continue;
        }
        uint64_t sequence = 0, timestamp = 0;
        if (test_pattern) {
            if (max_fps && now - last_pattern < 1000000000ULL / (unsigned)max_fps) {
                uint64_t delay = last_pattern + 1000000000ULL / (unsigned)max_fps - now;
                nap((long)(delay > 250000000 ? 250000000 : delay)); continue;
            }
            sequence = ++synthetic_sequence; last_pattern = now; timestamp = now; make_pattern(frame, sequence);
        } else if (!copy_frame(frame, &last_gen, &sequence, &timestamp)) {
            uint64_t delay = 1000000;
            if (period_ns > 0 && previous_timestamp) {
                uint64_t expected = previous_timestamp + (uint64_t)period_ns;
                /* Check by the earliest allowed frame too, so an abrupt FPS increase
                   cannot hide a new frame behind the slower EWMA prediction. */
                uint64_t earliest = previous_timestamp + 1000000000ULL / (unsigned)(max_fps ? max_fps : 120);
                if (earliest < expected) expected = earliest;
                if (expected > now + 2000000) delay = expected - now - 1000000;
                if (delay > 250000000) delay = 250000000;
            }
            nap((long)delay); continue;
        }
        pthread_mutex_lock(&lock);
        if (!test_pattern && (timestamp < lease_start_ns || lease_start != lease_start_ns)) {
            ++stale_skipped;
            pthread_mutex_unlock(&lock);
            continue;
        }
        if (!test_pattern && previous_timestamp && timestamp > previous_timestamp) {
            uint64_t interval = timestamp - previous_timestamp;
            if (interval >= 4000000 && interval <= 250000000)
                period_ns = period_ns ? period_ns * 0.75 + (double)interval * 0.25 : (double)interval;
        }
        if (!test_pattern) previous_timestamp = timestamp;
        newest_frame_timestamp_ns = timestamp;
        if (last_sequence && sequence > last_sequence + 1) dropped_frames += sequence - last_sequence - 1;
        ++frames_read; last_sequence = sequence;
        pthread_mutex_unlock(&lock);
        int age_recorded = 0;
        for (int i = 0; i < STREAMS; ++i) {
            if (!subscribers[i]) continue;
            int width = stream_width[i];
            const unsigned char *input = frame + (size_t)stream_first_camera[i] * PANEL;
            unsigned char *jpeg = NULL;
            size_t length = 0;
            uint64_t begin = now_ns();
            if (!age_recorded) {
                double age = begin >= timestamp ? (double)(begin - timestamp) / 1e6 : 0;
                pthread_mutex_lock(&lock);
                age_sum_ms += age; ++age_samples;
                if (age > age_max_ms) age_max_ms = age;
                pthread_mutex_unlock(&lock);
                age_recorded = 1;
            }
            if (tj3Compress8(encoder, input, width, WIDTH, HEIGHT, TJPF_GRAY, &jpeg, &length) != 0) {
                fprintf(stderr, "JPEG_FAILED stream=%d error=%s\n", i, tj3GetErrorStr(encoder));
                tj3Free(jpeg); continue;
            }
            Image *image = malloc(sizeof(*image));
            if (!image) { tj3Free(jpeg); continue; }
            *image = (Image){ jpeg, length, 1, sequence, timestamp };
            pthread_mutex_lock(&lock);
            if (lease_start != lease_start_ns || !streams[i].subscribers) {
                image_release(image); pthread_mutex_unlock(&lock); continue;
            }
            Image *old = streams[i].latest;
            streams[i].latest = image;
            ++streams[i].encodes;
            streams[i].jpeg_bytes += length;
            streams[i].encode_ms += (double)(now_ns() - begin) / 1000000.0;
            pthread_cond_broadcast(&changed);
            image_release(old);
            pthread_mutex_unlock(&lock);
        }
    }
    lease(0);
    tj3Destroy(encoder); free(frame);
    return NULL;
}
static int send_all(int fd, const void *data, size_t length) {
    const unsigned char *p = data;
    while (length) {
        ssize_t n = send(fd, p, length, MSG_NOSIGNAL);
        if (n > 0) { p += n; length -= (size_t)n; }
        else if (n < 0 && errno == EINTR) continue;
        else return 0;
    }
    return 1;
}
static int reply(int fd, int code, const char *label, const char *type, const char *body) {
    char h[512];
    size_t length = strlen(body);
    int n = snprintf(h, sizeof(h), "HTTP/1.0 %d %s\r\nContent-Type: %s\r\nContent-Length: %zu\r\nCache-Control: no-cache\r\n\r\n", code, label, type, length);
    return n > 0 && (size_t)n < sizeof(h) && send_all(fd, h, (size_t)n) && send_all(fd, body, length);
}
static int disconnected(int fd) {
    char c;
    ssize_t n = recv(fd, &c, 1, MSG_PEEK | MSG_DONTWAIT);
    return n == 0 || (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR);
}
static void status_reply(int fd) {
    char body[8192];
    size_t used = 0;
    pthread_mutex_lock(&lock);
    uint64_t now = now_ns();
    Snapshot current;
    snapshot(&current, now);
    double seconds = (double)(now - baseline.at_ns) / 1e9;
    if (seconds <= 0) seconds = 1e-9;
    double uptime = (double)(now - started_ns) / 1e9;
    uint64_t torn = shared ? __atomic_load_n((uint64_t *)(void *)(shared + 56), __ATOMIC_ACQUIRE) : 0;
    unsigned total_clients = 0;
    for (int i = 0; i < STREAMS; ++i) total_clients += streams[i].subscribers;
    const char *source_state = test_pattern ? "test_pattern" : !total_clients ? "idle" :
        (lease_active_since_ns && now - lease_active_since_ns > 2000000000ULL &&
         (!newest_frame_timestamp_ns || newest_frame_timestamp_ns < lease_active_since_ns || now - newest_frame_timestamp_ns > 2000000000ULL)
         ? "waiting_for_frames" : "ok");
    append(body, sizeof(body), &used, "{\"version\":\"%s\",\"uptime\":%.3f,\"clients\":[",
        VERSION, uptime);
    for (int i = 0; i < STREAMS; ++i)
        append(body, sizeof(body), &used, "%s%u", i ? "," : "", streams[i].subscribers);
    append(body, sizeof(body), &used, "],\"frames_read\":%llu,\"last_sequence\":%llu,\"torn_count\":%llu,\"lease_active\":%s,\"mean_encode_ms\":[",
        (unsigned long long)frames_read, (unsigned long long)last_sequence,
        (unsigned long long)torn,
        shared && __atomic_load_n((uint64_t *)(void *)(shared + 64), __ATOMIC_ACQUIRE) > now ? "true" : "false");
    for (int i = 0; i < STREAMS; ++i)
        append(body, sizeof(body), &used, "%s%.3f", i ? "," : "", streams[i].encodes ? streams[i].encode_ms / streams[i].encodes : 0);
    append(body, sizeof(body), &used, "],\"max_fps\":%d,\"quality\":%d,", max_fps, quality);
    append(body, sizeof(body), &used, "\"process\":{\"cpu_percent\":%.3f,\"cpu_percent_since_start\":%.3f,\"rss_kb\":%ld,\"voluntary_context_switches_per_second\":%.3f,\"involuntary_context_switches_per_second\":%.3f},",
        100.0 * (cpu_seconds(&current.usage) - cpu_seconds(&baseline.usage)) / seconds,
        uptime > 0 ? 100.0 * (cpu_seconds(&current.usage) - cpu_seconds(&start_usage)) / uptime : 0,
        rss_kb(),
        (double)(current.usage.ru_nvcsw - baseline.usage.ru_nvcsw) / seconds,
        (double)(current.usage.ru_nivcsw - baseline.usage.ru_nivcsw) / seconds);
    append(body, sizeof(body), &used, "\"input\":{\"frames_read_per_second\":%.3f,\"mean_frame_age_ms\":%.3f,\"max_frame_age_ms\":%.3f,\"dropped_frames\":%llu,\"stale_frames_skipped\":%llu},\"streams\":[",
        (double)(current.frames_read - baseline.frames_read) / seconds,
        current.age_samples > baseline.age_samples ? (current.age_sum_ms - baseline.age_sum_ms) / (current.age_samples - baseline.age_samples) : 0,
        age_max_ms, (unsigned long long)dropped_frames, (unsigned long long)stale_skipped);
    for (int i = 0; i < STREAMS; ++i) {
        uint64_t count = current.encodes[i] - baseline.encodes[i];
        append(body, sizeof(body), &used, "%s{\"output_frames_per_second\":%.3f,\"mean_jpeg_bytes\":%.3f,\"bytes_per_second\":%.3f,\"mean_encode_ms\":%.3f}",
            i ? "," : "", (double)count / seconds,
            count ? (double)(current.jpeg_bytes[i] - baseline.jpeg_bytes[i]) / count : 0,
            (double)(current.jpeg_bytes[i] - baseline.jpeg_bytes[i]) / seconds,
            count ? (current.encode_ms[i] - baseline.encode_ms[i]) / count : 0);
    }
    append(body, sizeof(body), &used, "],\"source\":{\"state\":\"%s\",\"last_frame_age_ms\":", source_state);
    if (newest_frame_timestamp_ns) append(body, sizeof(body), &used, "%.3f", now >= newest_frame_timestamp_ns ? (double)(now - newest_frame_timestamp_ns) / 1e6 : 0);
    else append(body, sizeof(body), &used, "null");
    append(body, sizeof(body), &used, "},\"device\":{");
    pthread_mutex_unlock(&lock);
    append_sysfs(body, sizeof(body), &used, "current_now", "/sys/class/power_supply/battery/current_now");
    append(body, sizeof(body), &used, ",");
    append_sysfs(body, sizeof(body), &used, "voltage_now", "/sys/class/power_supply/battery/voltage_now");
    append(body, sizeof(body), &used, ",");
    append_sysfs(body, sizeof(body), &used, "capacity", "/sys/class/power_supply/battery/capacity");
    append(body, sizeof(body), &used, ",");
    append_sysfs(body, sizeof(body), &used, "temp", "/sys/class/power_supply/battery/temp");
    append(body, sizeof(body), &used, ",\"thermal_zone_temps\":[");
    int found = 0;
    for (int i = 0; i < 64 && found < 4; ++i) {
        char path[128];
        long value;
        snprintf(path, sizeof(path), "/sys/class/thermal/thermal_zone%d/temp", i);
        if (read_sysfs(path, &value)) {
            append(body, sizeof(body), &used, "%s{\"zone\":%d,\"temp\":%ld}", found ? "," : "", i, value);
            ++found;
        }
    }
    if (!found) append(body, sizeof(body), &used, "null");
    append(body, sizeof(body), &used, "]}}\n");
    if (used < sizeof(body)) reply(fd, 200, "OK", "application/json", body);
}
static void *client_main(void *pointer) {
    Client *client = pointer;
    int fd = client->fd, stream = -1, single = 0;
    char request[2048], method[16], path[256];
    ssize_t got = recv(fd, request, sizeof(request) - 1, 0);
    if (got <= 0) goto done;
    request[got] = 0;
    if (sscanf(request, "%15s %255s", method, path) != 2 || strcmp(method, "GET")) {
        reply(fd, 405, "Method Not Allowed", "text/plain", "GET only\n"); goto done;
    }
    if (!strcmp(path, "/status")) { status_reply(fd); goto done; }
    if (!strcmp(path, "/")) {
        reply(fd, 200, "OK", "text/html", "<!doctype html><title>qpro-camd</title><h1>qpro-camd</h1><a href='/status'>status</a><br><a href='/strip.mjpg'>strip</a><br><a href='/eyes.mjpg'>eyes</a><br><a href='/face.mjpg'>face</a><br><a href='/mouth.mjpg'>mouth</a><br><a href='/camera0.mjpg'>camera0 MJPEG</a> <a href='/camera0.jpg'>JPEG</a><br><a href='/camera1.mjpg'>camera1 MJPEG</a> <a href='/camera1.jpg'>JPEG</a><br><a href='/camera2.mjpg'>camera2 MJPEG</a> <a href='/camera2.jpg'>JPEG</a><br><a href='/camera3.mjpg'>camera3 MJPEG</a> <a href='/camera3.jpg'>JPEG</a><br><a href='/camera4.mjpg'>camera4 MJPEG</a> <a href='/camera4.jpg'>JPEG</a>");
        goto done;
    }
    if (!strcmp(path, "/strip.mjpg")) stream = 5;
    else if (!strcmp(path, "/eyes.mjpg")) stream = 6;
    else if (!strcmp(path, "/face.mjpg")) stream = 7;
    else if (!strcmp(path, "/mouth.mjpg")) stream = 8;
    else if (strlen(path) == 13 && !strncmp(path, "/camera", 7) && path[7] >= '0' && path[7] <= '4' && !strcmp(path + 8, ".mjpg")) stream = path[7] - '0';
    else if (strlen(path) == 12 && !strncmp(path, "/camera", 7) && path[7] >= '0' && path[7] <= '4' && !strcmp(path + 8, ".jpg")) { stream = path[7] - '0'; single = 1; }
    if (stream < 0) { reply(fd, 404, "Not Found", "text/plain", "Not found\n"); goto done; }
    pthread_mutex_lock(&lock);
    unsigned existing = 0;
    for (int i = 0; i < STREAMS; ++i) existing += streams[i].subscribers;
    if (!existing) {
        lease_start_ns = now_ns();
        lease_active_since_ns = lease_start_ns;
        for (int i = 0; i < STREAMS; ++i) {
            image_release(streams[i].latest);
            streams[i].latest = NULL;
        }
    }
    if (!streams[stream].subscribers) {
        image_release(streams[stream].latest);
        streams[stream].latest = NULL;
    }
    ++streams[stream].subscribers;
    if (shared) lease(now_ns() + LEASE_NS);
    pthread_cond_broadcast(&changed);
    pthread_mutex_unlock(&lock);
    const char *mjpeg_header = "HTTP/1.0 200 OK\r\nContent-Type: multipart/x-mixed-replace; boundary=frame\r\nCache-Control: no-cache\r\n\r\n";
    if (!single && !send_all(fd, mjpeg_header, strlen(mjpeg_header))) goto unsubscribe;
    uint64_t last = 0;
    const uint64_t single_deadline = now_ns() + 1000000000ULL;
    for (;;) {
        Image *image = NULL;
        pthread_mutex_lock(&lock);
        if (streams[stream].latest && streams[stream].latest->serial != last) {
            image = streams[stream].latest;
            ++image->refs;
            last = image->serial;
        } else {
            struct timespec deadline;
            clock_gettime(CLOCK_REALTIME, &deadline);
            deadline.tv_nsec += 250000000;
            if (deadline.tv_nsec >= 1000000000L) { ++deadline.tv_sec; deadline.tv_nsec -= 1000000000L; }
            pthread_cond_timedwait(&changed, &lock, &deadline);
        }
        pthread_mutex_unlock(&lock);
        if (!image) {
            if (stopping || (single ? now_ns() >= single_deadline : disconnected(fd))) break;
            continue;
        }
        int ok;
        if (single) {
            char header[256];
            int n = snprintf(header, sizeof(header), "HTTP/1.0 200 OK\r\nContent-Type: image/jpeg\r\nContent-Length: %zu\r\nCache-Control: no-cache\r\n\r\n", image->length);
            ok = n > 0 && (size_t)n < sizeof(header) && send_all(fd, header, (size_t)n) && send_all(fd, image->data, image->length);
        } else {
            char header[256];
            int n = snprintf(header, sizeof(header), "--frame\r\nContent-Type: image/jpeg\r\nContent-Length: %zu\r\nX-Sequence: %llu\r\nX-Timestamp-Ns: %llu\r\n\r\n", image->length,
                (unsigned long long)image->serial, (unsigned long long)image->timestamp_ns);
            ok = n > 0 && (size_t)n < sizeof(header) && send_all(fd, header, (size_t)n) &&
                send_all(fd, image->data, image->length) && send_all(fd, "\r\n", 2);
        }
        pthread_mutex_lock(&lock); image_release(image); pthread_mutex_unlock(&lock);
        if (!ok || single || stopping) break;
    }
    if (single && !last) reply(fd, 504, "Gateway Timeout", "text/plain", "No frame\n");
unsubscribe:
    pthread_mutex_lock(&lock);
    --streams[stream].subscribers;
    unsigned total = 0;
    for (int i = 0; i < STREAMS; ++i) total += streams[i].subscribers;
    if (!total) lease(0);
    pthread_mutex_unlock(&lock);
done:
    printf("CLIENT_DISCONNECTED ip=%s stream=%d\n", client->ip, stream);
    close(fd);
    pthread_mutex_lock(&lock); --active_clients; pthread_mutex_unlock(&lock);
    free(client);
    return NULL;
}
int main(int argc, char **argv) {
    setvbuf(stdout, NULL, _IOLBF, 0);
    int daemonize = 0, stop = 0;
    if (!args(argc, argv, &daemonize, &stop)) {
        fprintf(stderr, "Usage: qpro-camd [--port N] [--bind IPv4] [--max-fps 0..120] [--quality 1..100] [--shared-path PATH] [--pid-path PATH] [--daemonize] [--log PATH] [--stop] [--test-pattern]\n"); return 1;
    }
    if (stop) {
        int result = stop_previous(1);
        if (result < 0) { perror("stop"); return 1; }
        printf("STOPPED running=%s lease=0\n", result ? "yes" : "no");
        return 0;
    }
    if (stop_previous(0) < 0) { perror("stop previous"); return 1; }
    warn_relay();
    if (daemonize) {
        pid_t child = fork();
        if (child < 0) { perror("fork"); return 1; }
        if (child > 0) return 0;
        if (setsid() < 0) { perror("setsid"); return 1; }
        int input = open("/dev/null", O_RDONLY);
        if (input >= 0) { dup2(input, STDIN_FILENO); close(input); }
        int nullout = open("/dev/null", O_WRONLY);
        if (nullout >= 0) { dup2(nullout, STDOUT_FILENO); dup2(nullout, STDERR_FILENO); close(nullout); }
    }
    signal(SIGPIPE, SIG_IGN); signal(SIGTERM, on_signal); signal(SIGINT, on_signal);
    if (!test_pattern && !open_shared()) { perror("shared file"); return 1; }
    listen_fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (listen_fd < 0) { perror("socket"); return 1; }
    int one = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    struct sockaddr_in address = {0};
    address.sin_family = AF_INET; address.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, bind_address, &address.sin_addr) != 1) { fprintf(stderr, "Invalid bind address\n"); return 1; }
    if (bind(listen_fd, (struct sockaddr *)&address, sizeof(address)) != 0 || listen(listen_fd, 32) != 0) { perror("bind/listen"); return 1; }
    FILE *pidfile = fopen(pid_path, "w");
    if (!pidfile) { perror("pid file"); return 1; }
    fprintf(pidfile, "%ld\n", (long)getpid()); fclose(pidfile);
    if (daemonize) {
        log_fd = open(log_path, O_CREAT | O_WRONLY | O_APPEND | O_TRUNC, 0666);
        if (log_fd < 0) { perror("log"); return 1; }
        if (dup2(log_fd, STDOUT_FILENO) < 0 || dup2(log_fd, STDERR_FILENO) < 0) {
            perror("redirect log"); return 1;
        }
    }
    if (getuid() == 0 && run_as != 0) {
        if (setgroups(0, NULL) != 0 || setgid((gid_t)run_as) != 0 || setuid((uid_t)run_as) != 0 ||
            getuid() != (uid_t)run_as || geteuid() != (uid_t)run_as) {
            fprintf(stderr, "PRIVILEGE_DROP_FAILED uid=%d errno=%d\n", run_as, errno);
            return 1;
        }
    }
    started_ns = now_ns();
    snapshot(&baseline, started_ns);
    start_usage = baseline.usage;
    pthread_t reader;
    if (pthread_create(&reader, NULL, reader_main, NULL) != 0) { fprintf(stderr, "reader thread failed\n"); return 1; }
    printf("LISTENING version=%s address=%s port=%d pid=%ld\n", VERSION, bind_address, port, (long)getpid());
    while (!stopping) {
        struct pollfd pending = { listen_fd, POLLIN, 0 };
        int ready = poll(&pending, 1, 250);
        if (ready < 0) { if (errno == EINTR) continue; perror("poll"); break; }
        if (!ready || !(pending.revents & POLLIN)) continue;
        struct sockaddr_in peer; socklen_t size = sizeof(peer);
        int fd = accept(listen_fd, (struct sockaddr *)&peer, &size);
        if (fd < 0) { if (errno == EINTR || stopping || errno == EBADF) continue; perror("accept"); break; }
        struct timeval timeout = {2, 0};
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
        pthread_mutex_lock(&lock);
        if (active_clients >= MAX_CLIENTS) { pthread_mutex_unlock(&lock); reply(fd, 503, "Service Unavailable", "text/plain", "Too many clients\n"); close(fd); continue; }
        ++active_clients; pthread_mutex_unlock(&lock);
        Client *client = calloc(1, sizeof(*client));
        if (!client) { close(fd); pthread_mutex_lock(&lock); --active_clients; pthread_mutex_unlock(&lock); continue; }
        client->fd = fd;
        inet_ntop(AF_INET, &peer.sin_addr, client->ip, sizeof(client->ip));
        printf("CLIENT_CONNECTED ip=%s\n", client->ip);
        pthread_t thread;
        if (pthread_create(&thread, NULL, client_main, client) != 0) {
            close(fd); free(client); pthread_mutex_lock(&lock); --active_clients; pthread_mutex_unlock(&lock);
        } else pthread_detach(thread);
    }
    stopping = 1;
    close(listen_fd);
    pthread_cond_broadcast(&changed);
    pthread_join(reader, NULL);
    for (int i = 0; i < 100; ++i) {
        pthread_mutex_lock(&lock);
        unsigned clients = active_clients;
        pthread_mutex_unlock(&lock);
        if (!clients) break;
        nap(50000000);
    }
    lease(0);
    if (shared) munmap(shared, BYTES);
    unlink(pid_path);
    return 0;
}
