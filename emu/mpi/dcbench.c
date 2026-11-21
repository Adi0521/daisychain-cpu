/*
 * dcbench — MPI microbenchmarks for the daisychain cluster (PRD F6).
 *
 * One binary, built from the same source against the same Open MPI package on the
 * nodes (native diagnostic runs) and inside the MPI job image (the containerised path).
 * Rank 0 prints one JSON object per line; everything else goes to stderr.
 *
 *   dcbench info                       placement, library version, container identity
 *   dcbench pingpong  [--sizes ...]    rank 0 <-> rank 1 round trips; latency = RTT/2
 *   dcbench bw        [--sizes ...]    ranks 0..k-1 stream to k..2k-1 (k = size/2),
 *                                      all pairs at once, windowed non-blocking sends
 *   dcbench allreduce|alltoall|bcast [--sizes ...]
 *   dcbench layer     [--sizes ...] [--microbatches M] [--compute-us U]
 *                                      synthetic pipeline: rank r receives each
 *                                      micro-batch's activations from r-1, "computes"
 *                                      for U us, forwards to r+1
 *   dcbench fail      [--rank R] [--code C]   rank R exits with C right after MPI_Init
 *   dcbench sleep     [--seconds S]    every rank spins in MPI for S seconds (cleanup tests)
 *
 * --sizes is a comma list of message sizes in bytes (per destination for alltoall).
 * Collective times are the slowest rank's time per iteration, barrier-separated.
 */
#include <mpi.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static int rank, nprocs;

static const long DEFAULT_SIZES[] = {1, 8, 64, 512, 4096, 32768, 262144, 1048576, 4194304};

static int cmp_double(const void *a, const void *b) {
    double x = *(const double *)a, y = *(const double *)b;
    return (x > y) - (x < y);
}

static double median(double *v, int n) {
    qsort(v, n, sizeof *v, cmp_double);
    return n % 2 ? v[n / 2] : 0.5 * (v[n / 2 - 1] + v[n / 2]);
}

static double minimum(const double *v, int n) {
    double m = v[0];
    for (int i = 1; i < n; i++) if (v[i] < m) m = v[i];
    return m;
}

/* Enough iterations for a stable median without spending seconds on large messages. */
static int iters_for(long bytes, int max_iters) {
    long it = (64L << 20) / (bytes > 0 ? bytes : 1);
    if (it > max_iters) it = max_iters;
    if (it < 8) it = 8;
    return (int)it;
}

static void die(const char *msg) {
    if (rank == 0) fprintf(stderr, "dcbench: %s\n", msg);
    MPI_Abort(MPI_COMM_WORLD, 2);
}

static void read_first_line(const char *path, char *out, size_t n) {
    out[0] = '\0';
    FILE *f = fopen(path, "r");
    if (!f) return;
    if (fgets(out, (int)n, f)) out[strcspn(out, "\n")] = '\0';
    fclose(f);
}

/* ---- info ---------------------------------------------------------------------------- */

#define FIELD 256
struct rank_info {
    char host[FIELD];
    char image[64];   /* contents of /etc/daisychain-image, or "" outside the job image */
    char cgroup[FIELD];
    char os[64];
    int uid;
};

static void os_id(char *out, size_t n) {
    char line[FIELD], id[FIELD] = "", ver[FIELD] = "";
    FILE *f = fopen("/etc/os-release", "r");
    out[0] = '\0';
    if (!f) return;
    while (fgets(line, sizeof line, f)) {
        line[strcspn(line, "\n")] = '\0';
        if (!strncmp(line, "ID=", 3)) memcpy(id, line + 3, strlen(line + 3) + 1);
        if (!strncmp(line, "VERSION_ID=", 11)) memcpy(ver, line + 11, strlen(line + 11) + 1);
    }
    fclose(f);
    char *v = ver;
    if (*v == '"') { v++; v[strcspn(v, "\"")] = '\0'; }
    snprintf(out, n, "%.30s-%.30s", id, v);
}

static void json_str(const char *s) {
    putchar('"');
    for (; *s; s++) {
        if (*s == '"' || *s == '\\') putchar('\\');
        if ((unsigned char)*s >= 0x20) putchar(*s);
    }
    putchar('"');
}

static void cmd_info(void) {
    struct rank_info me;
    memset(&me, 0, sizeof me);
    int len;
    MPI_Get_processor_name(me.host, &len);
    read_first_line("/etc/daisychain-image", me.image, sizeof me.image);
    read_first_line("/proc/self/cgroup", me.cgroup, sizeof me.cgroup);
    os_id(me.os, sizeof me.os);
    me.uid = (int)getuid();

    struct rank_info *all = NULL;
    if (rank == 0) all = malloc(sizeof *all * nprocs);
    MPI_Gather(&me, sizeof me, MPI_BYTE, all, sizeof me, MPI_BYTE, 0, MPI_COMM_WORLD);
    if (rank != 0) return;

    char lib[MPI_MAX_LIBRARY_VERSION_STRING];
    MPI_Get_library_version(lib, &len);
    lib[strcspn(lib, "\n,")] = '\0';
    printf("{\"bench\":\"info\",\"ranks\":%d,\"library\":", nprocs);
    json_str(lib);
    printf(",\"per_rank\":[");
    for (int i = 0; i < nprocs; i++) {
        printf("%s{\"rank\":%d,\"host\":", i ? "," : "", i);
        json_str(all[i].host);
        printf(",\"image\":");
        json_str(all[i].image);
        printf(",\"os\":");
        json_str(all[i].os);
        printf(",\"uid\":%d,\"cgroup\":", all[i].uid);
        json_str(all[i].cgroup);
        putchar('}');
    }
    printf("]}\n");
    free(all);
}

/* ---- point to point ------------------------------------------------------------------- */

static void cmd_pingpong(const long *sizes, int ns, int max_iters) {
    if (nprocs < 2) die("pingpong needs 2 ranks");
    long maxb = 0;
    for (int s = 0; s < ns; s++) if (sizes[s] > maxb) maxb = sizes[s];
    char *buf = calloc(maxb ? maxb : 1, 1);
    for (int s = 0; s < ns; s++) {
        long b = sizes[s];
        int it = iters_for(b, max_iters), warm = it / 10 + 2;
        double *t = calloc(it, sizeof *t);
        MPI_Barrier(MPI_COMM_WORLD);
        for (int i = -warm; i < it; i++) {
            double t0 = MPI_Wtime();
            if (rank == 0) {
                MPI_Send(buf, (int)b, MPI_BYTE, 1, 1, MPI_COMM_WORLD);
                MPI_Recv(buf, (int)b, MPI_BYTE, 1, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            } else if (rank == 1) {
                MPI_Recv(buf, (int)b, MPI_BYTE, 0, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                MPI_Send(buf, (int)b, MPI_BYTE, 0, 1, MPI_COMM_WORLD);
            }
            if (i >= 0) t[i] = (MPI_Wtime() - t0) / 2;
        }
        if (rank == 0) {
            double mn = minimum(t, it), med = median(t, it);
            printf("{\"bench\":\"pingpong\",\"bytes\":%ld,\"iters\":%d,"
                   "\"lat_us_min\":%.2f,\"lat_us_p50\":%.2f,\"mbps_p50\":%.2f}\n",
                   b, it, mn * 1e6, med * 1e6, b * 8 / med / 1e6);
            fflush(stdout);
        }
        free(t);
    }
    free(buf);
}

#define WINDOW 16

static void cmd_bw(const long *sizes, int ns, int max_iters) {
    if (nprocs < 2 || nprocs % 2) die("bw needs an even number of ranks");
    int k = nprocs / 2, sender = rank < k, peer = sender ? rank + k : rank - k;
    long maxb = 0;
    for (int s = 0; s < ns; s++) if (sizes[s] > maxb) maxb = sizes[s];
    char *buf = calloc((size_t)(maxb ? maxb : 1) * WINDOW, 1);
    MPI_Request req[WINDOW];
    char ack = 0;
    double t_start = 0;
    for (int s = 0; s < ns; s++) {
        long b = sizes[s];
        int it = iters_for(b * WINDOW, max_iters) / 4 + 2;
        for (int i = -2; i < it; i++) {
            if (i == 0) {
                /* Common start: the aggregate is total bytes over the slowest pair's time,
                   so pairs that finish early cannot inflate it. */
                MPI_Barrier(MPI_COMM_WORLD);
                t_start = MPI_Wtime();
            }
            for (int w = 0; w < WINDOW; w++) {
                char *p = buf + (size_t)w * b;
                if (sender) MPI_Isend(p, (int)b, MPI_BYTE, peer, 2, MPI_COMM_WORLD, &req[w]);
                else MPI_Irecv(p, (int)b, MPI_BYTE, peer, 2, MPI_COMM_WORLD, &req[w]);
            }
            MPI_Waitall(WINDOW, req, MPI_STATUSES_IGNORE);
            if (sender) MPI_Recv(&ack, 1, MPI_BYTE, peer, 3, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            else MPI_Send(&ack, 1, MPI_BYTE, peer, 3, MPI_COMM_WORLD);
        }
        double elapsed = MPI_Wtime() - t_start, slowest = 0;
        double mine = sender ? (double)b * WINDOW * it * 8 / elapsed / 1e6 : 0;
        double *all = rank == 0 ? calloc(nprocs, sizeof *all) : NULL;
        MPI_Gather(&mine, 1, MPI_DOUBLE, all, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);
        MPI_Reduce(&elapsed, &slowest, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        if (rank == 0) {
            printf("{\"bench\":\"bw\",\"bytes\":%ld,\"pairs\":%d,\"window\":%d,\"iters\":%d,"
                   "\"pair_mbps\":[", b, k, WINDOW, it);
            for (int p = 0; p < k; p++) printf("%s%.2f", p ? "," : "", all[p]);
            printf("],\"total_mbps\":%.2f}\n", (double)b * WINDOW * it * k * 8 / slowest / 1e6);
            fflush(stdout);
            free(all);
        }
    }
    free(buf);
}

/* ---- collectives ---------------------------------------------------------------------- */

enum coll { ALLREDUCE, ALLTOALL, BCAST };

static void cmd_collective(enum coll c, const long *sizes, int ns, int max_iters) {
    static const char *names[] = {"allreduce", "alltoall", "bcast"};
    long maxb = 0;
    for (int s = 0; s < ns; s++) if (sizes[s] > maxb) maxb = sizes[s];
    size_t cap = (size_t)(maxb < 8 ? 8 : maxb) * (c == ALLTOALL ? nprocs : 1);
    char *sbuf = calloc(cap, 1), *rbuf = calloc(cap, 1);
    for (int s = 0; s < ns; s++) {
        long b = sizes[s];
        if (c == ALLREDUCE && b < 8) continue;           /* whole doubles only */
        long wire = c == ALLTOALL ? b * (nprocs - 1) : b;  /* bytes each rank sends */
        int it = iters_for(wire, max_iters), warm = it / 10 + 2;
        double *t = calloc(it, sizeof *t), *tmax = rank == 0 ? calloc(it, sizeof *tmax) : NULL;
        for (int i = -warm; i < it; i++) {
            MPI_Barrier(MPI_COMM_WORLD);
            double t0 = MPI_Wtime();
            if (c == ALLREDUCE)
                MPI_Allreduce(sbuf, rbuf, (int)(b / 8), MPI_DOUBLE, MPI_SUM, MPI_COMM_WORLD);
            else if (c == ALLTOALL)
                MPI_Alltoall(sbuf, (int)b, MPI_BYTE, rbuf, (int)b, MPI_BYTE, MPI_COMM_WORLD);
            else
                MPI_Bcast(sbuf, (int)b, MPI_BYTE, 0, MPI_COMM_WORLD);
            if (i >= 0) t[i] = MPI_Wtime() - t0;
        }
        MPI_Reduce(t, tmax, it, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
        if (rank == 0) {
            printf("{\"bench\":\"%s\",\"bytes\":%ld,\"ranks\":%d,\"iters\":%d,"
                   "\"us_min\":%.2f,\"us_p50\":%.2f}\n",
                   names[c], b, nprocs, it, minimum(tmax, it) * 1e6, median(tmax, it) * 1e6);
            fflush(stdout);
        }
        free(t);
        free(tmax);
    }
    free(sbuf);
    free(rbuf);
}

/* ---- synthetic pipeline layer exchange ------------------------------------------------- */

static void spin_us(double us) {
    double end = MPI_Wtime() + us * 1e-6;
    volatile double x = 1.0;
    while (MPI_Wtime() < end) x = x * 1.0000001 + 1e-9;
}

static void cmd_layer(const long *sizes, int ns, int microbatches, double compute_us) {
    if (nprocs < 2) die("layer needs at least 2 ranks");
    long maxb = 0;
    for (int s = 0; s < ns; s++) if (sizes[s] > maxb) maxb = sizes[s];
    /* Two buffers: receive the next micro-batch while the previous one is still being sent. */
    char *buf[2] = {calloc(maxb ? maxb : 1, 1), calloc(maxb ? maxb : 1, 1)};
    int prev = rank - 1, next = rank + 1 < nprocs ? rank + 1 : -1;
    for (int s = 0; s < ns; s++) {
        long b = sizes[s];
        MPI_Request sreq[2] = {MPI_REQUEST_NULL, MPI_REQUEST_NULL};
        double *done = rank == nprocs - 1 ? calloc(microbatches, sizeof *done) : NULL;
        MPI_Barrier(MPI_COMM_WORLD);
        double t0 = MPI_Wtime();
        for (int m = 0; m < microbatches; m++) {
            char *p = buf[m % 2];
            MPI_Wait(&sreq[m % 2], MPI_STATUS_IGNORE);
            if (prev >= 0)
                MPI_Recv(p, (int)b, MPI_BYTE, prev, 4, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            spin_us(compute_us);
            if (next >= 0)
                MPI_Isend(p, (int)b, MPI_BYTE, next, 4, MPI_COMM_WORLD, &sreq[m % 2]);
            else
                done[m] = MPI_Wtime() - t0;
        }
        MPI_Waitall(2, sreq, MPI_STATUSES_IGNORE);
        /* The last stage knows when each micro-batch left the pipeline; report from rank 0. */
        double res[3] = {0, 0, 0};
        if (rank == nprocs - 1) {
            res[0] = done[microbatches - 1];                       /* makespan */
            res[1] = done[0];                                      /* first-batch latency */
            res[2] = microbatches > 1                              /* steady-state interval */
                ? (done[microbatches - 1] - done[0]) / (microbatches - 1) : done[0];
            MPI_Send(res, 3, MPI_DOUBLE, 0, 5, MPI_COMM_WORLD);
            free(done);
        }
        if (rank == 0) {
            MPI_Recv(res, 3, MPI_DOUBLE, nprocs - 1, 5, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            printf("{\"bench\":\"layer\",\"bytes\":%ld,\"stages\":%d,\"microbatches\":%d,"
                   "\"compute_us\":%.1f,\"makespan_ms\":%.3f,\"first_ms\":%.3f,"
                   "\"interval_ms\":%.3f,\"per_s\":%.2f}\n",
                   b, nprocs, microbatches, compute_us, res[0] * 1e3, res[1] * 1e3,
                   res[2] * 1e3, res[2] > 0 ? 1.0 / res[2] : 0.0);
            fflush(stdout);
        }
    }
    free(buf[0]);
    free(buf[1]);
}

/* ---- failure and cleanup helpers ----------------------------------------------------- */

static void cmd_fail(int who, int code) {
    MPI_Barrier(MPI_COMM_WORLD);
    if (rank == who) {
        fprintf(stderr, "dcbench: rank %d exiting with %d without MPI_Finalize\n", rank, code);
        exit(code);
    }
    /* Everyone else waits for a message that will never come. Only the launcher can end
       this step; a correct setup must not leave these ranks waiting forever. */
    char x;
    MPI_Recv(&x, 1, MPI_BYTE, who, 9, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
}

static void cmd_sleep(double seconds) {
    double end = MPI_Wtime() + seconds;
    int flag = 0;
    MPI_Request r;
    MPI_Ibarrier(MPI_COMM_WORLD, &r);
    while (MPI_Wtime() < end) {
        if (!flag) MPI_Test(&r, &flag, MPI_STATUS_IGNORE);
        usleep(10000);
    }
    if (!flag) MPI_Wait(&r, MPI_STATUS_IGNORE);
}

/* ---- main ---------------------------------------------------------------------------- */

static int parse_sizes(const char *s, long *out, int max) {
    int n = 0;
    while (*s && n < max) {
        char *end;
        long v = strtol(s, &end, 10);
        if (end == s || v < 0) return -1;
        out[n++] = v;
        s = *end == ',' ? end + 1 : end;
        if (*end && *end != ',') return -1;
    }
    return n;
}

int main(int argc, char **argv) {
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    if (argc < 2) die("usage: dcbench info|pingpong|bw|allreduce|alltoall|bcast|layer|fail|sleep");

    long sizes[64];
    int ns = sizeof DEFAULT_SIZES / sizeof *DEFAULT_SIZES;
    memcpy(sizes, DEFAULT_SIZES, sizeof DEFAULT_SIZES);
    int max_iters = 1000, microbatches = 32, fail_rank = 1, fail_code = 3;
    double compute_us = 0, seconds = 600;
    for (int i = 2; i < argc; i++) {
        const char *a = argv[i], *v = i + 1 < argc ? argv[i + 1] : NULL;
        if (!v) die("missing option value");
        if (!strcmp(a, "--sizes")) { if ((ns = parse_sizes(v, sizes, 64)) <= 0) die("bad --sizes"); }
        else if (!strcmp(a, "--max-iters")) max_iters = atoi(v);
        else if (!strcmp(a, "--microbatches")) microbatches = atoi(v);
        else if (!strcmp(a, "--compute-us")) compute_us = atof(v);
        else if (!strcmp(a, "--rank")) fail_rank = atoi(v);
        else if (!strcmp(a, "--code")) fail_code = atoi(v);
        else if (!strcmp(a, "--seconds")) seconds = atof(v);
        else die("unknown option");
        i++;
    }
    if (max_iters < 8) max_iters = 8;
    if (microbatches < 1) microbatches = 1;

    const char *cmd = argv[1];
    if (!strcmp(cmd, "info")) cmd_info();
    else if (!strcmp(cmd, "pingpong")) cmd_pingpong(sizes, ns, max_iters);
    else if (!strcmp(cmd, "bw")) cmd_bw(sizes, ns, max_iters);
    else if (!strcmp(cmd, "allreduce")) cmd_collective(ALLREDUCE, sizes, ns, max_iters);
    else if (!strcmp(cmd, "alltoall")) cmd_collective(ALLTOALL, sizes, ns, max_iters);
    else if (!strcmp(cmd, "bcast")) cmd_collective(BCAST, sizes, ns, max_iters);
    else if (!strcmp(cmd, "layer")) cmd_layer(sizes, ns, microbatches, compute_us);
    else if (!strcmp(cmd, "fail")) cmd_fail(fail_rank, fail_code);
    else if (!strcmp(cmd, "sleep")) cmd_sleep(seconds);
    else die("unknown command");
    MPI_Finalize();
    return 0;
}
