/*
 * pynixd-krun: one build guest, in this process (nixidae/nixidae#1186).
 *
 *   pynixd-krun --init FILE --cpus N --memory MIB [--dax-window BYTES]
 *               [--ro DIR:ALLOWLIST]... [--rw DIR]... [--rw-allow DIR:ALLOWLIST]...
 *               --cwd DIR --status PATH --exit PATH
 *               [--uid N --gid N] -- COMMAND [ARG]...
 *
 * The guest boots libkrunfw's kernel on a root that holds only --init (as
 * /init), its configuration and the mount points. Each --ro and --rw share is
 * mounted at its host path, in the order given, so a path means the same on
 * both sides. A --ro share shows the guest only what its allowlist names, one
 * store path per line, and maps its files through a DAX window of
 * --dax-window bytes: the guest's page cache holds no copy of them. A
 * --rw-allow share is writable and shows only what its allowlist names; above
 * a listed path the guest can change nothing. It gives the guest one file of a
 * directory, which virtio-fs cannot share alone.
 *
 * The init runs COMMAND in --cwd, a directory on a share, as --uid and --gid (this process's own by default),
 * the owners the shares show. It opens --status as its file descriptor 3, and writes
 * how COMMAND ended to --exit. Both are paths in the guest, on a --rw share.
 * The guest's console is this process's stdout. libkrun takes the process
 * over, and the guest's reset ends it. PYNIXD_KRUN_LOG_LEVEL is libkrun's log
 * level: 0 off to 5 trace, errors by default. PYNIXD_KRUN_CMDLINE goes on the
 * end of the kernel's command line ("loglevel=7" shows the boot).
 */

#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include <libkrun.h>

#define MAX_SHARES 16
#define MAX_DIRS 512

static bool push_to_stderr(void *userdata, KrunStr s)
{
    (void)userdata;
    fwrite(s.data, 1, s.len, stderr);
    return true;
}

static KrunVtableHandle stderr_writer =
    KRUN_VTABLE_HANDLE(KRUN_PUSH_STR_TYPE_TAG, ((KrunPushStrVtable){.drop = NULL, .push = push_to_stderr}), NULL);

#define CHECK(call)                                       \
    err = NULL;                                           \
    call;                                                 \
    if (err) {                                            \
        fprintf(stderr, "pynixd-krun: %s: ", #call);      \
        krun_error_message(err, &stderr_writer);          \
        fputc('\n', stderr);                              \
        return 1;                                         \
    }

struct share {
    const char *dir;
    const char *allowlist; /* NULL for a writable share that shows everything */
    bool writable;
};

static const char *dirs[MAX_DIRS];
static int ndirs;

static bool parse_long(const char *text, long min, long max, long *out)
{
    char *end;
    errno = 0;
    long value = strtol(text, &end, 10);
    if (errno || *end != '\0' || end == text || value < min || value > max)
        return false;
    *out = value;
    return true;
}

static void usage(void)
{
    fprintf(stderr, "usage: pynixd-krun --init FILE --cpus N --memory MIB [--dax-window BYTES]\n"
                    "                   [--ro DIR:ALLOWLIST]... [--rw DIR]... [--rw-allow DIR:ALLOWLIST]...\n"
                    "                   --cwd DIR --status PATH --exit PATH\n"
                    "                   [--uid N --gid N] -- COMMAND [ARG]...\n");
}

static char *slurp(const char *path, size_t *len)
{
    FILE *f = fopen(path, "rb");
    if (f == NULL)
        return NULL;
    fseek(f, 0, SEEK_END);
    long size = ftell(f);
    rewind(f);
    char *data = malloc(size > 0 ? (size_t)size : 1);
    *len = fread(data, 1, (size_t)size, f);
    fclose(f);
    return *len == (size_t)size ? data : NULL;
}

/* Every directory above and at *path*, each once: the overlay refuses an
 * entry whose parent it lacks. Relative to the root, without the leading "/". */
static int add_dirs(KrunFsOverlay overlay, const char *path)
{
    KrunError err;
    char *copy = strdup(path + strspn(path, "/"));
    for (char *slash = copy;; slash++) {
        bool end = *slash == '\0';
        if (*slash != '/' && !end)
            continue;
        char saved = *slash;
        *slash = '\0';
        bool seen = copy[0] == '\0';
        for (int i = 0; i < ndirs && !seen; i++)
            seen = strcmp(dirs[i], copy) == 0;
        if (!seen) {
            if (ndirs == MAX_DIRS) {
                fprintf(stderr, "pynixd-krun: more than %d directories on the root\n", MAX_DIRS);
                return 1;
            }
            dirs[ndirs++] = strdup(copy);
            CHECK(krun_fs_overlay_add_dir(overlay, KRUN_STR(copy), 0755, &err));
        }
        *slash = saved;
        if (end)
            break;
    }
    free(copy);
    return 0;
}

static int add_file(KrunFsOverlay overlay, const char *path, const char *data, size_t len, uint32_t mode)
{
    KrunError err;
    CHECK(krun_fs_overlay_add_file(overlay, KRUN_STR(path), ((KrunBytes){.data = (const uint8_t *)data, .len = len}),
                                   mode, false, &err));
    return 0;
}

int main(int argc, char *argv[])
{
    enum { INIT = 256, CPUS, MEMORY, DAX, RO, RW, RW_ALLOW, STATUS, EXIT, UID, GID, CWD };
    static const struct option options[] = {
        {"init", required_argument, NULL, INIT},     {"cpus", required_argument, NULL, CPUS},
        {"memory", required_argument, NULL, MEMORY}, {"dax-window", required_argument, NULL, DAX},
        {"ro", required_argument, NULL, RO},         {"rw", required_argument, NULL, RW},
        {"status", required_argument, NULL, STATUS}, {"exit", required_argument, NULL, EXIT},
        {"cwd", required_argument, NULL, CWD},       {"rw-allow", required_argument, NULL, RW_ALLOW},
        {"uid", required_argument, NULL, UID},       {"gid", required_argument, NULL, GID},
        {NULL, 0, NULL, 0},
    };
    const char *init = NULL, *status = NULL, *exit_path = NULL, *cwd = NULL;
    long cpus = 0, memory = 0, dax_window = 0, uid = (long)getuid(), gid = (long)getgid();
    struct share shares[MAX_SHARES];
    int nshares = 0, c;

    while ((c = getopt_long(argc, argv, "", options, NULL)) != -1) {
        bool ok = true;
        switch (c) {
        case INIT: init = optarg; break;
        case STATUS: status = optarg; break;
        case CWD: cwd = optarg; break;
        case EXIT: exit_path = optarg; break;
        case CPUS: ok = parse_long(optarg, 1, 255, &cpus); break;
        case MEMORY: ok = parse_long(optarg, 1, UINT32_MAX, &memory); break;
        case DAX: ok = parse_long(optarg, 0, INT64_MAX, &dax_window); break;
        case UID: ok = parse_long(optarg, 0, UINT32_MAX - 1, &uid); break;
        case GID: ok = parse_long(optarg, 0, UINT32_MAX - 1, &gid); break;
        case RO:
        case RW_ALLOW: {
            char *colon = strrchr(optarg, ':');
            ok = nshares < MAX_SHARES && colon != NULL && optarg[0] == '/';
            if (ok) {
                *colon = '\0';
                shares[nshares++] = (struct share){.dir = optarg, .allowlist = colon + 1, .writable = c == RW_ALLOW};
            }
            break;
        }
        case RW:
            ok = nshares < MAX_SHARES && optarg[0] == '/';
            if (ok)
                shares[nshares++] = (struct share){.dir = optarg, .allowlist = NULL, .writable = true};
            break;
        default: ok = false; break;
        }
        if (!ok) {
            usage();
            return 2;
        }
    }
    if (optind >= argc || !init || !cwd || !status || !exit_path || !cpus || !memory) {
        usage();
        return 2;
    }

    long level = KRUN_LOG_LEVEL_ERROR;
    const char *level_env = getenv("PYNIXD_KRUN_LOG_LEVEL");
    if (level_env != NULL && !parse_long(level_env, KRUN_LOG_LEVEL_OFF, KRUN_LOG_LEVEL_TRACE, &level)) {
        fprintf(stderr, "pynixd-krun: PYNIXD_KRUN_LOG_LEVEL is 0 to 5, not %s\n", level_env);
        return 2;
    }

    KrunError err;
    CHECK(krun_init_log(STDERR_FILENO, (uint32_t)level, KRUN_LOG_STYLE_AUTO, 0, &err));

    size_t init_len;
    char *init_data = slurp(init, &init_len);
    if (init_data == NULL) {
        fprintf(stderr, "pynixd-krun: cannot read %s: %s\n", init, strerror(errno));
        return 1;
    }

    /* The configuration the init reads from /.pynixd. */
    char ids[32];
    snprintf(ids, sizeof ids, "%ld %ld\n", uid, gid);
    char *mounts = NULL, *args = NULL;
    size_t mounts_len = 0, args_len = 0;
    FILE *m = open_memstream(&mounts, &mounts_len);
    for (int i = 0; i < nshares; i++)
        fprintf(m, "s%d %s %s\n", i, shares[i].writable ? "rw" : "ro", shares[i].dir);
    fclose(m);
    FILE *a = open_memstream(&args, &args_len);
    for (int i = optind; i < argc; i++)
        fwrite(argv[i], 1, strlen(argv[i]) + 1, a);
    fclose(a);

    KrunFsOverlay overlay = krun_fs_overlay_new();
    if (add_dirs(overlay, "dev") || add_dirs(overlay, "proc") || add_dirs(overlay, "sys") ||
        add_dirs(overlay, ".pynixd"))
        return 1;
    for (int i = 0; i < nshares; i++)
        if (add_dirs(overlay, shares[i].dir))
            return 1;
    if (add_file(overlay, "init", init_data, init_len, 0555) ||
        add_file(overlay, ".pynixd/mounts", mounts, mounts_len, 0444) ||
        add_file(overlay, ".pynixd/argv", args, args_len, 0444) ||
        add_file(overlay, ".pynixd/status", status, strlen(status), 0444) ||
        add_file(overlay, ".pynixd/cwd", cwd, strlen(cwd), 0444) ||
        add_file(overlay, ".pynixd/exit", exit_path, strlen(exit_path), 0444) ||
        add_file(overlay, ".pynixd/ids", ids, strlen(ids), 0444))
        return 1;

    KrunMmioDeviceManager devices = krun_mmio_device_manager_new();

    /* hvc0: the kernel's console and the build's output, on our stdout. A pipe
     * whose write end this process holds, because libkrun stops the guest when
     * a console's input reaches end of file. An inout port is no console, even
     * as port 0: the default console carries the terminal properties hvc0
     * needs. -1 skips stderr. */
    int console_in[2];
    if (pipe(console_in) != 0) {
        perror("pynixd-krun: pipe");
        return 1;
    }
    {
        KrunConsoleBuilder cb = krun_console_device_builder();
        CHECK(krun_console_builder_add_default_console(cb, console_in[0], STDOUT_FILENO, -1, &err));
        CHECK(KrunConsoleDevice console = krun_console_builder_build(cb, &err));
        krun_mmio_device_manager_add(devices, console);
    }

    {
        CHECK(KrunFsDevice root = krun_fs_device_new_null(KRUN_STR("/dev/root"), &err));
        krun_fs_device_set_overlay(root, overlay);
        krun_mmio_device_manager_add(devices, root);
    }
    for (int i = 0; i < nshares; i++) {
        char tag[16];
        snprintf(tag, sizeof tag, "s%d", i);
        if (!shares[i].writable) {
            /* Store paths never change, so the guest's cache of them never goes
             * stale: Always. Auto drops a DAX mapping in use on open, and the
             * guest takes a SIGBUS (smol-machines/smolvm#1741). */
            CHECK(KrunFsDevice fs = krun_fs_device_new_read_only(KRUN_STR(tag), KRUN_STR(shares[i].dir), &err));
            krun_fs_device_set_dax_window_size(fs, (uint64_t)dax_window);
            CHECK(krun_fs_device_set_cache_policy(fs, KRUN_CACHE_POLICY_ALWAYS, &err));
            CHECK(krun_fs_device_set_allowlist_file(fs, KRUN_STR(shares[i].allowlist), &err));
            krun_mmio_device_manager_add(devices, fs);
        } else {
            CHECK(KrunFsDevice fs = krun_fs_device_new(KRUN_STR(tag), KRUN_STR(shares[i].dir), &err));
            if (shares[i].allowlist) {
                CHECK(krun_fs_device_set_allowlist_file(fs, KRUN_STR(shares[i].allowlist), &err));
            }
            krun_mmio_device_manager_add(devices, fs);
        }
    }

    CHECK(KrunRngDevice rng = krun_rng_device_new(&err));
    krun_mmio_device_manager_add(devices, rng);
    /* Free page reporting: the guest's freed pages go back to the host. */
    CHECK(KrunBalloonDevice balloon = krun_balloon_device_new(&err));
    krun_mmio_device_manager_add(devices, balloon);

    CHECK(KrunPayload payload = krun_payload_load_krunfw(&err));
    krun_payload_append_cmdline(payload, KRUN_STR("root=/dev/root init=/init"));
    const char *extra = getenv("PYNIXD_KRUN_CMDLINE");
    if (extra != NULL)
        krun_payload_append_cmdline(payload, KRUN_STR(extra));

    KrunVmmBuilder builder = krun_vmm_builder_new();
    CHECK(krun_vmm_builder_vcpus(&builder, (uint8_t)cpus, &err));
    CHECK(krun_vmm_builder_ram_mib(&builder, (uint32_t)memory, &err));
    krun_vmm_builder_payload(&builder, payload);
    krun_vmm_builder_devices(&builder, devices);

    CHECK(KrunVmm vmm = krun_vmm_builder_build(&builder, &err));
    krun_vmm_run(vmm);
    fprintf(stderr, "pynixd-krun: the VMM's event loop ended\n");
    return 1;
}
