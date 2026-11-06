/*
 * pynixd-krun-init: PID 1 of a build guest (nixidae/nixidae#1186).
 *
 * pynixd-krun puts this binary at /init on a root with no other files, and
 * its configuration under /.pynixd:
 *
 *   mounts   one share per line: "TAG ro PATH" or "TAG rw PATH"
 *   argv     the command, each argument ended by a NUL byte
 *   status   a path, opened for writing as file descriptor 3
 *   exit     a path, where the command's end is written: "exit N" or "signal N"
 *
 * The command runs on a pseudoterminal, as a build on the host does, and its
 * output reaches hvc0, the console pynixd-krun gives the build's terminal.
 * hvc0 is raw: the host's terminal already turns "\n" into "\r\n".
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/reboot.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <termios.h>
#include <unistd.h>

#define CONFIG "/.pynixd/"

static void stop(void)
{
    sync();
    reboot(RB_AUTOBOOT);
    for (;;)
        pause();
}

static void fail(const char *what)
{
    dprintf(STDERR_FILENO, "pynixd-krun-init: %s: %s\n", what, strerror(errno));
    stop();
}

static char *slurp(const char *path, size_t *len)
{
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        fail(path);
    size_t cap = 4096, n = 0;
    char *buf = malloc(cap + 1);
    for (;;) {
        if (n == cap && !(buf = realloc(buf, (cap *= 2) + 1)))
            fail("realloc");
        ssize_t got = read(fd, buf + n, cap - n);
        if (got < 0)
            fail(path);
        if (got == 0)
            break;
        n += (size_t)got;
    }
    close(fd);
    buf[n] = '\0';
    *len = n;
    return buf;
}

static void make_raw(int fd)
{
    struct termios t;
    if (tcgetattr(fd, &t) == 0) {
        cfmakeraw(&t);
        tcsetattr(fd, TCSANOW, &t);
    }
}

/* The root has no /dev/console, so the kernel starts PID 1 with no standard
 * descriptors: until hvc0 is open, an error goes nowhere. libkrunfw's kernel
 * mounts devtmpfs itself (CONFIG_DEVTMPFS_MOUNT). */
static int open_console(void)
{
    if (mount("devtmpfs", "/dev", "devtmpfs", MS_NOSUID, NULL) != 0 && errno != EBUSY)
        stop();
    int console = open("/dev/hvc0", O_RDWR | O_NOCTTY);
    if (console < 0)
        stop();
    make_raw(console);
    for (int fd = 0; fd < 3; fd++)
        dup2(console, fd);
    fcntl(console, F_SETFD, FD_CLOEXEC);
    return console;
}

static void mount_base(void)
{
    if (mount("proc", "/proc", "proc", MS_NOSUID | MS_NODEV | MS_NOEXEC, NULL) != 0)
        fail("mount /proc");
    if (mount("sysfs", "/sys", "sysfs", MS_NOSUID | MS_NODEV | MS_NOEXEC, NULL) != 0)
        fail("mount /sys");
    mkdir("/dev/pts", 0755);
    if (mount("devpts", "/dev/pts", "devpts", MS_NOSUID | MS_NOEXEC, "ptmxmode=0666,mode=0620") != 0)
        fail("mount /dev/pts");
    mkdir("/dev/shm", 01777);
    if (mount("tmpfs", "/dev/shm", "tmpfs", MS_NOSUID | MS_NODEV, NULL) != 0)
        fail("mount /dev/shm");
}

/* The command starts in the first writable share: the root is a null fs, which
 * cannot list a directory, and Python puts its working directory on sys.path. */
static void mount_shares(void)
{
    bool cwd = false;
    size_t len;
    char *text = slurp(CONFIG "mounts", &len);
    for (char *line = strtok(text, "\n"); line; line = strtok(NULL, "\n")) {
        char tag[64], mode[3], path[4096];
        if (sscanf(line, "%63s %2s %4095s", tag, mode, path) != 3) {
            errno = EINVAL;
            fail(line);
        }
        unsigned long flags = MS_NOSUID | MS_NODEV;
        const char *data = NULL;
        if (strcmp(mode, "ro") == 0) {
            flags |= MS_RDONLY;
            /* A read-only share is the store, with a DAX window. */
            data = "dax=always";
        }
        if (mount(tag, path, "virtiofs", flags, data) != 0)
            fail(path);
        if (!cwd && data == NULL) {
            if (chdir(path) != 0)
                fail(path);
            cwd = true;
        }
    }
    free(text);
}

static char **read_argv(void)
{
    size_t len;
    char *text = slurp(CONFIG "argv", &len);
    size_t count = 0;
    for (size_t i = 0; i < len; i++)
        count += text[i] == '\0';
    if (count == 0) {
        errno = EINVAL;
        fail(CONFIG "argv");
    }
    char **argv = calloc(count + 1, sizeof *argv);
    for (size_t i = 0, at = 0; i < count; i++) {
        argv[i] = text + at;
        at += strlen(text + at) + 1;
    }
    return argv;
}

static char *read_path(const char *name)
{
    size_t len;
    char *path = slurp(name, &len);
    path[strcspn(path, "\n")] = '\0';
    return path;
}

static void write_all(int fd, const char *buf, ssize_t n)
{
    while (n > 0) {
        ssize_t put = write(fd, buf, (size_t)n);
        if (put < 0) {
            if (errno == EINTR)
                continue;
            return;
        }
        buf += put;
        n -= put;
    }
}

int main(void)
{
    int console = open_console();
    mount_base();
    mount_shares();
    char **argv = read_argv();
    char *status = read_path(CONFIG "status");
    char *exit_path = read_path(CONFIG "exit");

    int master = posix_openpt(O_RDWR | O_NOCTTY | O_CLOEXEC);
    if (master < 0 || grantpt(master) != 0 || unlockpt(master) != 0)
        fail("posix_openpt");
    char *slave_name = ptsname(master);

    pid_t child = fork();
    if (child < 0)
        fail("fork");
    if (child == 0) {
        setsid();
        int slave = open(slave_name, O_RDWR);
        int null = open("/dev/null", O_RDONLY);
        int status_fd = open(status, O_WRONLY | O_CREAT | O_TRUNC, 0600);
        if (slave < 0 || null < 0 || status_fd < 0)
            fail("open the command's files");
        ioctl(slave, TIOCSCTTY, 0);
        dup2(null, STDIN_FILENO);
        dup2(slave, STDOUT_FILENO);
        dup2(slave, STDERR_FILENO);
        dup2(status_fd, 3);
        for (int fd = 4; fd < 64; fd++)
            close(fd);
        execv(argv[0], argv);
        fail(argv[0]);
    }

    char buf[65536];
    for (;;) {
        ssize_t got = read(master, buf, sizeof buf);
        if (got > 0) {
            write_all(console, buf, got);
            continue;
        }
        if (got < 0 && errno == EINTR)
            continue;
        break; /* EIO: every holder of the slave closed it */
    }

    int wstatus;
    while (waitpid(child, &wstatus, 0) < 0)
        if (errno != EINTR)
            fail("waitpid");
    int out = open(exit_path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (out < 0)
        fail(exit_path);
    if (WIFSIGNALED(wstatus))
        dprintf(out, "signal %d\n", WTERMSIG(wstatus));
    else
        dprintf(out, "exit %d\n", WEXITSTATUS(wstatus));
    close(out);
    stop();
}
