/*
 * pynixd-krun-init: PID 1 of a build guest (nixidae/nixidae#1186).
 *
 * pynixd-krun puts this binary at /init on a root with no other files, and
 * its configuration under /.pynixd:
 *
 *   mounts   one share per line: "TAG ro PATH" or "TAG rw PATH"
 *   argv     the command, each argument ended by a NUL byte
 *   status   a path, opened for writing as file descriptor 3
 *   exit     a path, where the command's end is written: "exit N" or "signal N";
 *            or "init MESSAGE", the init's error that stopped the guest first
 *   ids      "UID GID SOCKET_UID SOCKET_GID": who the command runs as, and who
 *            owns each listen socket
 *   cwd      the directory the command starts in
 *   disks    one directory per line: where /dev/vda, /dev/vdb, ... mount, as XFS
 *   vsock    one port per line: "fd PORT N", a connection to the host's PORT
 *            as the command's descriptor N; or "listen PORT PATH", a Unix
 *            socket at PATH whose every connection joins a new one to PORT
 *   net      optional: "ADDRESS PREFIX GATEWAY MTU", the IPv4 configuration of
 *            eth0, the NIC passt serves; without it the guest has lo alone
 *
 * A listen socket is the build's: its owner is SOCKET_UID, mode 0600. Its
 * relay runs outside the command's namespaces, as the command's uid: as root
 * with a build user, a relay as the build uid would die in the helper's
 * kill(-1) of that uid.
 *
 * The shares show their files with the host's owners, so the command runs as
 * the uid that owns the build's directories, as it does on the host. As root it
 * would lose the directories in its own user namespace, where that uid is not
 * mapped.
 *
 * The command runs on a pseudoterminal, as a build on the host does, and its
 * output reaches hvc0, the console pynixd-krun gives the build's terminal.
 * hvc0 is raw: the host's terminal already turns "\n" into "\r\n".
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <poll.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/reboot.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <termios.h>
#include <unistd.h>

#include <arpa/inet.h>
#include <net/if.h>
#include <net/route.h>
#include <netinet/in.h>

#include <linux/vm_sockets.h>

#define CONFIG "/.pynixd/"
#define MAX_PORTS 8
/* Above every descriptor the command gets, until each moves to its own. */
#define PARKED_FD 64

static void stop(void)
{
    sync();
    reboot(RB_AUTOBOOT);
    for (;;)
        pause();
}

/* The exit file, once the shares are up: fail() records its error there too, so the host knows why the guest
 * stopped when no console carried the line. */
static const char *exit_record;

static void fail(const char *what)
{
    const char *reason = strerror(errno);
    dprintf(STDERR_FILENO, "pynixd-krun-init: %s: %s\n", what, reason);
    int out = exit_record ? open(exit_record, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600) : -1;
    if (out >= 0) {
        dprintf(out, "init %s: %s\n", what, reason);
        close(out);
    }
    stop();
}

/* In the command's process: its stderr is the terminal the init relays, and a reboot would end the guest before
 * the line reaches the console. As the build's uid, reboot fails and the process would wait forever. */
static void child_fail(const char *what)
{
    dprintf(STDERR_FILENO, "pynixd-krun-init: %s: %s\n", what, strerror(errno));
    _exit(127);
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
 * mounts devtmpfs itself (CONFIG_DEVTMPFS_MOUNT). Without hvc0 the init goes on
 * with /dev/null, and fails once the exit file can record why. */
static int console_errno;

static int open_console(void)
{
    if (mount("devtmpfs", "/dev", "devtmpfs", MS_NOSUID, NULL) != 0 && errno != EBUSY)
        stop();
    int console = open("/dev/hvc0", O_RDWR | O_NOCTTY);
    if (console < 0) {
        console_errno = errno;
        int null = open("/dev/null", O_RDWR);
        for (int fd = 0; null >= 0 && fd < 3; fd++)
            dup2(null, fd);
        return -1;
    }
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

static void mount_shares(void)
{
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
    }
    free(text);
}

/* After the shares: a disk's directory is on one. */
static void mount_disks(void)
{
    size_t len;
    char *text = slurp(CONFIG "disks", &len);
    char device[] = "/dev/vda";
    for (char *line = strtok(text, "\n"); line; line = strtok(NULL, "\n")) {
        if (mount(device, line, "xfs", MS_NOSUID | MS_NODEV, NULL) != 0)
            fail(line);
        device[sizeof device - 2]++;
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

static void set_ipv4(int s, const char *name, unsigned long request, const char *address)
{
    struct ifreq ifr = {0};
    strncpy(ifr.ifr_name, name, IFNAMSIZ - 1);
    struct sockaddr_in *sin = (struct sockaddr_in *)&ifr.ifr_addr;
    sin->sin_family = AF_INET;
    if (inet_pton(AF_INET, address, &sin->sin_addr) != 1) {
        errno = EINVAL;
        fail(address);
    }
    if (ioctl(s, request, &ifr) != 0)
        fail(name);
}

static void link_up(int s, const char *name)
{
    struct ifreq ifr = {0};
    strncpy(ifr.ifr_name, name, IFNAMSIZ - 1);
    if (ioctl(s, SIOCGIFFLAGS, &ifr) != 0)
        fail(name);
    ifr.ifr_flags |= IFF_UP;
    if (ioctl(s, SIOCSIFFLAGS, &ifr) != 0)
        fail(name);
}

/* lo, and eth0 as CONFIG "net" says. IPv6 on eth0 comes from passt's router
 * advertisements, which the kernel takes once the link is up. */
static void configure_network(void)
{
    int s = socket(AF_INET, SOCK_DGRAM | SOCK_CLOEXEC, 0);
    if (s < 0)
        fail("socket");
    link_up(s, "lo");
    if (access(CONFIG "net", R_OK) == 0) {
        char address[INET_ADDRSTRLEN], gateway[INET_ADDRSTRLEN];
        unsigned prefix, mtu;
        char *net = read_path(CONFIG "net");
        if (sscanf(net, "%15s %u %15s %u", address, &prefix, gateway, &mtu) != 4 || prefix > 32) {
            errno = EINVAL;
            fail(CONFIG "net");
        }
        set_ipv4(s, "eth0", SIOCSIFADDR, address);
        char mask[INET_ADDRSTRLEN];
        struct in_addr bits = {.s_addr = htonl(prefix ? ~0u << (32 - prefix) : 0)};
        inet_ntop(AF_INET, &bits, mask, sizeof mask);
        set_ipv4(s, "eth0", SIOCSIFNETMASK, mask);
        struct ifreq ifr = {0};
        strncpy(ifr.ifr_name, "eth0", IFNAMSIZ - 1);
        ifr.ifr_mtu = (int)mtu;
        if (ioctl(s, SIOCSIFMTU, &ifr) != 0)
            fail("eth0 mtu");
        link_up(s, "eth0");
        struct rtentry route = {0};
        struct sockaddr_in *dst = (struct sockaddr_in *)&route.rt_dst;
        struct sockaddr_in *via = (struct sockaddr_in *)&route.rt_gateway;
        struct sockaddr_in *genmask = (struct sockaddr_in *)&route.rt_genmask;
        dst->sin_family = via->sin_family = genmask->sin_family = AF_INET;
        if (inet_pton(AF_INET, gateway, &via->sin_addr) != 1) {
            errno = EINVAL;
            fail(gateway);
        }
        route.rt_flags = RTF_UP | RTF_GATEWAY;
        if (ioctl(s, SIOCADDRT, &route) != 0)
            fail("default route");
    }
    close(s);
}

static int write_all(int fd, const char *buf, ssize_t n)
{
    while (n > 0) {
        ssize_t put = write(fd, buf, (size_t)n);
        if (put < 0) {
            if (errno == EINTR)
                continue;
            return -1;
        }
        buf += put;
        n -= put;
    }
    return 0;
}

/* The build's cgroup, `GUEST` in `builder/cgroup.py`. The helper starts in
 * the root and moves itself, which takes write access to both cgroup.procs
 * files; the guest holds this one build alone. */
static void make_build_cgroup(unsigned uid, unsigned gid)
{
    if (mount("cgroup2", "/sys/fs/cgroup", "cgroup2", MS_NOSUID | MS_NODEV | MS_NOEXEC, NULL) != 0)
        fail("mount /sys/fs/cgroup");
    int control = open("/sys/fs/cgroup/cgroup.subtree_control", O_WRONLY | O_CLOEXEC);
    const char controllers[] = "+cpu +memory +io";
    if (control < 0 || write_all(control, controllers, sizeof controllers - 1) != 0)
        fail("enable the cgroup controllers");
    close(control);
    if (mkdir("/sys/fs/cgroup/build", 0755) != 0 || chown("/sys/fs/cgroup/build", uid, gid) != 0 ||
        chown("/sys/fs/cgroup/build/cgroup.procs", uid, gid) != 0 || chown("/sys/fs/cgroup/cgroup.procs", uid, gid) != 0)
        fail("/sys/fs/cgroup/build");
}

static int vsock_connect(unsigned port)
{
    struct sockaddr_vm addr = {.svm_family = AF_VSOCK, .svm_cid = VMADDR_CID_HOST, .svm_port = port};
    int sock = socket(AF_VSOCK, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (sock < 0 || connect(sock, (struct sockaddr *)&addr, sizeof addr) != 0)
        return -1;
    return sock;
}

/* Copies each way until both ends are done; an end of file goes on as a
 * half close, which a client that waits for the last reply needs. */
static void relay(int a, int b)
{
    int fds[2] = {a, b};
    struct pollfd polls[2] = {{.fd = a, .events = POLLIN}, {.fd = b, .events = POLLIN}};
    int open_ends = 2;
    char buf[65536];
    while (open_ends > 0) {
        if (poll(polls, 2, -1) < 0) {
            if (errno == EINTR)
                continue;
            return;
        }
        for (int i = 0; i < 2; i++) {
            if (polls[i].fd < 0 || !polls[i].revents)
                continue;
            ssize_t got = read(fds[i], buf, sizeof buf);
            if (got < 0 && errno == EINTR)
                continue;
            if (got <= 0) {
                shutdown(fds[1 - i], SHUT_WR);
                polls[i].fd = -1;
                open_ends--;
            } else if (write_all(fds[1 - i], buf, got) != 0) {
                return;
            }
        }
    }
}

/* A Unix socket bound through its directory: a build directory is longer
 * than sun_path. */
static int listen_at(const char *path, unsigned uid, unsigned gid)
{
    char *copy = strdup(path), *slash = strrchr(copy, '/');
    *slash = '\0';
    int dir = open(copy[0] ? copy : "/", O_PATH | O_DIRECTORY | O_CLOEXEC);
    struct sockaddr_un addr = {.sun_family = AF_UNIX};
    snprintf(addr.sun_path, sizeof addr.sun_path, "/proc/self/fd/%d/%s", dir, slash + 1);
    int sock = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (dir < 0 || sock < 0 || bind(sock, (struct sockaddr *)&addr, sizeof addr) != 0 ||
        fchownat(dir, slash + 1, uid, gid, 0) != 0 || fchmodat(dir, slash + 1, 0600, 0) != 0 || listen(sock, 64) != 0)
        fail(path);
    close(dir);
    free(copy);
    return sock;
}

/* The listener's own process, as the build's uid. Each connection gets a
 * process of its own: a relay holds two descriptors and blocks in poll. */
static void serve(int listener, unsigned port, unsigned uid, unsigned gid)
{
    pid_t pid = fork();
    if (pid < 0)
        fail("fork");
    if (pid > 0) {
        close(listener);
        return;
    }
    if (setgroups(0, NULL) != 0 || setgid(gid) != 0 || setuid(uid) != 0)
        fail("drop to the build's uid");
    signal(SIGCHLD, SIG_IGN);
    for (;;) {
        int conn = accept4(listener, NULL, NULL, SOCK_CLOEXEC);
        if (conn < 0) {
            if (errno == EINTR || errno == ECONNABORTED)
                continue;
            fail("accept");
        }
        if (fork() == 0) {
            int host = vsock_connect(port);
            if (host >= 0)
                relay(conn, host);
            _exit(0);
        }
        close(conn);
    }
}

/* Connects or listens for each line of the vsock file. The command's
 * descriptors wait at PARKED_FD and up. */
static int open_ports(unsigned uid, unsigned gid, unsigned socket_uid, unsigned socket_gid, int targets[MAX_PORTS])
{
    size_t len;
    char *text = slurp(CONFIG "vsock", &len);
    int n = 0;
    for (char *line = strtok(text, "\n"); line; line = strtok(NULL, "\n")) {
        unsigned port;
        int fd;
        char path[4096];
        if (sscanf(line, "fd %u %d", &port, &fd) == 2 && n < MAX_PORTS) {
            int sock = vsock_connect(port);
            if (sock < 0 || dup3(sock, PARKED_FD + n, O_CLOEXEC) < 0)
                fail(line);
            close(sock);
            targets[n++] = fd;
        } else if (sscanf(line, "listen %u %4095s", &port, path) == 2) {
            serve(listen_at(path, socket_uid, socket_gid), port, uid, gid);
        } else {
            errno = EINVAL;
            fail(line);
        }
    }
    free(text);
    return n;
}

int main(void)
{
    int console = open_console();
    mount_base();
    mount_shares();
    exit_record = read_path(CONFIG "exit");
    if (console < 0) {
        errno = console_errno;
        fail("/dev/hvc0");
    }
    mount_disks();
    configure_network();
    char **argv = read_argv();
    char *status = read_path(CONFIG "status");
    /* Not the root: a null fs cannot list a directory. */
    char *cwd = read_path(CONFIG "cwd");
    if (chdir(cwd) != 0)
        fail(cwd);
    unsigned uid, gid, socket_uid, socket_gid;
    char *ids = read_path(CONFIG "ids");
    if (sscanf(ids, "%u %u %u %u", &uid, &gid, &socket_uid, &socket_gid) != 4) {
        errno = EINVAL;
        fail(CONFIG "ids");
    }
    make_build_cgroup(uid, gid);
    int targets[MAX_PORTS];
    int nports = open_ports(uid, gid, socket_uid, socket_gid, targets);

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
            child_fail("open the command's files");
        ioctl(slave, TIOCSCTTY, 0);
        dup2(null, STDIN_FILENO);
        dup2(slave, STDOUT_FILENO);
        dup2(slave, STDERR_FILENO);
        dup2(status_fd, 3);
        for (int i = 0; i < nports; i++)
            if (dup2(PARKED_FD + i, targets[i]) < 0)
                child_fail("move a port to its descriptor");
        for (int fd = 4; fd < PARKED_FD + MAX_PORTS; fd++) {
            bool kept = false;
            for (int i = 0; i < nports; i++)
                kept |= targets[i] == fd;
            if (!kept)
                close(fd);
        }
        if (setgroups(0, NULL) != 0 || setgid(gid) != 0 || setuid(uid) != 0)
            child_fail("drop to the build's uid");
        execv(argv[0], argv);
        child_fail(argv[0]);
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
    int out = open(exit_record, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (out < 0)
        fail(exit_record);
    if (WIFSIGNALED(wstatus))
        dprintf(out, "signal %d\n", WTERMSIG(wstatus));
    else
        dprintf(out, "exit %d\n", WEXITSTATUS(wstatus));
    close(out);
    stop();
}
