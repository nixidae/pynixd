//! pynixd-agent: host samples for pynixd, over one SSH exec channel. Issue #98.
//!
//! usage: pynixd-agent INTERVAL_MS [RESOURCE:KIND:STALL_US:WINDOW_US | /PATH]...
//!
//! Writes one frame per wake to stdout and exits at EOF on stdin, so the
//! channel's end is the agent's end. It wakes every INTERVAL_MS, at once when
//! a PSI trigger named on the command line fires, and at once for a line on
//! stdin.
//!
//! Output: `pynixd-agent 1 <cpus>\n`, then frames:
//!   F <path> <length>\n<bytes>    once per readable file
//!   D <free> <total>\n            bytes of the filesystem of /PATH, when given
//!   E <reason> <uptime_ms>\n      reason: tick, ask, or the resource that fired
//!
//! The agent parses nothing. pynixd parses the files with the code that also
//! reads them over SFTP.
//!
//! A trigger fd always polls readable (`DEFAULT_POLLMASK` in
//! `psi_trigger_poll`); only POLLPRI says it fired. Waiting for POLLIN on one
//! wakes on every iteration.

use std::ffi::CString;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;
use std::process::exit;
use std::time::{Duration, Instant};

/// `SAMPLED_FILES` of pynixd/monitor.py.
const FILES: [&str; 6] = [
    "/proc/pressure/cpu",
    "/proc/pressure/memory",
    "/proc/pressure/io",
    "/proc/meminfo",
    "/proc/loadavg",
    "/sys/fs/cgroup/cpu.stat",
];
const MAX_TRIGGERS: usize = 6;
const POLLIN: i16 = 0x1;
const POLLPRI: i16 = 0x2;
const POLLERR: i16 = 0x8;
const O_NONBLOCK: i32 = 0o4000;

#[repr(C)]
struct PollFd {
    fd: i32,
    events: i16,
    revents: i16,
}

/// `struct statvfs` on 64-bit Linux, glibc and musl alike: 112 bytes.
#[repr(C)]
#[derive(Default)]
#[allow(dead_code)]
struct StatVfs {
    bsize: u64,
    frsize: u64,
    blocks: u64,
    bfree: u64,
    bavail: u64,
    files: u64,
    ffree: u64,
    favail: u64,
    fsid: u64,
    flag: u64,
    namemax: u64,
    spare: [i32; 6],
}

extern "C" {
    fn poll(fds: *mut PollFd, nfds: std::ffi::c_ulong, timeout: i32) -> i32;
    fn statvfs(path: *const std::ffi::c_char, buf: *mut StatVfs) -> i32;
}

fn disk(path: &CString) -> Option<(u64, u64)> {
    let mut st = StatVfs::default();
    // SAFETY: `path` is NUL-terminated and `st` is a whole `struct statvfs`.
    if unsafe { statvfs(path.as_ptr(), &mut st) } != 0 {
        return None;
    }
    Some((st.bavail * st.frsize, st.blocks * st.frsize))
}

fn frame(reason: &str, start: Instant, store: Option<&CString>) -> Vec<u8> {
    let mut out = Vec::new();
    for path in FILES {
        let Ok(text) = std::fs::read(path) else { continue };
        out.extend_from_slice(format!("F {path} {}\n", text.len()).as_bytes());
        out.extend_from_slice(&text);
    }
    if let Some((free, total)) = store.and_then(disk) {
        out.extend_from_slice(format!("D {free} {total}\n").as_bytes());
    }
    out.extend_from_slice(format!("E {reason} {}\n", start.elapsed().as_millis()).as_bytes());
    out
}

/// `memory:full:40000:2000000` -> the resource name and an armed trigger.
fn arm(spec: &str) -> std::io::Result<(String, File)> {
    let parts: Vec<&str> = spec.split(':').collect();
    let [resource, kind, stall, window] = parts[..] else {
        return Err(std::io::Error::other("want RESOURCE:KIND:STALL_US:WINDOW_US"));
    };
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(O_NONBLOCK)
        .open(format!("/proc/pressure/{resource}"))?;
    file.write_all(format!("{kind} {stall} {window}\0").as_bytes())?;
    Ok((resource.to_owned(), file))
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let Some(interval) = args.get(1).and_then(|a| a.parse::<u64>().ok()) else {
        eprintln!("usage: pynixd-agent INTERVAL_MS [RESOURCE:KIND:STALL_US:WINDOW_US | /PATH]...");
        exit(2);
    };
    let interval = Duration::from_millis(interval.max(100));

    let store = args[2..].iter().find(|a| a.starts_with('/')).and_then(|a| CString::new(a.as_str()).ok());
    let triggers: Vec<(String, File)> = args[2..]
        .iter()
        .filter(|a| !a.starts_with('/'))
        .take(MAX_TRIGGERS)
        .filter_map(|spec| arm(spec).map_err(|e| eprintln!("pynixd-agent: trigger {spec}: {e}")).ok())
        .collect();

    let cpus = std::thread::available_parallelism().map_or(1, |n| n.get());
    let start = Instant::now();
    let mut stdout = std::io::stdout().lock();
    let mut emit = |bytes: &[u8]| {
        if stdout.write_all(bytes).and_then(|()| stdout.flush()).is_err() {
            exit(1);
        }
    };
    emit(format!("pynixd-agent 1 {cpus}\n").as_bytes());
    emit(&frame("tick", start, store.as_ref()));

    let mut fds: Vec<PollFd> = std::iter::once(PollFd { fd: 0, events: POLLIN, revents: 0 })
        .chain(triggers.iter().map(|(_, f)| PollFd { fd: f.as_raw_fd(), events: POLLPRI, revents: 0 }))
        .collect();
    let mut stdin = std::io::stdin().lock();
    let mut next = Instant::now() + interval;
    loop {
        let wait = next.saturating_duration_since(Instant::now()).as_millis() as i32;
        // SAFETY: `fds` is a live array of `fds.len()` correctly laid out entries.
        if unsafe { poll(fds.as_mut_ptr(), fds.len() as _, wait) } < 0 {
            if std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            exit(1);
        }
        let mut reason = None;
        if fds[0].revents != 0 {
            match stdin.read(&mut [0; 512]) {
                Ok(0) | Err(_) => exit(0),
                Ok(_) => reason = Some("ask"),
            }
        }
        for (pfd, (name, _)) in fds[1..].iter().zip(&triggers) {
            if pfd.revents & POLLERR != 0 {
                exit(1);
            }
            if pfd.revents & POLLPRI != 0 {
                reason = Some(name.as_str());
            }
        }
        if reason.is_none() && Instant::now() < next {
            continue;
        }
        emit(&frame(reason.unwrap_or("tick"), start, store.as_ref()));
        next = Instant::now() + interval;
    }
}
