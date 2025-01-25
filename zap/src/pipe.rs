use std::{io, os::fd::RawFd};

// Create a new pipe with the `O_CLOEXEC` and `O_NONBLOCK` flags set.
// This is a wrapper around `libc::pipe2` or `libc::pipe` depending on the platform.
//
// Stole from: https://github.com/tokio-rs/mio/blob/master/src/sys/unix/pipe.rs
pub(crate) fn new_raw() -> io::Result<[RawFd; 2]> {
    let mut fds: [RawFd; 2] = [-1, -1];

    #[cfg(any(
        target_os = "android",
        target_os = "dragonfly",
        target_os = "freebsd",
        target_os = "fuchsia",
        target_os = "hurd",
        target_os = "linux",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "illumos",
        target_os = "redox",
        target_os = "solaris",
        target_os = "vita",
    ))]
    unsafe {
        if libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC | libc::O_NONBLOCK) != 0 {
            return Err(io::Error::last_os_error());
        }
    }

    #[cfg(any(
        target_os = "aix",
        target_os = "haiku",
        target_os = "ios",
        target_os = "macos",
        target_os = "tvos",
        target_os = "visionos",
        target_os = "watchos",
        target_os = "espidf",
        target_os = "nto",
    ))]
    unsafe {
        // For platforms that don't have `pipe2(2)` we need to manually set the
        // correct flags on the file descriptor.
        if libc::pipe(fds.as_mut_ptr()) != 0 {
            return Err(io::Error::last_os_error());
        }

        for fd in &fds {
            if libc::fcntl(*fd, libc::F_SETFL, libc::O_NONBLOCK) != 0
                || libc::fcntl(*fd, libc::F_SETFD, libc::FD_CLOEXEC) != 0
            {
                let err = io::Error::last_os_error();
                // Don't leak file descriptors. Can't handle closing error though.
                let _ = libc::close(fds[0]);
                let _ = libc::close(fds[1]);
                return Err(err);
            }
        }
    }

    Ok(fds)
}
