#![cfg(all(feature = "uring", unix))]
//! Experimental io_uring backend (feature-gated, Linux-only)
//!
//! Provides a thin synchronous wrapper around io_uring submit/wait for
//! vectored writes. This is a pragmatic prototype: it submits a single
//! `writev` request via io_uring and waits for completion. The goal is to
//! measure the potential benefit of using io_uring for vectored WAL
//! submissions; a production implementation should reuse an IoUring
//! instance, support batching and submission queue management.

use crate::error::{MidgeError, MidgeResult};
use std::cell::RefCell;
use std::fs::File;
use std::os::unix::io::AsRawFd;

use io_uring::{opcode, types, IoUring};

thread_local! {
    // Each thread lazily initializes its own IoUring instance. This avoids
    // synchronization overhead on the hot path and is a pragmatic step
    // toward high-performance submissions.
    static TLS_URING: RefCell<Option<IoUring>> = RefCell::new(None);
}

/// Write multiple buffers using io_uring by submitting a single writev SQE
/// and waiting for its completion. This blocks the calling thread while the
/// kernel processes the IO but avoids a user->kernel syscall for writev.
pub fn write_vectored_uring(file: &mut File, buffers: &[&[u8]]) -> MidgeResult<()> {
    if buffers.is_empty() {
        return Ok(());
    }

    // Build libc iovec array referencing the provided slices. Keep them
    // alive in this stack frame until completion.
    let mut iovecs: Vec<libc::iovec> = Vec::with_capacity(buffers.len());
    for b in buffers {
        iovecs.push(libc::iovec {
            iov_base: b.as_ptr() as *mut libc::c_void,
            iov_len: b.len(),
        });
    }

    let fd = file.as_raw_fd();

    TLS_URING.with(|cell| {
        let mut opt = cell.borrow_mut();
        if opt.is_none() {
            // Initialize with a modest queue depth; real tuning may increase this.
            *opt = Some(IoUring::new(1024).map_err(|e| MidgeError::IoError {
                source: std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("io_uring init: {}", e),
                ),
            })?);
        }

        let uring = opt.as_mut().unwrap();

        let iov_ptr = iovecs.as_ptr();
        let sqe = opcode::Writev::new(
            types::Fd(fd),
            iov_ptr as *const libc::iovec,
            iovecs.len() as _,
        )
        .build();

        unsafe {
            let mut sq = uring.submission();
            sq.push(&sqe).map_err(|_| MidgeError::IoError {
                source: std::io::Error::new(std::io::ErrorKind::Other, "submission queue full"),
            })?;
        }

        uring.submit_and_wait(1).map_err(|e| MidgeError::IoError {
            source: std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("submit_and_wait: {}", e),
            ),
        })?;

        if let Some(cqe) = uring.completion().next() {
            let res = cqe.result();
            if res < 0 {
                return Err(MidgeError::IoError {
                    source: std::io::Error::from_raw_os_error(-res),
                });
            }
            Ok(())
        } else {
            Err(MidgeError::IoError {
                source: std::io::Error::new(std::io::ErrorKind::Other, "no completion"),
            })
        }
    })
}
