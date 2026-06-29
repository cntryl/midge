//! Filesystem-backed SST implementation using `io::Fs` abstraction
//!
//! This module provides SST file reader and factory using the base `io::Fs` trait,
//! allowing for swappable filesystem implementations (Real, Mock, Chaos) for testing.

pub mod factory_io;
pub mod reader_io;

use std::io::Write;
use std::path::Path;

use crate::common::{MidgeError, MidgeResult};
use crate::sst::traits::DynSstWriter;

pub use factory_io::FsSstFactoryIo;
pub use reader_io::{SstFileIo, SstFileSummary};

/// Finalize an SST writer and atomically persist the resulting bytes to a path.
pub fn finish_writer_to_path(writer: Box<dyn DynSstWriter>, path: &Path) -> MidgeResult<()> {
    let finish_start = std::time::Instant::now();
    let bytes = writer.finish_bytes()?;
    let write_bytes = bytes.len() as u64;
    let parent = path.parent().unwrap_or_else(|| Path::new("."));

    let tmp = path.with_extension("tmp");
    fail::fail_point!("midge::sst::inject_no_space_on_finish_to_path", |_| Err(
        MidgeError::NoSpace("failpoint: no space while finalizing SST".to_string())
    ));

    let write_start = std::time::Instant::now();
    {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)
            .map_err(MidgeError::Io)?;
        file.write_all(&bytes).map_err(MidgeError::Io)?;

        let sync_start = std::time::Instant::now();
        file.sync_all().map_err(MidgeError::Io)?;
        let sync_ns = sync_start.elapsed().as_nanos();

        tracing::debug!(path = ?tmp, write_bytes, sync_ns, "sst temp file written and fsynced");
    }
    let write_ns = write_start.elapsed().as_nanos();

    let rename_start = std::time::Instant::now();
    std::fs::rename(&tmp, path).map_err(MidgeError::Io)?;
    let rename_ns = rename_start.elapsed().as_nanos();

    let dir_fsync_start = std::time::Instant::now();
    match std::fs::File::open(parent) {
        Ok(dir_f) => {
            if let Err(error) = dir_f.sync_all() {
                tracing::debug!("failed to fsync parent dir for sst: {}", error);
            }
        }
        Err(error) => {
            tracing::debug!("failed to open parent dir for fsync: {}", error);
        }
    }
    let dir_fsync_ns = dir_fsync_start.elapsed().as_nanos();

    tracing::info!(
        path = ?path,
        bytes = write_bytes,
        finish_total_ms = finish_start.elapsed().as_secs_f64() * 1000.0,
        write_ms = (write_ns as f64) / 1_000_000.0,
        rename_ms = (rename_ns as f64) / 1_000_000.0,
        dir_fsync_ms = (dir_fsync_ns as f64) / 1_000_000.0,
        "sst finished to path"
    );

    Ok(())
}
