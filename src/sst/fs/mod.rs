//! Filesystem-backed SST implementation using `io::Fs` abstraction
//!
//! This module provides SST file reader and factory using the base `io::Fs` trait,
//! allowing for swappable real and mock filesystem implementations in tests.

pub mod factory_io;
pub mod reader_io;
mod scratch;

use std::io::Read;
use std::path::Path;

use crate::common::{MidgeError, MidgeResult};
use crate::sst::traits::DynSstWriter;

pub use factory_io::FsSstFactoryIo;
pub use reader_io::{SstFileIo, SstFileSummary};

/// Compute immutable SST length and CRC with fixed stack space.
pub(crate) fn file_identity(path: &Path) -> MidgeResult<(u64, u32)> {
    let mut file = std::fs::File::open(path)?;
    // Fixed stack space, independent of SST size; every byte remains covered.
    let mut buffer = [0_u8; 8192];
    let mut size = 0_u64;
    let mut checksum = 0;
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            return Ok((size, checksum));
        }
        size = size
            .checked_add(count as u64)
            .ok_or_else(|| MidgeError::ResourceLimit("SST size overflow".into()))?;
        checksum = crc32c::crc32c_append(checksum, &buffer[..count]);
    }
}

/// Finalize an SST writer and atomically persist the resulting bytes to a path.
///
/// # Errors
///
/// Returns an error if finalizing the writer, writing the temp file, syncing, or renaming fails.
pub fn finish_writer_to_path(writer: Box<dyn DynSstWriter>, path: &Path) -> MidgeResult<()> {
    writer.finish_to_path(path)
}

/// Atomically persist finalized SST bytes. This is public within the crate so
/// an SST writer can stream a finalized scratch file without reconstructing a
/// whole byte vector first.
pub(crate) fn persist_sst_bytes_to_path(bytes: &[u8], path: &Path) -> MidgeResult<()> {
    let mut source = std::io::Cursor::new(bytes);
    persist_sst_stream_to_path(&mut source, path)
}

/// Atomically persist a finalized SST byte stream.
pub(crate) fn persist_sst_stream_to_path(source: &mut dyn Read, path: &Path) -> MidgeResult<()> {
    let finish_start = std::time::Instant::now();
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    crate::failpoints::fail_point!("midge::sst::inject_no_space_on_finish_to_path", |_| Err(
        MidgeError::NoSpace("failpoint: no space while finalizing SST".to_string())
    ));

    let target_name = path
        .file_name()
        .ok_or(MidgeError::InvalidPath)?
        .to_string_lossy()
        .into_owned();
    let target_path = crate::io::FsPath::new(&target_name);
    let temp_path = crate::io::FsPath::new(format!("{target_name}.tmp"));
    let fs: std::sync::Arc<dyn crate::io::Fs> =
        std::sync::Arc::new(crate::io::RealFs::new(parent).map_err(MidgeError::from)?);
    let write_bytes =
        crate::io::staging::stage_stream(&fs, &temp_path, &target_path, source, |message| {
            MidgeError::from(std::io::Error::other(message))
        })?;

    tracing::info!(
        path = ?path,
        bytes = write_bytes,
        finish_total_ms = finish_start.elapsed().as_secs_f64() * 1000.0,
        "sst finished to path"
    );

    Ok(())
}

#[cfg(test)]
mod regression_tests;
