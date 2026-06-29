//! Atomic staging helpers for filesystem-backed persistence.
//!
//! These helpers centralize the common pattern of:
//! write -> fsync -> atomic rename -> best-effort temp cleanup.
//!
//! They are intentionally small and generic so metadata, intent logs, leader
//! records, and cloud recovery bootstrap writes all follow the same lifecycle.

use crate::io::traits::{Durability, Fs, FsPath, OpenMode, OpenOptions};
use bytes::Bytes;
use std::sync::Arc;

fn cleanup_temp_file(fs: &Arc<dyn Fs>, temp_path: &FsPath) {
    if let Err(error) = fs.remove_file(temp_path) {
        tracing::debug!(
            path = ?temp_path,
            error = ?error,
            "staging temp cleanup skipped or failed"
        );
    }
}

/// Stage a byte buffer to `temp_path`, sync it, rename it into `target_path`,
/// and remove the temp file on failure.
pub(crate) fn stage_bytes<E, M>(
    fs: &Arc<dyn Fs>,
    temp_path: &FsPath,
    target_path: &FsPath,
    data: &[u8],
    map_error: M,
) -> Result<(), E>
where
    M: Fn(String) -> E,
{
    stage_bytes_with_hook(fs, temp_path, target_path, data, || Ok(()), map_error)
}

/// Stage a byte buffer with a hook that runs after the temp file is synced and
/// before the atomic rename.
pub(crate) fn stage_bytes_with_hook<E, F, M>(
    fs: &Arc<dyn Fs>,
    temp_path: &FsPath,
    target_path: &FsPath,
    data: &[u8],
    before_rename: F,
    map_error: M,
) -> Result<(), E>
where
    F: FnOnce() -> Result<(), E>,
    M: Fn(String) -> E,
{
    let result = (|| {
        let mut file = fs
            .open(
                temp_path,
                OpenOptions {
                    mode: OpenMode::ReadWrite,
                    create: true,
                    create_new: false,
                    truncate: true,
                },
            )
            .map_err(|error| {
                map_error(format!(
                    "failed to open staging file {temp_path:?}: {error:?}"
                ))
            })?;

        file.write_at(0, Bytes::copy_from_slice(data))
            .map_err(|error| {
                map_error(format!(
                    "failed to write staging file {temp_path:?}: {error:?}"
                ))
            })?;
        file.sync(Durability::Durable).map_err(|error| {
            map_error(format!(
                "failed to sync staging file {temp_path:?}: {error:?}"
            ))
        })?;
        drop(file);

        before_rename()?;

        fs.rename_atomic(temp_path, target_path).map_err(|error| {
            map_error(format!(
                "failed to rename staging file {temp_path:?} -> {target_path:?}: {error:?}"
            ))
        })?;

        Ok(())
    })();

    if result.is_err() {
        cleanup_temp_file(fs, temp_path);
    }

    result
}
