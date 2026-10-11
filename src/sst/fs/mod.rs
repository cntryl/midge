//! Filesystem-backed SST implementation using `io::Fs` abstraction
//!
//! This module provides SST file reader and factory using the base `io::Fs` trait,
//! allowing for swappable real and mock filesystem implementations in tests.

use crate::io::FsError;
pub mod factory_io;
pub mod reader_io;
mod scratch;

use std::io::Read;
use std::path::Path;
use std::sync::Arc;

use crate::common::{MidgeError, MidgeResult};
use crate::io::{Fs, FsPath};
use crate::sst::traits::DynSstWriter;

pub use factory_io::FsSstFactoryIo;
pub use reader_io::{SstFileIo, SstFileSummary};

/// The SST directory below the database root. Every runtime SST location is
/// a root-relative path under it, so one injected root addresses them all.
pub(crate) const SST_DIR: &str = "sst";

/// Non-authoritative flush outputs and build scratch awaiting publication,
/// named within [`SST_DIR`] and as a root-relative path.
pub(crate) const STAGING_DIR_NAME: &str = ".flush-staging";
pub(crate) const FLUSH_STAGING_DIR: &str = "sst/.flush-staging";

/// Overlap-repair runs, which never enter the manifest, named within
/// [`SST_DIR`] and as a root-relative path.
pub(crate) const REPAIR_DIR_NAME: &str = ".compaction-repair";
pub(crate) const REPAIR_SCRATCH_DIR: &str = "sst/.compaction-repair";

/// Root-relative location of the SST named `name`.
pub(crate) fn sst_location(name: &str) -> std::path::PathBuf {
    Path::new(SST_DIR).join(name)
}

/// Root filesystem key of the SST named `name`.
pub(crate) fn sst_key(name: &str) -> FsPath {
    FsPath::new(format!("{SST_DIR}/{name}"))
}

/// Compute an SST identity through the same filesystem that published it.
pub(crate) fn identity_with_fs(
    fs: &Arc<dyn Fs>,
    path: &Path,
) -> MidgeResult<crate::sst::identity::SstIdentity> {
    let path = fs_relative_sst_path(fs, path)?;
    let size = fs.metadata(&path).map_err(FsError::into_midge)?.len;
    let file = fs
        .open(
            &path,
            crate::io::OpenOptions {
                mode: crate::io::OpenMode::ReadOnly,
                create: false,
                create_new: false,
                truncate: false,
            },
        )
        .map_err(FsError::into_midge)?;
    crate::sst::identity::SstIdentity::of_file(file.as_ref(), size, None)
}

/// Compute immutable SST length and CRC through the filesystem that published it.
pub(crate) fn file_identity_with_fs(fs: &Arc<dyn Fs>, path: &Path) -> MidgeResult<(u64, u32)> {
    let identity = identity_with_fs(fs, path)?;
    Ok((identity.size_bytes, identity.crc32c))
}

/// Finalize an SST writer and atomically persist the resulting bytes to a path.
///
/// # Errors
///
/// Returns an error if finalizing the writer, writing the temp file, syncing, or renaming fails.
pub fn finish_writer_to_path(writer: Box<dyn DynSstWriter>, path: &Path) -> MidgeResult<()> {
    writer.finish_to_path(path)
}

/// Map an SST target onto the key space of `fs`.
///
/// A relative target is already a key below the filesystem root: the engine
/// names every SST location that way (`sst/<name>`), so mapping it touches
/// neither the host nor the process working directory. An absolute target is
/// accepted only by a host-rooted filesystem, and only below its root. A
/// target outside it is rejected rather than silently rewritten into the
/// root: publishing an SST somewhere other than where the caller named it
/// would leave the manifest pointing at a file that does not exist.
///
/// Components stay exactly as written. [`crate::io::RealFs`] rejects a
/// symlink in any component, so an in-root symlink fails closed instead of
/// redirecting the publish to a path no reader could open.
///
/// Every failure here is an engine or deployment fault reached from flush and
/// compaction rather than something an API caller asked for, so they are
/// reported as [`MidgeError::Internal`] ([`crate::common::Severity::Defect`])
/// instead of a caller error that would be documented as pointless to retry.
pub(crate) fn fs_relative_sst_path(fs: &Arc<dyn Fs>, path: &Path) -> MidgeResult<FsPath> {
    let relative = if path.is_absolute() {
        let addressing = fs.host_addressing().ok_or_else(|| {
            MidgeError::Internal(format!(
                "SST target {} is a host path, but the filesystem has no host root",
                path.display()
            ))
        })?;
        addressing.relative(path).ok_or_else(|| {
            MidgeError::Internal(format!(
                "SST target {} lies outside the filesystem root {}",
                path.display(),
                addressing.root.display()
            ))
        })?
    } else {
        path
    };

    // Reject rather than drop traversal components: a rooted filesystem
    // silently discards them, which would publish the SST somewhere other
    // than where the caller named it.
    let mut parts = Vec::new();
    for component in relative.components() {
        match component {
            std::path::Component::Normal(part) => {
                parts.push(part.to_str().ok_or_else(|| {
                    MidgeError::Internal(format!(
                        "SST target {} is not valid UTF-8",
                        path.display()
                    ))
                })?);
            }
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir
            | std::path::Component::RootDir
            | std::path::Component::Prefix(_) => {
                return Err(MidgeError::Internal(format!(
                    "SST target {} escapes the filesystem root",
                    path.display()
                )))
            }
        }
    }
    if parts.is_empty() {
        return Err(MidgeError::Internal(format!(
            "SST target {} names the filesystem root rather than a file",
            path.display()
        )));
    }
    Ok(FsPath::new(parts.join("/")))
}

/// Atomically persist finalized SST bytes through `fs`. This is public within
/// the crate so an SST writer can stream a finalized scratch file without
/// reconstructing a whole byte vector first.
pub(crate) fn persist_sst_bytes_to_path(
    fs: &Arc<dyn Fs>,
    bytes: &[u8],
    path: &Path,
) -> MidgeResult<()> {
    let mut source = std::io::Cursor::new(bytes);
    persist_sst_stream_to_path(fs, &mut source, path)
}

/// Atomically persist a finalized SST byte stream through `fs`.
pub(crate) fn persist_sst_stream_to_path(
    fs: &Arc<dyn Fs>,
    source: &mut dyn Read,
    path: &Path,
) -> MidgeResult<()> {
    let finish_start = std::time::Instant::now();
    crate::failpoints::fail_point!("midge::sst::inject_no_space_on_finish_to_path", |_| Err(
        MidgeError::NoSpace("failpoint: no space while finalizing SST".to_string())
    ));

    let target_path = fs_relative_sst_path(fs, path)?;
    let temp_path = FsPath::new(format!("{}.tmp", target_path.0));
    let write_bytes =
        crate::io::staging::stage_stream(fs, &temp_path, &target_path, source, |message| {
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
