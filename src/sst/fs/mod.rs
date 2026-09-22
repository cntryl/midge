//! Filesystem-backed SST implementation using `io::Fs` abstraction
//!
//! This module provides SST file reader and factory using the base `io::Fs` trait,
//! allowing for swappable real and mock filesystem implementations in tests.

pub mod factory_io;
pub mod reader_io;
mod scratch;

use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::common::{MidgeError, MidgeResult};
use crate::io::{Fs, FsPath};
use crate::sst::traits::DynSstWriter;

pub use factory_io::FsSstFactoryIo;
pub use reader_io::{SstFileIo, SstFileSummary};

/// Compute immutable SST length and CRC with fixed stack space.
pub(crate) fn file_identity(path: &Path) -> MidgeResult<(u64, u32)> {
    let identity = crate::sst::identity::SstIdentity::of_path(path)?;
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

/// Resolve an SST target into a host path whose *ancestors* are canonical.
///
/// A relative target is resolved against `anchor`, the directory the
/// filesystem's own root was resolved from, so the two are read in the same
/// frame even if the host process later changes working directory.
///
/// Only the target's existing ancestors are canonicalized, so that a root
/// recorded in canonical form (as [`crate::io::RealFs`] records it) still
/// matches a target that reaches the same directory through a symlink or an
/// uncanonical prefix. The final component is deliberately left unresolved:
/// [`crate::io::RealFs`] rejects a symlink in *any* component, including the
/// last, and resolving the target here would turn that fail-closed rejection
/// into a publish redirected to a path the caller never named — one the
/// manifest would not record and no reader could open.
fn canonical_host_path(anchor: Option<&Path>, path: &Path) -> MidgeResult<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        let anchor = match anchor {
            Some(anchor) => anchor.to_path_buf(),
            None => std::env::current_dir().map_err(MidgeError::Io)?,
        };
        anchor.join(path)
    };
    let (Some(parent), Some(name)) = (absolute.parent(), absolute.file_name()) else {
        return Err(MidgeError::Internal(format!(
            "SST target {} names no file",
            path.display()
        )));
    };
    let name = name.to_os_string();
    let mut suffix: Vec<std::ffi::OsString> = Vec::new();
    let mut current = parent;
    loop {
        if let Ok(resolved) = std::fs::canonicalize(current) {
            let mut resolved = resolved;
            for component in suffix.iter().rev() {
                resolved.push(component);
            }
            resolved.push(name);
            return Ok(resolved);
        }
        let (Some(parent), Some(component)) = (current.parent(), current.file_name()) else {
            return Err(MidgeError::Internal(format!(
                "SST target {} has no resolvable ancestor directory",
                path.display()
            )));
        };
        suffix.push(component.to_os_string());
        current = parent;
    }
}

/// Map an SST target path onto the path space of `fs`.
///
/// Filesystems rooted on the host only address paths inside their root, so a
/// target outside it is rejected rather than silently rewritten into the root:
/// publishing an SST somewhere other than where the caller named it would
/// leave the manifest pointing at a file that does not exist. Rootless
/// backends (in-memory mocks, object stores) own their key space and take the
/// path as written.
///
/// Every failure here is an engine or deployment fault reached from flush and
/// compaction rather than something an API caller asked for, so they are
/// reported as [`MidgeError::Internal`] ([`crate::common::Severity::Defect`])
/// instead of a caller error that would be documented as pointless to retry.
pub(crate) fn fs_relative_sst_path(fs: &Arc<dyn Fs>, path: &Path) -> MidgeResult<FsPath> {
    let Some(addressing) = fs.host_addressing() else {
        // A rootless filesystem owns its key space, so the caller's own path
        // string is already the key it addresses.
        let path = path.to_str().ok_or_else(|| {
            MidgeError::Internal(format!("SST target {} is not valid UTF-8", path.display()))
        })?;
        return Ok(FsPath::new(path));
    };
    let relative = canonical_host_path(addressing.anchor, path)?
        .strip_prefix(addressing.root)
        .map_err(|_| {
            MidgeError::Internal(format!(
                "SST target {} lies outside the filesystem root {}",
                path.display(),
                addressing.root.display()
            ))
        })?
        .to_path_buf();

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
                    "SST target {} escapes the filesystem root {}",
                    path.display(),
                    addressing.root.display()
                )))
            }
        }
    }
    if parts.is_empty() {
        return Err(MidgeError::Internal(format!(
            "SST target {} names the filesystem root {} rather than a file",
            path.display(),
            addressing.root.display()
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

/// Atomically persist finalized SST bytes for a writer that carries no
/// injected filesystem.
///
/// Only writers created outside [`FsSstFactoryIo`] reach this: production
/// writers carry their factory's `Arc<dyn Fs>` and persist through it. The
/// staging, fsync, rename, and parent-directory sync sequence is the same one
/// [`persist_sst_stream_to_path`] performs; only the filesystem differs.
pub(crate) fn persist_sst_bytes_with_host_fs(bytes: &[u8], path: &Path) -> MidgeResult<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let name = path.file_name().ok_or_else(|| {
        MidgeError::Internal(format!("SST target {} names no file", path.display()))
    })?;
    let fs: Arc<dyn Fs> = Arc::new(crate::io::RealFs::new(parent).map_err(MidgeError::from)?);
    // Address the SST inside the root just created for its parent. Re-using
    // `path` verbatim would re-resolve a relative target against the root's
    // own anchor and double the parent prefix.
    let target = fs.host_addressing().map_or_else(
        || path.to_path_buf(),
        |addressing| addressing.root.join(name),
    );
    persist_sst_bytes_to_path(&fs, bytes, &target)
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
