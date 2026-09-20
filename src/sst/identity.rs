//! One owner for "is this file the SST the manifest names?".
//!
//! Recovery, intent replay, cloud startup recovery, prune proofs, flush
//! publication and `midge verify` all ask the same question, and before this
//! module each answered it slightly differently: some treated `size_bytes == 0`
//! as "unknown", others as a literal zero-length file; some rejected a manifest
//! entry with no `content_crc32c`, others skipped the check; only the prune
//! path compared key and sequence bounds. The differences are now a single
//! explicit [`ProofPolicy`] rather than an accident of which call site ran.
//!
//! Computing the identity and judging it are kept apart: callers stream bytes
//! through whichever I/O abstraction they already hold, then hand the result to
//! [`SstIdentity::verify_against`] and map one [`IdentityMismatch`] onto the
//! error kind their layer reports.

use crate::common::{MidgeError, MidgeResult};
use crate::sst::fs::SstFileSummary;

/// A borrowed view of the proofs a manifest entry records about one SST.
///
/// The manifest and the runtime message types carry the same fields in two
/// structs, so the checker borrows from either rather than forcing a clone.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ExpectedSst<'a> {
    pub(crate) name: &'a str,
    pub(crate) size_bytes: u64,
    pub(crate) content_crc32c: Option<u32>,
    pub(crate) smallest_key: Option<&'a [u8]>,
    pub(crate) largest_key: Option<&'a [u8]>,
    pub(crate) smallest_seq: Option<u64>,
    pub(crate) largest_seq: Option<u64>,
}

impl<'a> From<&'a crate::metadata::FileMeta> for ExpectedSst<'a> {
    fn from(meta: &'a crate::metadata::FileMeta) -> Self {
        Self {
            name: &meta.name,
            size_bytes: meta.size_bytes,
            content_crc32c: meta.content_crc32c,
            smallest_key: meta.smallest_key.as_deref(),
            largest_key: meta.largest_key.as_deref(),
            smallest_seq: meta.smallest_seq,
            largest_seq: meta.largest_seq,
        }
    }
}

impl<'a> From<&'a crate::runtime::FileMeta> for ExpectedSst<'a> {
    fn from(meta: &'a crate::runtime::FileMeta) -> Self {
        Self {
            name: &meta.name,
            size_bytes: meta.size_bytes,
            content_crc32c: meta.content_crc32c,
            smallest_key: meta.smallest_key.as_deref(),
            largest_key: meta.largest_key.as_deref(),
            smallest_seq: meta.smallest_seq,
            largest_seq: meta.largest_seq,
        }
    }
}

/// Physical identity of an SST file: its length and whole-file CRC32C.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SstIdentity {
    pub(crate) size_bytes: u64,
    pub(crate) crc32c: u32,
}

/// How strictly a manifest entry that omits a proof is judged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProofPolicy {
    /// Every proof the manifest can carry must be present and must match.
    ///
    /// Used where the answer is a verdict about the database rather than a
    /// decision to keep going: flush publication and `midge verify`.
    Required,
    /// A manifest entry written by an older writer may omit its proof.
    ///
    /// `size_bytes == 0` and `content_crc32c == None` mean "not recorded", so
    /// they are skipped instead of failing. Anything the manifest does record
    /// is still enforced. Used on recovery paths, where rejecting an entry
    /// that was never proven would discard readable data.
    Legacy,
}

/// Why a file is not the SST a manifest entry names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IdentityMismatch(String);

impl IdentityMismatch {
    fn new(reason: impl Into<String>) -> Self {
        Self(reason.into())
    }
}

impl std::fmt::Display for IdentityMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<IdentityMismatch> for String {
    fn from(mismatch: IdentityMismatch) -> Self {
        mismatch.0
    }
}

/// Chunk size for streaming CRC passes over an SST.
const READ_CHUNK_BYTES: usize = 1024 * 1024;

impl SstIdentity {
    /// Identity of an in-memory object body.
    pub(crate) fn of_bytes(data: &[u8]) -> Self {
        Self {
            size_bytes: data.len() as u64,
            crc32c: crc32c::crc32c(data),
        }
    }

    /// Identity of a local file, streamed with fixed stack space.
    pub(crate) fn of_path(path: &std::path::Path) -> MidgeResult<Self> {
        use std::io::Read as _;

        let mut file = std::fs::File::open(path)?;
        // Fixed stack space, independent of SST size; every byte stays covered.
        let mut buffer = [0_u8; 8192];
        let mut size_bytes = 0_u64;
        let mut crc32c = 0;
        loop {
            let count = file.read(&mut buffer)?;
            if count == 0 {
                return Ok(Self { size_bytes, crc32c });
            }
            size_bytes = size_bytes
                .checked_add(count as u64)
                .ok_or_else(|| MidgeError::ResourceLimit("SST size overflow".into()))?;
            crc32c = crc32c::crc32c_append(crc32c, &buffer[..count]);
        }
    }

    /// Identity of an opened file, read through the `io::File` abstraction.
    ///
    /// The caller supplies the length it pinned, so a file that grows or
    /// shrinks under the read is reported as a short read rather than
    /// silently producing a CRC over different bytes.
    pub(crate) fn of_file(
        file: &dyn crate::io::File,
        file_len: u64,
        deadline: Option<&crate::common::OperationDeadline>,
    ) -> MidgeResult<Self> {
        let mut crc32c = 0;
        let mut offset = 0_u64;
        while offset < file_len {
            if let Some(deadline) = deadline {
                if deadline.is_expired() {
                    return Err(MidgeError::Timeout(
                        "SST identity pass exceeded the operation deadline".into(),
                    ));
                }
            }
            let want = std::cmp::min(READ_CHUNK_BYTES as u64, file_len - offset);
            let chunk = file.read_at(offset, want)?;
            if chunk.len() as u64 != want {
                return Err(MidgeError::Corruption(format!(
                    "short read at offset {offset}: wanted {want} bytes, read {}",
                    chunk.len()
                )));
            }
            crc32c = crc32c::crc32c_append(crc32c, &chunk);
            offset += want;
        }
        Ok(Self {
            size_bytes: file_len,
            crc32c,
        })
    }

    /// Judge this identity, and optionally a decoded summary, against a
    /// manifest entry under one explicit policy.
    pub(crate) fn verify_against<'a>(
        &self,
        expected: impl Into<ExpectedSst<'a>>,
        summary: Option<&SstFileSummary>,
        policy: ProofPolicy,
    ) -> Result<(), IdentityMismatch> {
        let meta = expected.into();
        let name = meta.name;
        let size_recorded = policy == ProofPolicy::Required || meta.size_bytes != 0;
        if size_recorded && self.size_bytes != meta.size_bytes {
            return Err(IdentityMismatch::new(format!(
                "SST '{name}' size {} does not match manifest {}",
                self.size_bytes, meta.size_bytes
            )));
        }
        match meta.content_crc32c {
            Some(expected) if expected != self.crc32c => {
                return Err(IdentityMismatch::new(format!(
                    "SST '{name}' content CRC {:#010x} does not match manifest {expected:#010x}",
                    self.crc32c
                )));
            }
            None if policy == ProofPolicy::Required => {
                return Err(IdentityMismatch::new(format!(
                    "SST '{name}' is missing manifest content CRC"
                )));
            }
            _ => {}
        }
        if let Some(summary) = summary {
            verify_summary(name, summary, meta)?;
        }
        Ok(())
    }
}

/// Compare a decoded SST summary with the size and bounds the manifest records.
///
/// Used where the proof is the decoded summary itself rather than a byte pass,
/// such as the cloud prune guard, which verifies the CRC incrementally as it
/// streams and then confirms the decoded file describes the manifest entry.
pub(crate) fn verify_summary_against<'a>(
    summary: &SstFileSummary,
    expected: impl Into<ExpectedSst<'a>>,
    policy: ProofPolicy,
) -> Result<(), IdentityMismatch> {
    let meta = expected.into();
    let size_recorded = policy == ProofPolicy::Required || meta.size_bytes != 0;
    if size_recorded && summary.size_bytes != meta.size_bytes {
        return Err(IdentityMismatch::new(format!(
            "SST '{}' physical size {} does not match manifest {}",
            meta.name, summary.size_bytes, meta.size_bytes
        )));
    }
    verify_summary(meta.name, summary, meta)
}

/// Compare a decoded SST summary with the bounds the manifest records.
///
/// A bound the manifest does not record is not evidence of a mismatch, so it
/// is skipped under either policy.
fn verify_summary(
    name: &str,
    summary: &SstFileSummary,
    meta: ExpectedSst<'_>,
) -> Result<(), IdentityMismatch> {
    if meta
        .smallest_key
        .is_some_and(|key| summary.smallest_key.as_slice() != key)
    {
        return Err(IdentityMismatch::new(format!(
            "SST '{name}' smallest key does not match manifest"
        )));
    }
    if meta
        .largest_key
        .is_some_and(|key| summary.largest_key.as_slice() != key)
    {
        return Err(IdentityMismatch::new(format!(
            "SST '{name}' largest key does not match manifest"
        )));
    }
    if meta
        .smallest_seq
        .is_some_and(|sequence| summary.smallest_seq != sequence)
    {
        return Err(IdentityMismatch::new(format!(
            "SST '{name}' smallest sequence {} does not match manifest {:?}",
            summary.smallest_seq, meta.smallest_seq
        )));
    }
    if meta
        .largest_seq
        .is_some_and(|sequence| summary.largest_seq != sequence)
    {
        return Err(IdentityMismatch::new(format!(
            "SST '{name}' largest sequence {} does not match manifest {:?}",
            summary.largest_seq, meta.largest_seq
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::FileMeta;

    fn meta(size_bytes: u64, content_crc32c: Option<u32>) -> FileMeta {
        FileMeta {
            name: "000001.sst".to_string(),
            size_bytes,
            content_crc32c,
            ..FileMeta::default()
        }
    }

    fn identity(size_bytes: u64, crc32c: u32) -> SstIdentity {
        SstIdentity { size_bytes, crc32c }
    }

    #[test]
    fn should_accept_an_unrecorded_proof_when_policy_is_legacy() {
        // Arrange
        let entry = meta(0, None);

        // Act
        let verdict = identity(4096, 7).verify_against(&entry, None, ProofPolicy::Legacy);

        // Assert
        assert!(verdict.is_ok(), "{verdict:?}");
    }

    #[test]
    fn should_reject_an_unrecorded_crc_when_policy_is_required() {
        // Arrange
        let entry = meta(4096, None);

        // Act
        let mismatch = identity(4096, 7)
            .verify_against(&entry, None, ProofPolicy::Required)
            .unwrap_err();

        // Assert
        assert!(
            mismatch
                .to_string()
                .contains("missing manifest content CRC"),
            "{mismatch}"
        );
    }

    /// A manifest size of 0 means "not recorded" to the recovery paths, so a
    /// legacy check must not read it as "this file must be empty".
    #[test]
    fn should_not_read_an_unrecorded_size_as_an_empty_file() {
        // Arrange
        let entry = meta(0, Some(crc32c::crc32c(b"payload")));

        // Act
        let verdict =
            SstIdentity::of_bytes(b"payload").verify_against(&entry, None, ProofPolicy::Legacy);

        // Assert
        assert!(verdict.is_ok(), "{verdict:?}");
    }

    #[test]
    fn should_reject_a_recorded_crc_that_does_not_match() {
        // Arrange
        let entry = meta(7, Some(crc32c::crc32c(b"payload")));

        // Act
        let mismatch = SstIdentity::of_bytes(b"corrupt")
            .verify_against(&entry, None, ProofPolicy::Legacy)
            .unwrap_err();

        // Assert
        assert!(mismatch.to_string().contains("content CRC"), "{mismatch}");
    }

    #[test]
    fn should_reject_a_summary_whose_bounds_disagree_with_the_manifest() {
        // Arrange
        let entry = FileMeta {
            smallest_key: Some(b"a".to_vec()),
            largest_seq: Some(9),
            ..meta(4, Some(crc32c::crc32c(b"data")))
        };
        let summary = SstFileSummary {
            size_bytes: 4,
            smallest_key: b"z".to_vec(),
            largest_key: b"z".to_vec(),
            smallest_seq: 1,
            largest_seq: 9,
        };

        // Act
        let mismatch = SstIdentity::of_bytes(b"data")
            .verify_against(&entry, Some(&summary), ProofPolicy::Legacy)
            .unwrap_err();

        // Assert
        assert!(mismatch.to_string().contains("smallest key"), "{mismatch}");
    }
}
