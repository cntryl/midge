//! The `wal/LEDGER` journal that persists the [`WalLedger`].
//!
//! ## Format
//!
//! A 12-byte header, the magic `MDGLEDGR` then a `u32` LE format version, is
//! followed by frames `[u32 len LE][u32 crc32c LE][payload]`. The checksum
//! covers the length bytes and the payload, so a zero-filled region never
//! reads as a valid frame. The payload is a JSON-encoded [`Frame`]. The first
//! frame is a `Snapshot`; every later frame is an `Edit` that loading applies
//! to it with [`WalLedger::apply`], and an edit that does not apply is
//! corruption.
//!
//! ## Durability
//!
//! LEDGER is only ever published whole, as one snapshot:
//! [`LedgerStore::create`], [`LedgerStore::compact`] and [`LedgerStore::open`]
//! write `LEDGER.tmp`, sync it, rename it over LEDGER and sync the directory.
//! LEDGER is never truncated in place. [`LedgerStore::append`] adds one frame
//! and syncs the file, so only the final frame can be torn by a crash. A
//! damaged frame is that torn tail when no complete, checksum-valid frame
//! follows it; when one does, acknowledged edits would be lost, so it is
//! corruption. A damaged snapshot is always corruption.
//!
//! Every open and every [`COMPACT_AFTER_EDITS`] appends replace the journal
//! with a snapshot, so its size stays bounded however long the WAL runs.

use super::ledger::{LedgerEdit, WalLedger};
use crate::common::{MidgeError, MidgeResult};
use crate::io::{Durability, Fs, FsError, FsPath, OpenMode, OpenOptions};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// The ledger journal, relative to the WAL directory's filesystem root.
pub(crate) const LEDGER_FILE: &str = "LEDGER";
/// Where a new LEDGER image is staged before it is renamed over LEDGER.
const LEDGER_TEMP_FILE: &str = "LEDGER.tmp";
const MAGIC: [u8; 8] = *b"MDGLEDGR";
const VERSION: u32 = 1;
const HEADER_LEN: usize = MAGIC.len() + 4;
/// The `len` and `crc32c` words before each payload.
const FRAME_HEADER_LEN: usize = 8;
/// Edits appended after a snapshot before the store compacts.
pub(crate) const COMPACT_AFTER_EDITS: usize = 1024;

/// How LEDGER ended when it was loaded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LedgerTail {
    /// Every byte after the header belongs to a valid frame.
    Clean,
    /// The final frame was torn by a crash during its append and is ignored.
    /// [`LedgerStore::open`] drops it before appending.
    Torn,
}

/// One LEDGER frame's payload.
#[derive(Serialize, Deserialize)]
enum Frame {
    Snapshot(WalLedger),
    Edit(LedgerEdit),
}

/// The open LEDGER journal of one WAL directory.
///
/// The store keeps the ledger LEDGER holds and refuses an edit that does not
/// apply to it, so it never writes a journal that cannot be loaded. After a
/// failed write the content of LEDGER is unknown: the store republishes its
/// ledger as a snapshot (it heals) before it appends again.
pub(crate) struct LedgerStore {
    /// Rooted at the WAL directory.
    fs: Arc<dyn Fs>,
    /// The state LEDGER holds.
    ledger: WalLedger,
    /// LEDGER's length in bytes.
    len: u64,
    /// Edits appended after the snapshot.
    edits: usize,
    /// A write failed, so `ledger` and `len` may not describe LEDGER.
    failed: bool,
}

/// A LEDGER image parsed up to its last valid frame.
struct Parsed {
    ledger: WalLedger,
    tail: LedgerTail,
}

/// What the bytes at one offset of LEDGER hold.
enum NextFrame {
    /// Nothing: the image ends here.
    End,
    /// A torn final frame, ignored.
    Torn,
    /// A valid frame, and the offset after it.
    Frame { frame: Frame, end: usize },
}

impl LedgerStore {
    /// Read LEDGER. `Ok(None)` if absent. A torn final frame (short, or bad
    /// checksum, as the last thing in the file) is reported as `Torn` and
    /// ignored; a bad frame followed by a valid frame is corruption (`Err`).
    ///
    /// Loading makes nothing durable: a state read after a process crash can
    /// still be lost to a power loss until [`LedgerStore::open`] returns.
    pub(crate) fn load(fs: &Arc<dyn Fs>) -> MidgeResult<Option<(WalLedger, LedgerTail)>> {
        let Some(bytes) = read_ledger(fs.as_ref())? else {
            return Ok(None);
        };
        let parsed = parse(&bytes)?;
        Ok(Some((parsed.ledger, parsed.tail)))
    }

    /// Create LEDGER for a new database holding `ledger` as its snapshot:
    /// write `LEDGER.tmp`, sync it, rename it over `LEDGER`, sync the directory.
    ///
    /// Refuses an uninitialized `ledger`, and refuses to replace an existing
    /// LEDGER.
    pub(crate) fn create(fs: Arc<dyn Fs>, ledger: &WalLedger) -> MidgeResult<Self> {
        if fs
            .exists(&FsPath::new(LEDGER_FILE))
            .map_err(FsError::into_midge)?
        {
            return Err(MidgeError::InvalidArgument(format!(
                "WAL ledger {LEDGER_FILE} already exists"
            )));
        }
        if !ledger.is_initialized() {
            return Err(MidgeError::InvalidArgument(
                "cannot persist an uninitialized WAL ledger".into(),
            ));
        }
        let mut store = Self {
            fs,
            ledger: ledger.clone(),
            len: 0,
            edits: 0,
            failed: true,
        };
        store.compact()?;
        Ok(store)
    }

    /// Open the existing LEDGER for appending; `Ok(None)` if absent.
    ///
    /// Publishes the loaded ledger, without a torn tail, as a new snapshot
    /// LEDGER (staged, synced, renamed, directory synced). The state loaded is
    /// therefore durable once this returns, even when it was read from a
    /// crashed process's unsynced writes, and the journal starts small again.
    pub(crate) fn open(fs: Arc<dyn Fs>) -> MidgeResult<Option<Self>> {
        let Some(bytes) = read_ledger(fs.as_ref())? else {
            return Ok(None);
        };
        let parsed = parse(&bytes)?;
        let mut store = Self {
            fs,
            ledger: parsed.ledger,
            len: 0,
            edits: 0,
            failed: true,
        };
        store.compact()?;
        Ok(Some(store))
    }

    /// The ledger as persisted.
    pub(crate) fn ledger(&self) -> &WalLedger {
        &self.ledger
    }

    /// Append one edit frame and sync (durable when this returns).
    ///
    /// An edit that does not apply to the ledger is refused, as corruption,
    /// without writing anything. After an earlier failed write the store
    /// heals first; if it cannot, nothing is written. Once
    /// [`COMPACT_AFTER_EDITS`] edits follow the snapshot, the store compacts;
    /// a failed compaction does not fail the durable append, and the next
    /// append heals.
    pub(crate) fn append(&mut self, edit: &LedgerEdit) -> MidgeResult<()> {
        self.heal()?;
        let mut next = self.ledger.clone();
        next.apply(edit)?;
        let frame = encode_frame(&Frame::Edit(edit.clone()))?;
        if let Err(error) = self.write_frame(&frame) {
            self.failed = true;
            return Err(error);
        }
        self.ledger = next;
        self.len += byte_len(&frame);
        self.edits += 1;
        if self.edits >= COMPACT_AFTER_EDITS {
            if let Err(error) = self.compact() {
                tracing::warn!(%error, "WAL ledger compaction failed; the next edit retries it");
            }
        }
        Ok(())
    }

    /// Make LEDGER describe the store again after a failed write, by
    /// republishing the ledger as a snapshot. Does nothing otherwise.
    ///
    /// # Errors
    ///
    /// Returns the publication error; the store stays unhealed.
    pub(crate) fn heal(&mut self) -> MidgeResult<()> {
        if self.failed {
            self.compact()?;
        }
        Ok(())
    }

    /// Replace LEDGER with a single snapshot of the ledger (tmp + sync +
    /// rename + dir sync), then append after it. Never truncates LEDGER in
    /// place. LEDGER afterwards holds exactly the ledger, so a successful
    /// compaction also clears an earlier failed write.
    pub(crate) fn compact(&mut self) -> MidgeResult<()> {
        let mut image = Vec::with_capacity(HEADER_LEN);
        image.extend_from_slice(&MAGIC);
        image.extend_from_slice(&VERSION.to_le_bytes());
        image.extend(encode_frame(&Frame::Snapshot(self.ledger.clone()))?);
        self.failed = true;
        publish(&self.fs, &image)?;
        self.len = byte_len(&image);
        self.edits = 0;
        self.failed = false;
        Ok(())
    }

    fn write_frame(&self, frame: &[u8]) -> MidgeResult<()> {
        let mut file = self
            .fs
            .open(
                &FsPath::new(LEDGER_FILE),
                OpenOptions {
                    mode: OpenMode::ReadWrite,
                    create: false,
                    create_new: false,
                    truncate: false,
                },
            )
            .map_err(FsError::into_midge)?;
        let len = file.len().map_err(FsError::into_midge)?;
        if len != self.len {
            return Err(corruption(format!(
                "holds {len} bytes where {} were written",
                self.len
            )));
        }
        file.write_at(len, Bytes::copy_from_slice(frame))
            .map_err(FsError::into_midge)?;
        file.sync(Durability::Durable).map_err(FsError::into_midge)
    }
}

fn corruption(detail: impl std::fmt::Display) -> MidgeError {
    MidgeError::Corruption(format!("WAL ledger {LEDGER_FILE} {detail}"))
}

fn byte_len(bytes: &[u8]) -> u64 {
    u64::try_from(bytes.len()).unwrap_or(u64::MAX)
}

fn read_ledger(fs: &dyn Fs) -> MidgeResult<Option<Bytes>> {
    match crate::io::read_all(fs, &FsPath::new(LEDGER_FILE)) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(FsError::NotFound(_)) => Ok(None),
        Err(error) => Err(error.into_midge()),
    }
}

/// Make `image` LEDGER's durable content without touching the current LEDGER
/// until the rename.
fn publish(fs: &Arc<dyn Fs>, image: &[u8]) -> MidgeResult<()> {
    crate::io::staging::stage_bytes_typed(
        fs,
        &FsPath::new(LEDGER_TEMP_FILE),
        &FsPath::new(LEDGER_FILE),
        image,
        || Ok(()),
    )
}

fn encode_frame(frame: &Frame) -> MidgeResult<Vec<u8>> {
    let payload = serde_json::to_vec(frame).map_err(|error| {
        MidgeError::Internal(format!("cannot encode a WAL ledger frame: {error}"))
    })?;
    let length = u32::try_from(payload.len())
        .map_err(|_| MidgeError::Internal("a WAL ledger frame exceeds 4 GiB".into()))?
        .to_le_bytes();
    let checksum = crc32c::crc32c_append(crc32c::crc32c(&length), &payload);
    let mut bytes = Vec::with_capacity(FRAME_HEADER_LEN + payload.len());
    bytes.extend_from_slice(&length);
    bytes.extend_from_slice(&checksum.to_le_bytes());
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}

/// The payload and total length of the frame starting at `bytes[0]`, when it
/// is complete and its checksum matches.
fn checked_frame(bytes: &[u8]) -> Option<(&[u8], usize)> {
    let header = bytes.get(..FRAME_HEADER_LEN)?;
    let length = [header[0], header[1], header[2], header[3]];
    let checksum = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
    let end = usize::try_from(u32::from_le_bytes(length))
        .ok()?
        .checked_add(FRAME_HEADER_LEN)?;
    let payload = bytes.get(FRAME_HEADER_LEN..end)?;
    (crc32c::crc32c_append(crc32c::crc32c(&length), payload) == checksum).then_some((payload, end))
}

/// Whether a complete, checksum-valid, decodable frame starts anywhere in
/// `bytes`.
fn holds_valid_frame(bytes: &[u8]) -> bool {
    (0..bytes.len()).any(|start| {
        checked_frame(&bytes[start..])
            .is_some_and(|(payload, _)| serde_json::from_slice::<Frame>(payload).is_ok())
    })
}

fn next_frame(bytes: &[u8], at: usize) -> MidgeResult<NextFrame> {
    let rest = &bytes[at..];
    if rest.is_empty() {
        return Ok(NextFrame::End);
    }
    let Some((payload, len)) = checked_frame(rest) else {
        // Short, zero-filled, or failing its checksum. Only one append is
        // ever in flight, so a torn frame is the last one; a valid frame
        // after this one proves it is not torn.
        if holds_valid_frame(&rest[1..]) {
            return Err(corruption(format!(
                "has a damaged frame at byte {at} before a valid frame"
            )));
        }
        return Ok(NextFrame::Torn);
    };
    let frame = serde_json::from_slice(payload)
        .map_err(|error| corruption(format!("frame at byte {at} does not decode: {error}")))?;
    Ok(NextFrame::Frame {
        frame,
        end: at + len,
    })
}

fn parse(bytes: &[u8]) -> MidgeResult<Parsed> {
    let Some(header) = bytes.get(..HEADER_LEN) else {
        return Err(corruption("is shorter than its header"));
    };
    if header[..MAGIC.len()] != MAGIC {
        return Err(corruption("does not start with the ledger magic"));
    }
    let version = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
    if version != VERSION {
        return Err(MidgeError::CompatibilityError(format!(
            "WAL ledger {LEDGER_FILE} has format version {version}; this build reads {VERSION}"
        )));
    }
    let (mut ledger, mut at) = match next_frame(bytes, HEADER_LEN)? {
        NextFrame::Frame {
            frame: Frame::Snapshot(ledger),
            end,
        } => (ledger, end),
        NextFrame::Frame {
            frame: Frame::Edit(_),
            ..
        } => return Err(corruption("opens with an edit instead of a snapshot")),
        NextFrame::End | NextFrame::Torn => return Err(corruption("has no complete snapshot")),
    };
    if !ledger.is_initialized() {
        return Err(corruption("holds an uninitialized snapshot"));
    }
    loop {
        match next_frame(bytes, at)? {
            NextFrame::End => {
                return Ok(Parsed {
                    ledger,
                    tail: LedgerTail::Clean,
                })
            }
            NextFrame::Torn => {
                return Ok(Parsed {
                    ledger,
                    tail: LedgerTail::Torn,
                })
            }
            NextFrame::Frame {
                frame: Frame::Edit(edit),
                end,
            } => {
                ledger.apply(&edit).map_err(|error| {
                    corruption(format!("edit at byte {at} does not apply: {error}"))
                })?;
                at = end;
            }
            NextFrame::Frame {
                frame: Frame::Snapshot(_),
                ..
            } => return Err(corruption(format!("holds a second snapshot at byte {at}"))),
        }
    }
}

#[cfg(test)]
mod tests;
