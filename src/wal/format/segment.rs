//! WAL segment format v2 (#843): a fixed header, record frames, and a
//! footer frame that seals the segment.
//!
//! ## Header
//!
//! [`HEADER_LEN`] bytes at offset 0, little-endian: the magic `MWSEGHDR`, a
//! `u32` version (2), the segment `id`, the writer `epoch`, the ledger
//! `holder` (16 bytes), the `first_seq` every record is at least, and `prev`,
//! the [`ChainLink`] of the segment before (`id`, `last_seq`, 16-byte
//! digest). Forty zero bytes follow, then a `crc32c` of everything before
//! it. The header is not a frame; it is written and synced before any ledger
//! edit names the segment.
//!
//! ## Footer
//!
//! A sealed segment ends with one more frame in the ordinary
//! `[len][crc32c][payload]` framing, whose payload starts `MF` 1 and holds
//! the segment's facts: `id`, `record_count`, `first_seq`, `last_seq`,
//! `data_len` (the footer frame's offset), the highest sequence per column
//! family, and the xxh3-128 digest of every byte before the footer. Nothing
//! may follow it. Facts count outer records; a transaction batch counts once,
//! at its commit sequence.
//!
//! [`SegmentScan`] is the one interpretation of these bytes. It does no I/O:
//! every walker over a segment feeds it the header, then each verified frame,
//! then each decoded record's sequence and family.

use super::link::{ChainLink, Digest128, OpenFacts, SegmentId};
use crate::common::{MidgeError, MidgeResult};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use xxhash_rust::xxh3::Xxh3;

/// Length of the segment header.
pub const HEADER_LEN: usize = 128;
const HEADER_MAGIC: [u8; 8] = *b"MWSEGHDR";
const SEGMENT_VERSION: u32 = 2;
/// Bytes covered by the header checksum.
const HEADER_CHECKED_LEN: usize = HEADER_LEN - 4;
/// The zero bytes reserved after `prev`.
const HEADER_RESERVED: std::ops::Range<usize> = 84..HEADER_CHECKED_LEN;
const FOOTER_MAGIC: [u8; 2] = *b"MF";
const FOOTER_VERSION: u8 = 1;

/// Whether a segment file of `len` bytes holds no record: it is at most a
/// header, as a creation that crashed or nothing appended leaves it.
#[must_use]
pub(crate) fn holds_no_records(len: u64) -> bool {
    len <= HEADER_LEN as u64
}

/// What a sealed segment's bytes say it is (#843): the id and writer epoch
/// its header names and the last sequence its records hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SegmentIdentity {
    pub(crate) id: SegmentId,
    pub(crate) epoch: u64,
    pub(crate) last_seq: u64,
}

/// The fixed header at the start of every segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentHeader {
    pub id: SegmentId,
    pub epoch: u64,
    pub holder: [u8; 16],
    pub first_seq: u64,
    pub prev: ChainLink,
}

impl SegmentHeader {
    /// The facts this header fixes besides its holder.
    #[must_use]
    pub(crate) fn open_facts(&self) -> OpenFacts {
        OpenFacts {
            id: self.id,
            epoch: self.epoch,
            first_seq: self.first_seq,
            prev: self.prev,
        }
    }

    /// The header bytes, checksum included.
    #[must_use]
    pub fn encode(&self) -> [u8; HEADER_LEN] {
        let mut bytes = [0_u8; HEADER_LEN];
        bytes[0..8].copy_from_slice(&HEADER_MAGIC);
        bytes[8..12].copy_from_slice(&SEGMENT_VERSION.to_le_bytes());
        bytes[12..20].copy_from_slice(&self.id.to_le_bytes());
        bytes[20..28].copy_from_slice(&self.epoch.to_le_bytes());
        bytes[28..44].copy_from_slice(&self.holder);
        bytes[44..52].copy_from_slice(&self.first_seq.to_le_bytes());
        bytes[52..60].copy_from_slice(&self.prev.id.to_le_bytes());
        bytes[60..68].copy_from_slice(&self.prev.last_seq.to_le_bytes());
        bytes[68..84].copy_from_slice(&self.prev.digest);
        let crc = crc32c::crc32c(&bytes[..HEADER_CHECKED_LEN]);
        bytes[HEADER_CHECKED_LEN..].copy_from_slice(&crc.to_le_bytes());
        bytes
    }

    /// Decode the header at the start of `bytes`.
    ///
    /// # Errors
    ///
    /// `Corruption` when the bytes are short, carry another magic or
    /// version, or fail the checksum.
    pub fn decode(bytes: &[u8]) -> MidgeResult<Self> {
        let Some(bytes) = bytes.get(..HEADER_LEN) else {
            return Err(corruption(format!(
                "WAL segment header needs {HEADER_LEN} bytes, found {}",
                bytes.len()
            )));
        };
        if bytes[0..8] != HEADER_MAGIC {
            return Err(corruption(
                "WAL segment does not start with its header magic",
            ));
        }
        let version = u32_at(bytes, 8);
        if version != SEGMENT_VERSION {
            return Err(corruption(format!(
                "WAL segment format version {version} is not {SEGMENT_VERSION}"
            )));
        }
        let crc = u32_at(bytes, HEADER_CHECKED_LEN);
        if crc32c::crc32c(&bytes[..HEADER_CHECKED_LEN]) != crc {
            return Err(corruption("WAL segment header fails its checksum"));
        }
        if bytes[HEADER_RESERVED].iter().any(|byte| *byte != 0) {
            return Err(corruption("WAL segment header has nonzero reserved bytes"));
        }
        let mut holder = [0_u8; 16];
        holder.copy_from_slice(&bytes[28..44]);
        let mut digest = [0_u8; 16];
        digest.copy_from_slice(&bytes[68..84]);
        Ok(Self {
            id: u64_at(bytes, 12),
            epoch: u64_at(bytes, 20),
            holder,
            first_seq: u64_at(bytes, 44),
            prev: ChainLink {
                id: u64_at(bytes, 52),
                last_seq: u64_at(bytes, 60),
                digest,
            },
        })
    }
}

/// The records a segment holds so far, as its writer or a scan counts them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SegmentTail {
    pub(crate) record_count: u64,
    pub(crate) first_seq: Option<u64>,
    pub(crate) last_seq: Option<u64>,
    pub(crate) max_seq_per_cf: BTreeMap<u32, u64>,
}

impl SegmentTail {
    /// Whether a record at `seq` may come next in segment `header` after
    /// this tail (#843): a segment's sequences strictly increase from its
    /// header's first one. The one admission rule its writer and every scan
    /// apply.
    ///
    /// # Errors
    ///
    /// Why `seq` may not come next.
    pub(crate) fn admit(&self, header: &SegmentHeader, seq: u64) -> Result<(), String> {
        self.admit_all(header, [seq])
    }

    /// Whether records at `seqs`, in order, may come next: [`Self::admit`]
    /// for a batch the writer appends at once.
    ///
    /// # Errors
    ///
    /// Why the first refused sequence may not come next.
    pub(crate) fn admit_all(
        &self,
        header: &SegmentHeader,
        seqs: impl IntoIterator<Item = u64>,
    ) -> Result<(), String> {
        let mut last = self.last_seq;
        for seq in seqs {
            if seq < header.first_seq {
                return Err(format!(
                    "conflicting or repeated WAL sequence {seq}: segment {} starts at {}",
                    header.id, header.first_seq
                ));
            }
            if let Some(last) = last.filter(|last| seq <= *last) {
                return Err(format!(
                    "conflicting or repeated WAL sequence {seq} after {last}"
                ));
            }
            last = Some(seq);
        }
        Ok(())
    }

    /// Count a record [`Self::admit`] accepted; fixtures that build
    /// deliberately invalid segments count without admitting.
    pub(crate) fn count(&mut self, seq: u64, cf: u32) {
        self.record_count = self.record_count.saturating_add(1);
        self.first_seq.get_or_insert(seq);
        self.last_seq = Some(self.last_seq.map_or(seq, |last| last.max(seq)));
        let max = self.max_seq_per_cf.entry(cf).or_insert(seq);
        *max = (*max).max(seq);
    }
}

/// What sealing a segment fixes, as its footer and the ledger record it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SealedFacts {
    /// The segment's id, last sequence (its predecessor's when it holds no
    /// record) and digest.
    pub(crate) link: ChainLink,
    pub(crate) record_count: u64,
    /// Bytes before the footer, header included.
    pub(crate) data_len: u64,
    /// The highest sequence per column family, by family.
    pub(crate) max_seq_per_cf: Vec<(u32, u64)>,
}

/// The digest of a segment's bytes before its footer, header first, in the
/// order they were written (#843): what its footer and chain link seal. Its
/// writer extends it with each write that lands, a scan with each frame.
#[derive(Clone)]
pub(crate) struct SegmentDigest {
    hasher: Xxh3,
    data_len: u64,
}

impl SegmentDigest {
    /// The digest of exactly `bytes`.
    pub(crate) fn of(bytes: &[u8]) -> Self {
        let mut digest = Self {
            hasher: Xxh3::new(),
            data_len: 0,
        };
        digest.update(bytes);
        digest
    }

    /// Extend the digest with `bytes`, the segment's next ones.
    pub(crate) fn update(&mut self, bytes: &[u8]) {
        self.hasher.update(bytes);
        self.data_len = self.data_len.saturating_add(bytes.len() as u64);
    }

    /// The bytes the digest covers.
    pub(crate) fn data_len(&self) -> u64 {
        self.data_len
    }

    /// The digest of those bytes.
    pub(crate) fn value(&self) -> Digest128 {
        self.hasher.digest128().to_le_bytes()
    }
}

/// A sealed segment's final frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Footer {
    pub(crate) id: SegmentId,
    pub(crate) record_count: u64,
    pub(crate) first_seq: u64,
    pub(crate) last_seq: u64,
    pub(crate) data_len: u64,
    pub(crate) max_seq_per_cf: Vec<(u32, u64)>,
    pub(crate) digest: Digest128,
}

impl Footer {
    /// The footer sealing segment `header` holding `tail`, whose bytes
    /// before the footer `digest` covers. The one construction its writer,
    /// a scan, recovery's sealing and fixtures share: a segment without
    /// records names its header's first sequence and its predecessor's last.
    pub(crate) fn sealing(
        header: &SegmentHeader,
        tail: &SegmentTail,
        digest: &SegmentDigest,
    ) -> Self {
        Self {
            id: header.id,
            record_count: tail.record_count,
            first_seq: tail.first_seq.unwrap_or(header.first_seq),
            last_seq: tail.last_seq.unwrap_or(header.prev.last_seq),
            data_len: digest.data_len(),
            max_seq_per_cf: tail
                .max_seq_per_cf
                .iter()
                .map(|(cf, seq)| (*cf, *seq))
                .collect(),
            digest: digest.value(),
        }
    }

    pub(crate) fn sealed_facts(&self) -> SealedFacts {
        SealedFacts {
            link: ChainLink {
                id: self.id,
                last_seq: self.last_seq,
                digest: self.digest,
            },
            record_count: self.record_count,
            data_len: self.data_len,
            max_seq_per_cf: self.max_seq_per_cf.clone(),
        }
    }

    fn encode_payload(&self) -> Vec<u8> {
        let mut payload = Vec::with_capacity(64 + self.max_seq_per_cf.len() * 12);
        payload.extend_from_slice(&FOOTER_MAGIC);
        payload.push(FOOTER_VERSION);
        for value in [
            self.id,
            self.record_count,
            self.first_seq,
            self.last_seq,
            self.data_len,
        ] {
            payload.extend_from_slice(&value.to_le_bytes());
        }
        let families = u32::try_from(self.max_seq_per_cf.len()).unwrap_or(u32::MAX);
        payload.extend_from_slice(&families.to_le_bytes());
        for (cf, seq) in &self.max_seq_per_cf {
            payload.extend_from_slice(&cf.to_le_bytes());
            payload.extend_from_slice(&seq.to_le_bytes());
        }
        payload.extend_from_slice(&self.digest);
        payload
    }

    /// The footer as a complete frame.
    ///
    /// # Errors
    ///
    /// `InvalidArgument` when the frame exceeds the WAL frame limits.
    pub(crate) fn encode_frame(&self) -> MidgeResult<Vec<u8>> {
        let mut frame = Vec::new();
        crate::wal::frame::append_frame(&mut frame, &self.encode_payload())?;
        Ok(frame)
    }

    /// Decode a footer frame's payload.
    ///
    /// # Errors
    ///
    /// `Corruption` when the payload is not a whole footer.
    pub(crate) fn decode_payload(payload: &[u8]) -> MidgeResult<Self> {
        if !is_footer_payload(payload) {
            return Err(corruption("WAL frame is not a segment footer"));
        }
        let mut input = &payload[3..];
        let mut next_u64 = || take(&mut input, 8).map(|bytes| u64_at(bytes, 0));
        let (id, record_count, first_seq, last_seq, data_len) = (
            next_u64()?,
            next_u64()?,
            next_u64()?,
            next_u64()?,
            next_u64()?,
        );
        let families = usize::try_from(u32_at(take(&mut input, 4)?, 0)).unwrap_or(usize::MAX);
        if families > input.len() / 12 {
            return Err(corruption(
                "WAL segment footer family count exceeds its bytes",
            ));
        }
        let mut max_seq_per_cf = Vec::with_capacity(families);
        for _ in 0..families {
            let entry = take(&mut input, 12)?;
            max_seq_per_cf.push((u32_at(entry, 0), u64_at(entry, 4)));
        }
        if max_seq_per_cf.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
            return Err(corruption("WAL segment footer families are not in order"));
        }
        let mut digest = [0_u8; 16];
        digest.copy_from_slice(take(&mut input, 16)?);
        if !input.is_empty() {
            return Err(corruption("WAL segment footer has trailing bytes"));
        }
        Ok(Self {
            id,
            record_count,
            first_seq,
            last_seq,
            data_len,
            max_seq_per_cf,
            digest,
        })
    }
}

/// Whether a frame payload is a segment footer rather than a record.
#[must_use]
pub fn is_footer_payload(payload: &[u8]) -> bool {
    payload.len() >= 3 && payload[..2] == FOOTER_MAGIC && payload[2] == FOOTER_VERSION
}

/// Whether a verified frame payload decodes as a record or a footer: what a
/// segment may hold after its header.
#[must_use]
pub(crate) fn decodes_as_segment_frame(payload: &[u8]) -> bool {
    crate::wal::encoding::decode_view(payload).is_ok() || Footer::decode_payload(payload).is_ok()
}

/// What one frame of a segment is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScannedFrame {
    /// A record: the caller decodes it and reports it to
    /// [`SegmentScan::observe`].
    Record,
    /// The footer, which matched every record before it.
    Footer,
}

/// The one interpretation of a segment's bytes. See the module docs.
#[derive(Clone)]
pub(crate) struct SegmentScan {
    header: SegmentHeader,
    digest: SegmentDigest,
    tail: SegmentTail,
    footer: Option<Footer>,
}

impl std::fmt::Debug for SegmentScan {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SegmentScan")
            .field("header", &self.header)
            .field("data_len", &self.digest.data_len())
            .field("tail", &self.tail)
            .field("footer", &self.footer)
            .finish_non_exhaustive()
    }
}

impl SegmentScan {
    /// Start a scan at the segment's header bytes.
    ///
    /// # Errors
    ///
    /// The header's decode error.
    pub(crate) fn start(header_bytes: &[u8]) -> MidgeResult<Self> {
        let header = SegmentHeader::decode(header_bytes)?;
        Ok(Self {
            header,
            digest: SegmentDigest::of(&header_bytes[..HEADER_LEN]),
            tail: SegmentTail::default(),
            footer: None,
        })
    }

    pub(crate) fn header(&self) -> &SegmentHeader {
        &self.header
    }

    pub(crate) fn tail(&self) -> &SegmentTail {
        &self.tail
    }

    /// The footer, once the scan reached it.
    pub(crate) fn footer(&self) -> Option<&Footer> {
        self.footer.as_ref()
    }

    /// Take the next complete, checksum-verified frame `payload`. A record
    /// frame joins the digest; the footer must match everything scanned.
    ///
    /// # Errors
    ///
    /// `Corruption` for a frame after the footer, or a footer that does not
    /// match the records before it.
    pub(crate) fn frame(&mut self, payload: &[u8]) -> MidgeResult<ScannedFrame> {
        if self.footer.is_some() {
            return Err(MidgeError::Corruption(format!(
                "bytes follow the footer of WAL segment {}",
                self.header.id
            )));
        }
        if is_footer_payload(payload) {
            let footer = Footer::decode_payload(payload)?;
            let expected = self.sealing();
            if footer != expected {
                return Err(MidgeError::Corruption(format!(
                    "the footer of WAL segment {} does not match its records: footer {footer:?}, records {expected:?}",
                    self.header.id
                )));
            }
            self.footer = Some(footer);
            return Ok(ScannedFrame::Footer);
        }
        let frame_header = crate::wal::frame::frame_header(payload)
            .map_err(|_| corruption("WAL frame exceeds the frame limits"))?;
        self.digest.update(&frame_header);
        self.digest.update(payload);
        Ok(ScannedFrame::Record)
    }

    /// Report the record just scanned: its sequence and column family.
    ///
    /// # Errors
    ///
    /// `Corruption` when the sequence does not strictly increase or falls
    /// below the header's first sequence.
    pub(crate) fn observe(&mut self, seq: u64, cf: u32) -> MidgeResult<()> {
        self.check_record(seq)?;
        self.tail.count(seq, cf);
        Ok(())
    }

    /// Whether a record frame at `seq` may come next, without scanning it:
    /// a caller that must stop between checking a record and accepting it
    /// (a resumable inspection) checks first and scans once it accepted.
    ///
    /// # Errors
    ///
    /// `Corruption` when the segment is already sealed, or the sequence does
    /// not strictly increase or falls below the header's first sequence.
    pub(crate) fn check_record(&self, seq: u64) -> MidgeResult<()> {
        if self.footer.is_some() {
            return Err(MidgeError::Corruption(format!(
                "bytes follow the footer of WAL segment {}",
                self.header.id
            )));
        }
        self.tail
            .admit(&self.header, seq)
            .map_err(MidgeError::Corruption)
    }

    /// The footer sealing what the scan has read so far.
    pub(crate) fn sealing(&self) -> Footer {
        Footer::sealing(&self.header, &self.tail, &self.digest)
    }
}

/// Every record frame of `bytes`, a whole segment, decoded; the footer is
/// skipped.
///
/// # Errors
///
/// The first decode, frame or footer error.
#[cfg(any(test, feature = "internal-testing"))]
pub fn records(bytes: &[u8]) -> MidgeResult<Vec<crate::wal::WalRecord>> {
    let mut scan = SegmentScan::start(bytes)?;
    let mut records = Vec::new();
    let mut pos = HEADER_LEN as u64;
    loop {
        let step = crate::wal::frame::next_frame(
            &bytes,
            &"segment",
            pos,
            crate::wal::frame::FrameLimits::default(),
        )
        .map_err(crate::wal::frame::FrameError::into_error)?;
        let crate::wal::frame::FrameStep::Frame { payload, next_pos } = step else {
            return Ok(records);
        };
        if scan.frame(&payload)? == ScannedFrame::Record {
            let record = crate::wal::encoding::decode(payload.as_ref())?;
            scan.observe(record.seq, record.cf_id)?;
            records.push(record);
        }
        pos = next_pos;
    }
}

/// The writer epoch in `bytes`' header.
///
/// # Errors
///
/// The header's decode error.
#[cfg(any(test, feature = "internal-testing"))]
pub fn header_epoch(bytes: &[u8]) -> MidgeResult<u64> {
    Ok(SegmentHeader::decode(bytes)?.epoch)
}

/// The chain link the footer of `bytes` seals, or `None` for a segment that
/// is still open.
///
/// # Errors
///
/// The first decode, frame or footer error.
#[cfg(any(test, feature = "internal-testing"))]
pub fn footer_link(bytes: &[u8]) -> MidgeResult<Option<ChainLink>> {
    let mut scan = SegmentScan::start(bytes)?;
    let mut pos = HEADER_LEN as u64;
    loop {
        let step = crate::wal::frame::next_frame(
            &bytes,
            &"segment",
            pos,
            crate::wal::frame::FrameLimits::default(),
        )
        .map_err(crate::wal::frame::FrameError::into_error)?;
        let crate::wal::frame::FrameStep::Frame { payload, next_pos } = step else {
            return Ok(scan.footer().map(|footer| footer.sealed_facts().link));
        };
        if scan.frame(&payload)? == ScannedFrame::Record {
            let record = crate::wal::encoding::decode_view(payload.as_ref())?;
            scan.observe(record.seq, record.cf_id)?;
        }
        pos = next_pos;
    }
}

/// The holder fixtures write into segment headers and record in the ledgers
/// they create, so their segments belong to their ledgers.
#[cfg(any(test, feature = "internal-testing"))]
pub const FIXTURE_HOLDER: [u8; 16] = *b"midge-fixture-01";

/// Builds segment bytes for tests and fixtures: a header, frames, and a
/// footer whose facts come from whatever frames decode, so a deliberately
/// corrupt body still seals.
#[cfg(any(test, feature = "internal-testing"))]
#[derive(Debug, Clone)]
pub struct SegmentBuilder {
    header: SegmentHeader,
    body: Vec<u8>,
}

#[cfg(any(test, feature = "internal-testing"))]
impl SegmentBuilder {
    /// Segment `id` written by `epoch`, linked to `prev` and starting just
    /// past it.
    #[must_use]
    pub fn new(id: SegmentId, epoch: u64, prev: ChainLink) -> Self {
        Self {
            header: SegmentHeader {
                id,
                epoch,
                holder: FIXTURE_HOLDER,
                first_seq: prev.last_seq.saturating_add(1),
                prev,
            },
            body: Vec::new(),
        }
    }

    /// A segment with exactly `header`.
    #[must_use]
    pub fn from_header(header: SegmentHeader) -> Self {
        Self {
            header,
            body: Vec::new(),
        }
    }

    /// Start the segment at `first_seq` instead.
    #[must_use]
    pub fn first_seq(mut self, first_seq: u64) -> Self {
        self.header.first_seq = first_seq;
        self
    }

    /// The header as built so far.
    #[must_use]
    pub fn header(&self) -> SegmentHeader {
        self.header
    }

    /// Append `record` as a frame.
    ///
    /// # Panics
    ///
    /// Panics if the record does not encode.
    #[must_use]
    pub fn record(mut self, record: &crate::wal::WalRecord) -> Self {
        let payload = crate::wal::encoding::encode(record).expect("encode fixture record");
        crate::wal::frame::append_frame(&mut self.body, &payload).expect("frame fixture record");
        self
    }

    /// Append raw bytes, frames or not.
    #[must_use]
    pub fn bytes(mut self, bytes: &[u8]) -> Self {
        self.body.extend_from_slice(bytes);
        self
    }

    /// The length of the body appended so far.
    #[must_use]
    pub fn body_len(&self) -> usize {
        self.body.len()
    }

    /// The segment still open: its header and body.
    #[must_use]
    pub fn open(&self) -> Vec<u8> {
        let mut bytes = self.header.encode().to_vec();
        bytes.extend_from_slice(&self.body);
        bytes
    }

    /// The segment sealed, and its link.
    ///
    /// # Panics
    ///
    /// Panics if the footer does not encode.
    #[must_use]
    pub fn seal(&self) -> (Vec<u8>, ChainLink) {
        let mut bytes = self.open();
        let footer = lenient_footer(&bytes).expect("a built header decodes");
        bytes.extend(footer.encode_frame().expect("encode fixture footer"));
        (bytes, footer.sealed_facts().link)
    }
}

/// The footer a fixture gives `bytes`, a header and a body that may be
/// deliberately invalid: facts counted from whatever frames decode, over a
/// digest of every byte.
///
/// # Errors
///
/// The header's decode error.
#[cfg(any(test, feature = "internal-testing"))]
pub(crate) fn lenient_footer(bytes: &[u8]) -> MidgeResult<Footer> {
    let header = SegmentHeader::decode(bytes)?;
    let tail = lenient_tail(&bytes[HEADER_LEN..]);
    Ok(Footer::sealing(&header, &tail, &SegmentDigest::of(bytes)))
}

/// The records a fixture body holds, up to its first frame that does not
/// decode, counted without ordering checks.
#[cfg(any(test, feature = "internal-testing"))]
fn lenient_tail(body: &[u8]) -> SegmentTail {
    let mut tail = SegmentTail::default();
    let mut pos = 0;
    while let Ok(crate::wal::frame::FrameStep::Frame { payload, next_pos }) =
        crate::wal::frame::next_frame(
            &body,
            &"fixture",
            pos,
            crate::wal::frame::FrameLimits::default(),
        )
    {
        let Ok(record) = crate::wal::encoding::decode_view(&payload) else {
            break;
        };
        tail.count(record.seq, record.cf_id);
        pos = next_pos;
    }
    tail
}

fn take<'a>(input: &mut &'a [u8], len: usize) -> MidgeResult<&'a [u8]> {
    if input.len() < len {
        return Err(corruption("WAL segment footer is truncated"));
    }
    let (head, rest) = input.split_at(len);
    *input = rest;
    Ok(head)
}

fn u32_at(bytes: &[u8], at: usize) -> u32 {
    let mut word = [0_u8; 4];
    word.copy_from_slice(&bytes[at..at + 4]);
    u32::from_le_bytes(word)
}

fn u64_at(bytes: &[u8], at: usize) -> u64 {
    let mut word = [0_u8; 8];
    word.copy_from_slice(&bytes[at..at + 8]);
    u64::from_le_bytes(word)
}

fn corruption(message: impl Into<String>) -> MidgeError {
    MidgeError::Corruption(message.into())
}

#[cfg(test)]
mod tests;
