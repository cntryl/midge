use super::range::{
    lookup_range_index, read_range_header, validate_range_index, write_range_index_file,
    RangeHeader,
};
use super::{
    consider_lookup, op_heap_bytes, op_primary_key_bytes, IntentLookup, OrdinalOp, SpillDiskBudget,
    SpillDiskCharge, SpillRun, TransactionMemoryPool, TransactionOp, MAX_FRAME_BYTES,
    RUN_HEADER_LEN, RUN_MAGIC, RUN_VERSION, SPARSE_INDEX_STRIDE,
};
use crate::common::{MidgeError, MidgeResult};
use bytes::Bytes;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

/// Read-ahead window for one open spill file.
///
/// Runs are read as long chains of small CRC frames, so an unbuffered handle
/// costs several syscalls per field. The window stays small because a spilled
/// transaction keeps one handle per run.
const RUN_READ_BUFFER_BYTES: usize = 16 * 1024;

#[cfg(test)]
thread_local! {
    /// Per-test counter of spill files opened for reading.
    static RUN_FILE_OPENS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(super) fn reset_run_file_opens() {
    RUN_FILE_OPENS.with(|opens| opens.set(0));
}

#[cfg(test)]
pub(super) fn run_file_opens() -> usize {
    RUN_FILE_OPENS.with(std::cell::Cell::get)
}

/// Buffered, position-tracking reader over one immutable spill file.
///
/// Every read path used to seek by absolute offset and then ask the kernel for
/// the resulting position. Tracking the offset here keeps `BufReader`'s window
/// intact across frames and removes an `lseek` per field.
#[derive(Debug)]
pub(super) struct RunFile {
    reader: BufReader<File>,
    position: u64,
}

impl RunFile {
    pub(super) fn open(path: &Path) -> MidgeResult<Self> {
        let file = File::open(path)?;
        #[cfg(test)]
        RUN_FILE_OPENS.with(|opens| opens.set(opens.get().saturating_add(1)));
        Ok(Self {
            reader: BufReader::with_capacity(RUN_READ_BUFFER_BYTES, file),
            position: 0,
        })
    }

    pub(super) fn seek_to(&mut self, offset: u64) -> MidgeResult<()> {
        if offset == self.position {
            return Ok(());
        }
        // A relative seek keeps buffered bytes when the target is still inside
        // the window, which is the common case while walking one sparse chunk.
        match (i64::try_from(offset), i64::try_from(self.position)) {
            (Ok(target), Ok(current)) if target.checked_sub(current).is_some() => {
                self.reader.seek_relative(target - current)?;
            }
            _ => {
                self.reader.seek(SeekFrom::Start(offset))?;
            }
        }
        self.position = offset;
        Ok(())
    }

    #[must_use]
    pub(super) fn position(&self) -> u64 {
        self.position
    }
}

impl Read for RunFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let read = self.reader.read(buf)?;
        self.position = self
            .position
            .saturating_add(u64::try_from(read).unwrap_or(u64::MAX));
        Ok(read)
    }
}

/// One run's open handles plus its decoded sparse index.
///
/// Point lookups used to reopen both files and re-walk the whole sparse index
/// for every key. The reader keeps the handles open for the life of the run and
/// binary-searches an in-memory index whose bytes are charged to the pool.
#[derive(Debug)]
pub(super) struct RunReader {
    data: RunFile,
    range: RunFile,
    header: RunHeader,
    range_header: RangeHeader,
    sparse: Option<Vec<(Bytes, u64)>>,
    charged_bytes: usize,
    pool: Arc<TransactionMemoryPool>,
}

impl RunReader {
    pub(super) fn open(run: &SpillRun) -> MidgeResult<Self> {
        let mut data = RunFile::open(&run.path)?;
        let header = read_header(&mut data)?;
        if header.record_count != run.record_count {
            return Err(MidgeError::Corruption(
                "transaction spill record count changed".to_string(),
            ));
        }
        let mut range = RunFile::open(&run.range_path)?;
        let range_header = read_range_header(&mut range)?;
        // Validating and decoding the index once replaces the per-lookup walk
        // that previously re-verified every sparse frame.
        let entries = read_sparse_entries(&mut data, &header)?;
        let charge = sparse_charge_bytes(&entries);
        let (sparse, charged_bytes) = if run.pool.try_reserve(charge) {
            (Some(entries), charge)
        } else {
            // The pool is the memory bound the transaction agreed to. Fall back
            // to reading the index from the file rather than exceeding it.
            (None, 0)
        };
        Ok(Self {
            data,
            range,
            header,
            range_header,
            sparse,
            charged_bytes,
            pool: Arc::clone(&run.pool),
        })
    }

    pub(super) fn header(&self) -> RunHeader {
        self.header
    }

    pub(super) fn sparse_start(&mut self, target: Option<&[u8]>) -> MidgeResult<u64> {
        if let Some(entries) = &self.sparse {
            return Ok(sparse_start_in(entries, target));
        }
        sparse_start_for_key(&mut self.data, &self.header, target)
    }

    pub(super) fn sparse_offsets(&mut self) -> MidgeResult<Vec<u64>> {
        if let Some(entries) = &self.sparse {
            return Ok(entries.iter().map(|(_, offset)| *offset).collect());
        }
        read_sparse_offsets(&mut self.data, &self.header)
    }
}

impl Drop for RunReader {
    fn drop(&mut self) {
        self.pool.release(self.charged_bytes);
        self.charged_bytes = 0;
    }
}

fn sparse_charge_bytes(entries: &[(Bytes, u64)]) -> usize {
    entries.iter().fold(0_usize, |total, (key, _)| {
        total
            .saturating_add(key.len())
            .saturating_add(size_of::<(Bytes, u64)>())
    })
}

/// Offset of the last indexed record whose key is strictly below `target`.
///
/// Runs are key-sorted, so every record for the target lies at or after that
/// offset however many index strides the target's own records span.
fn sparse_start_in(entries: &[(Bytes, u64)], target: Option<&[u8]>) -> u64 {
    let Some(target) = target else {
        return RUN_HEADER_LEN as u64;
    };
    let index = entries.partition_point(|(key, _)| key.as_ref() < target);
    if index == 0 {
        RUN_HEADER_LEN as u64
    } else {
        entries[index - 1].1
    }
}

#[cfg(test)]
pub(super) fn write_run(
    spill_dir: &Path,
    txn_id: u64,
    run_number: usize,
    ops: &mut [OrdinalOp],
) -> MidgeResult<SpillRun> {
    write_run_with_budget(
        spill_dir,
        txn_id,
        run_number,
        ops,
        None,
        &Arc::new(TransactionMemoryPool::new(usize::MAX)),
    )
}

pub(super) fn write_run_with_budget(
    spill_dir: &Path,
    txn_id: u64,
    run_number: usize,
    ops: &mut [OrdinalOp],
    budget: Option<&SpillDiskBudget>,
    pool: &Arc<TransactionMemoryPool>,
) -> MidgeResult<SpillRun> {
    let path = spill_dir.join(format!("{txn_id:016x}-{run_number:08x}.run"));
    let temp_path = path.with_extension("run.tmp");
    let range_path = path.with_extension("ranges");
    let range_temp_path = path.with_extension("ranges.tmp");
    let disk_charge = budget
        .map(|budget| {
            let bytes = spill_size_bound(ops)?;
            budget.0.admit_local_scratch_bytes(bytes)?;
            Ok::<_, MidgeError>(std::sync::Arc::new(SpillDiskCharge {
                budget: budget.clone(),
                bytes,
                paths: [
                    path.clone(),
                    temp_path.clone(),
                    range_path.clone(),
                    range_temp_path.clone(),
                ],
            }))
        })
        .transpose()?;
    fs::create_dir_all(spill_dir)?;
    let result = write_run_file(&temp_path, ops)
        .and_then(|()| write_range_index_file(&range_temp_path, ops))
        .and_then(|()| {
            fs::rename(&range_temp_path, &range_path)?;
            fs::rename(&temp_path, &path)?;
            // Spill files are private pre-commit scratch. WAL commit is their
            // durability boundary, so no parent-directory fsync is required.
            Ok(())
        });
    if let Err(error) = result {
        let _ = fs::remove_file(&temp_path);
        let _ = fs::remove_file(&range_temp_path);
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(&range_path);
        return Err(error);
    }
    Ok(SpillRun {
        path,
        range_path,
        record_count: ops.len(),
        pool: Arc::clone(pool),
        reader: Mutex::new(None),
        _disk_charge: disk_charge,
    })
}

fn spill_size_bound(ops: &[OrdinalOp]) -> MidgeResult<u64> {
    let largest_range_end = ops
        .iter()
        .filter_map(|entry| match &entry.op {
            TransactionOp::DeleteRange { end_key, .. } => Some(end_key.len()),
            _ => None,
        })
        .max()
        .unwrap_or(0);
    let mut bytes = usize_to_u64(RUN_HEADER_LEN + super::RANGE_HEADER_LEN)?;
    for entry in ops {
        // Data frame, ordinal frame, and a sparse frame for every entry
        // conservatively cover any key ordering selected by run sorting.
        let mut fields = [
            82,
            op_heap_bytes(&entry.op),
            entry.primary_key().len(),
            0,
            0,
            0,
            0,
        ];
        if let TransactionOp::DeleteRange {
            start_key, end_key, ..
        } = &entry.op
        {
            // Each tree node can inherit the largest endpoint from another
            // range. Charge that bound as well as its own keys and table slot.
            fields[3..].copy_from_slice(&[56, start_key.len(), end_key.len(), largest_range_end]);
        }
        for field in fields {
            bytes = bytes.checked_add(usize_to_u64(field)?).ok_or_else(|| {
                MidgeError::NoSpace("transaction spill disk reservation overflow".into())
            })?;
        }
    }
    Ok(bytes)
}

fn write_run_file(path: &Path, ops: &mut [OrdinalOp]) -> MidgeResult<()> {
    ops.sort_unstable_by(|left, right| {
        left.primary_key()
            .cmp(right.primary_key())
            .then_with(|| left.ordinal.cmp(&right.ordinal))
    });
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(&[0; RUN_HEADER_LEN])?;

    let mut ordinal_offsets = Vec::with_capacity(ops.len());
    let mut sparse_entries = Vec::with_capacity(ops.len().div_ceil(SPARSE_INDEX_STRIDE));
    for (index, ordinal_op) in ops.iter().enumerate() {
        let offset = file.stream_position()?;
        write_op_frame(&mut file, ordinal_op)?;
        ordinal_offsets.push((ordinal_op.ordinal, offset));
        if index % SPARSE_INDEX_STRIDE == 0 {
            sparse_entries.push((op_primary_key_bytes(&ordinal_op.op), offset));
        }
    }

    let ordinal_table_offset = file.stream_position()?;
    ordinal_offsets.sort_unstable_by_key(|(ordinal, _)| *ordinal);
    for (ordinal, offset) in ordinal_offsets {
        let mut payload = Vec::with_capacity(16);
        payload.extend_from_slice(&ordinal.to_le_bytes());
        payload.extend_from_slice(&offset.to_le_bytes());
        write_frame(&mut file, &payload)?;
    }

    let sparse_index_offset = file.stream_position()?;
    for (key, offset) in &sparse_entries {
        write_sparse_frame(&mut file, key, *offset)?;
    }

    let header = encode_header(
        ops.len(),
        ordinal_table_offset,
        sparse_index_offset,
        sparse_entries.len(),
    )?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&header)?;
    // Spill runs are pre-commit scratch that startup deletes wholesale, so an
    // fsync here buys no durability and only costs write latency.
    Ok(())
}

fn encode_header(
    record_count: usize,
    ordinal_table_offset: u64,
    sparse_index_offset: u64,
    sparse_count: usize,
) -> MidgeResult<[u8; RUN_HEADER_LEN]> {
    let mut header = [0_u8; RUN_HEADER_LEN];
    header[..8].copy_from_slice(RUN_MAGIC);
    header[8..12].copy_from_slice(&RUN_VERSION.to_le_bytes());
    header[12..20].copy_from_slice(&usize_to_u64(record_count)?.to_le_bytes());
    header[20..28].copy_from_slice(&ordinal_table_offset.to_le_bytes());
    header[28..36].copy_from_slice(&sparse_index_offset.to_le_bytes());
    header[36..44].copy_from_slice(&usize_to_u64(sparse_count)?.to_le_bytes());
    let crc = crc32c::crc32c(&header[..44]);
    header[44..48].copy_from_slice(&crc.to_le_bytes());
    Ok(header)
}

#[derive(Debug, Clone, Copy)]
pub(super) struct RunHeader {
    pub(super) record_count: usize,
    pub(super) ordinal_table_offset: u64,
    pub(super) sparse_index_offset: u64,
    pub(super) sparse_count: usize,
}

pub(super) fn read_header(file: &mut RunFile) -> MidgeResult<RunHeader> {
    let mut header = [0_u8; RUN_HEADER_LEN];
    file.seek_to(0)?;
    file.read_exact(&mut header)?;
    if &header[..8] != RUN_MAGIC {
        return Err(MidgeError::Corruption(
            "transaction spill run has invalid magic".to_string(),
        ));
    }
    let version = read_u32_at(&header, 8)?;
    if version != RUN_VERSION {
        return Err(MidgeError::Corruption(format!(
            "unsupported transaction spill version {version}"
        )));
    }
    let expected_crc = read_u32_at(&header, 44)?;
    if crc32c::crc32c(&header[..44]) != expected_crc {
        return Err(MidgeError::Corruption(
            "transaction spill header checksum mismatch".to_string(),
        ));
    }
    Ok(RunHeader {
        record_count: u64_to_usize(read_u64_at(&header, 12)?)?,
        ordinal_table_offset: read_u64_at(&header, 20)?,
        sparse_index_offset: read_u64_at(&header, 28)?,
        sparse_count: u64_to_usize(read_u64_at(&header, 36)?)?,
    })
}

pub(super) fn for_each_run_ordinal<F>(run: &SpillRun, mut visitor: F) -> MidgeResult<()>
where
    F: FnMut(OrdinalOp) -> MidgeResult<()>,
{
    let mut file = RunFile::open(&run.path)?;
    let header = read_header(&mut file)?;
    if header.record_count != run.record_count {
        return Err(MidgeError::Corruption(
            "transaction spill record count changed".to_string(),
        ));
    }
    let mut table_cursor = header.ordinal_table_offset;
    for _ in 0..header.record_count {
        file.seek_to(table_cursor)?;
        let (payload, next_cursor) = read_frame(&mut file)?;
        table_cursor = next_cursor;
        if payload.len() != 16 {
            return Err(MidgeError::Corruption(
                "transaction spill ordinal entry has invalid length".to_string(),
            ));
        }
        let ordinal = read_u64_at(&payload, 0)?;
        let record_offset = read_u64_at(&payload, 8)?;
        if record_offset < RUN_HEADER_LEN as u64 || record_offset >= header.ordinal_table_offset {
            return Err(MidgeError::Corruption(
                "transaction spill ordinal offset is out of bounds".to_string(),
            ));
        }
        file.seek_to(record_offset)?;
        let (ordinal_op, _) = read_op_frame(&mut file)?;
        if ordinal_op.ordinal != ordinal {
            return Err(MidgeError::Corruption(
                "transaction spill ordinal index does not match record".to_string(),
            ));
        }
        visitor(ordinal_op)?;
    }
    validate_sparse_index(&mut file, &header)?;
    validate_range_index(&run.range_path)
}

fn validate_sparse_index(file: &mut RunFile, header: &RunHeader) -> MidgeResult<()> {
    for_each_sparse_entry(file, header, |_, _| {})
}

/// Decode and validate every sparse-index frame exactly once.
///
/// All three sparse readers share this walk so that the checksum, ordering, and
/// bounds checks cannot drift apart between the cached and uncached paths.
fn for_each_sparse_entry<F>(
    file: &mut RunFile,
    header: &RunHeader,
    mut visitor: F,
) -> MidgeResult<()>
where
    F: FnMut(&[u8], u64),
{
    let mut cursor = header.sparse_index_offset;
    let mut previous_key: Option<Vec<u8>> = None;
    let mut previous_offset: Option<u64> = None;
    for index in 0..header.sparse_count {
        file.seek_to(cursor)?;
        let (payload, next_cursor) = read_frame(file)?;
        cursor = next_cursor;
        if payload.len() < 12 {
            return Err(MidgeError::Corruption(
                "transaction spill sparse index entry is truncated".to_string(),
            ));
        }
        let key_len = read_u32_at(&payload, 0)? as usize;
        let key_end = 4_usize.checked_add(key_len).ok_or_else(|| {
            MidgeError::Corruption("transaction spill sparse key length overflow".to_string())
        })?;
        if key_end.checked_add(8) != Some(payload.len()) {
            return Err(MidgeError::Corruption(
                "transaction spill sparse index entry has invalid length".to_string(),
            ));
        }
        let key = payload[4..key_end].to_vec();
        if previous_key
            .as_ref()
            .is_some_and(|previous| previous > &key)
        {
            return Err(MidgeError::Corruption(
                "transaction spill sparse index is not sorted".to_string(),
            ));
        }
        let record_offset = read_u64_at(&payload, key_end)?;
        if record_offset < RUN_HEADER_LEN as u64 || record_offset >= header.ordinal_table_offset {
            return Err(MidgeError::Corruption(
                "transaction spill sparse index offset is out of bounds".to_string(),
            ));
        }
        if previous_offset.is_some_and(|previous| previous >= record_offset) {
            return Err(MidgeError::Corruption(
                "transaction spill sparse offsets are not increasing".to_string(),
            ));
        }
        if index == 0 && record_offset != RUN_HEADER_LEN as u64 {
            return Err(MidgeError::Corruption(
                "transaction spill sparse index does not cover the first record".to_string(),
            ));
        }
        visitor(&key, record_offset);
        previous_key = Some(key);
        previous_offset = Some(record_offset);
    }
    if header.record_count != 0 && previous_offset.is_none() {
        return Err(MidgeError::Corruption(
            "transaction spill sparse index does not cover the first record".to_string(),
        ));
    }
    Ok(())
}

fn read_sparse_entries(file: &mut RunFile, header: &RunHeader) -> MidgeResult<Vec<(Bytes, u64)>> {
    let mut entries = Vec::with_capacity(header.sparse_count);
    for_each_sparse_entry(file, header, |key, offset| {
        entries.push((Bytes::copy_from_slice(key), offset));
    })?;
    Ok(entries)
}

pub(super) fn read_sparse_offsets(file: &mut RunFile, header: &RunHeader) -> MidgeResult<Vec<u64>> {
    let mut offsets = Vec::with_capacity(header.sparse_count);
    for_each_sparse_entry(file, header, |_, offset| offsets.push(offset))?;
    Ok(offsets)
}

pub(super) fn sparse_start_for_key(
    file: &mut RunFile,
    header: &RunHeader,
    target: Option<&[u8]>,
) -> MidgeResult<u64> {
    // Start at the last indexed record whose key is strictly below the
    // target. Runs are sorted by key, so every record for the target lies at
    // or after it, however many index strides those records span.
    let mut selected_offset = RUN_HEADER_LEN as u64;
    for_each_sparse_entry(file, header, |key, offset| {
        if target.is_some_and(|target| key < target) {
            selected_offset = offset;
        }
    })?;
    Ok(selected_offset)
}

pub(super) fn lookup_run_key(
    run: &SpillRun,
    key: &[u8],
    ordinal_ceiling: u64,
    latest: &mut Option<(u64, IntentLookup)>,
) -> MidgeResult<()> {
    run.with_reader(|reader| {
        let header = reader.header();
        let mut cursor = reader.sparse_start(Some(key))?;
        while cursor < header.ordinal_table_offset {
            reader.data.seek_to(cursor)?;
            let (ordinal_op, next_cursor) = read_op_frame(&mut reader.data)?;
            if next_cursor > header.ordinal_table_offset {
                return Err(MidgeError::Corruption(
                    "transaction spill data frame overlaps ordinal table".to_string(),
                ));
            }
            cursor = next_cursor;
            match ordinal_op.primary_key().cmp(key) {
                std::cmp::Ordering::Less => {}
                std::cmp::Ordering::Equal => match &ordinal_op.op {
                    TransactionOp::Put { .. } | TransactionOp::Delete { .. } => {
                        if ordinal_op.ordinal < ordinal_ceiling {
                            consider_lookup(&ordinal_op, key, latest);
                        }
                    }
                    TransactionOp::DeleteRange { .. } => {}
                },
                std::cmp::Ordering::Greater => break,
            }
        }

        lookup_range_index(
            &mut reader.range,
            &reader.range_header,
            key,
            ordinal_ceiling,
            latest,
        )
    })
}

fn write_op_frame(file: &mut File, ordinal_op: &OrdinalOp) -> MidgeResult<()> {
    let (tag, cf_id, ttl, key, second) = match &ordinal_op.op {
        TransactionOp::Put {
            cf_id,
            key,
            value,
            ttl_seconds,
            insert_only,
        } => (
            u8::from(*insert_only),
            *cf_id,
            *ttl_seconds,
            key.as_ref(),
            value.as_ref(),
        ),
        TransactionOp::Delete { cf_id, key } => (2, *cf_id, None, key.as_ref(), &[][..]),
        TransactionOp::DeleteRange {
            cf_id,
            start_key,
            end_key,
        } => (3, *cf_id, None, start_key.as_ref(), end_key.as_ref()),
    };
    let ordinal = ordinal_op.ordinal.to_le_bytes();
    let tag = [tag];
    let cf_id = cf_id.to_le_bytes();
    let ttl_present = [u8::from(ttl.is_some())];
    let ttl = ttl.unwrap_or(0).to_le_bytes();
    let key_len = field_len_bytes(key.len())?;
    let second_len = field_len_bytes(second.len())?;
    write_frame_parts(
        file,
        &[
            &ordinal,
            &tag,
            &cf_id,
            &ttl_present,
            &ttl,
            &key_len,
            key,
            &second_len,
            second,
        ],
    )
}

pub(super) fn read_op_frame(file: &mut RunFile) -> MidgeResult<(OrdinalOp, u64)> {
    let (payload_len, expected_crc) = read_frame_header(file)?;
    if payload_len < 30 {
        return Err(MidgeError::Corruption(
            "transaction spill operation is truncated".to_string(),
        ));
    }
    let mut crc = 0_u32;
    let mut fixed = [0_u8; 22];
    read_crc_exact(file, &mut fixed, &mut crc)?;
    let ordinal = read_u64_at(&fixed, 0)?;
    let tag = fixed[8];
    let cf_id = read_u32_at(&fixed, 9)?;
    let ttl_present = fixed[13];
    if ttl_present > 1 {
        return Err(MidgeError::Corruption(format!(
            "transaction spill operation has invalid TTL-presence byte {ttl_present}"
        )));
    }
    let ttl = read_u64_at(&fixed, 14)?;
    if ttl_present == 0 && ttl != 0 {
        return Err(MidgeError::Corruption(
            "transaction spill operation without TTL has nonzero TTL bytes".to_string(),
        ));
    }

    let mut key_len_bytes = [0_u8; 4];
    read_crc_exact(file, &mut key_len_bytes, &mut crc)?;
    let key_len = read_u32_at(&key_len_bytes, 0)? as usize;
    let mut consumed = 26_usize;
    if consumed.saturating_add(key_len).saturating_add(4) > payload_len {
        return Err(MidgeError::Corruption(
            "transaction spill key length exceeds its frame".to_string(),
        ));
    }
    let key = read_crc_vec(file, key_len, &mut crc)?;
    consumed = consumed.saturating_add(key_len);

    let mut second_len_bytes = [0_u8; 4];
    read_crc_exact(file, &mut second_len_bytes, &mut crc)?;
    consumed = consumed.saturating_add(4);
    let second_len = read_u32_at(&second_len_bytes, 0)? as usize;
    if consumed.saturating_add(second_len) != payload_len {
        return Err(MidgeError::Corruption(
            "transaction spill value length does not match its frame".to_string(),
        ));
    }
    let second = read_crc_vec(file, second_len, &mut crc)?;
    if crc != expected_crc {
        return Err(MidgeError::Corruption(
            "transaction spill frame checksum mismatch".to_string(),
        ));
    }
    let op = match tag {
        0 | 1 => TransactionOp::Put {
            cf_id,
            key: Bytes::from(key),
            value: Bytes::from(second),
            ttl_seconds: (ttl_present == 1).then_some(ttl),
            insert_only: tag == 1,
        },
        2 if second.is_empty() => TransactionOp::Delete {
            cf_id,
            key: Bytes::from(key),
        },
        3 => TransactionOp::DeleteRange {
            cf_id,
            start_key: Bytes::from(key),
            end_key: Bytes::from(second),
        },
        _ => {
            return Err(MidgeError::Corruption(format!(
                "transaction spill operation has invalid tag {tag}"
            )))
        }
    };
    if tag >= 2 && ttl_present != 0 {
        return Err(MidgeError::Corruption(
            "transaction spill delete operation carries a TTL".to_string(),
        ));
    }
    Ok((OrdinalOp { ordinal, op }, file.position()))
}

/// Read and checksum one key-sorted operation while retaining only its key.
///
/// A spilled value may be as large as a WAL frame. Scan cursors must not
/// reconstruct that value merely to merge intent keys, so the second field is
/// checksummed through a fixed scratch buffer and discarded.
pub(super) fn read_op_primary_key_frame(file: &mut RunFile) -> MidgeResult<(Bytes, u64)> {
    let (payload_len, expected_crc) = read_frame_header(file)?;
    if payload_len < 30 {
        return Err(MidgeError::Corruption(
            "transaction spill operation is truncated".to_string(),
        ));
    }

    let mut crc = 0_u32;
    let mut fixed = [0_u8; 22];
    read_crc_exact(file, &mut fixed, &mut crc)?;
    let tag = fixed[8];
    if tag > 3 {
        return Err(MidgeError::Corruption(format!(
            "transaction spill operation has invalid tag {tag}"
        )));
    }
    let ttl_present = fixed[13];
    let ttl = read_u64_at(&fixed, 14)?;
    if ttl_present > 1 || (ttl_present == 0 && ttl != 0) || (tag >= 2 && ttl_present != 0) {
        return Err(MidgeError::Corruption(
            "transaction spill operation has invalid TTL encoding".to_string(),
        ));
    }

    let mut key_len_bytes = [0_u8; 4];
    read_crc_exact(file, &mut key_len_bytes, &mut crc)?;
    let key_len = read_u32_at(&key_len_bytes, 0)? as usize;
    let mut consumed = 26_usize;
    if consumed.saturating_add(key_len).saturating_add(4) > payload_len {
        return Err(MidgeError::Corruption(
            "transaction spill key length exceeds its frame".to_string(),
        ));
    }
    let key = read_crc_vec(file, key_len, &mut crc)?;
    consumed = consumed.saturating_add(key_len);

    let mut second_len_bytes = [0_u8; 4];
    read_crc_exact(file, &mut second_len_bytes, &mut crc)?;
    consumed = consumed.saturating_add(4);
    let second_len = read_u32_at(&second_len_bytes, 0)? as usize;
    if consumed.saturating_add(second_len) != payload_len {
        return Err(MidgeError::Corruption(
            "transaction spill value length does not match its frame".to_string(),
        ));
    }
    if tag == 2 && second_len != 0 {
        return Err(MidgeError::Corruption(
            "transaction spill delete has a value".to_string(),
        ));
    }

    let mut remaining = second_len;
    let mut scratch = [0_u8; 8192];
    while remaining != 0 {
        let chunk_len = remaining.min(scratch.len());
        read_crc_exact(file, &mut scratch[..chunk_len], &mut crc)?;
        remaining -= chunk_len;
    }
    if crc != expected_crc {
        return Err(MidgeError::Corruption(
            "transaction spill frame checksum mismatch".to_string(),
        ));
    }
    Ok((Bytes::from(key), file.position()))
}

fn write_frame(file: &mut File, payload: &[u8]) -> MidgeResult<()> {
    let length = u32::try_from(payload.len()).map_err(|_| {
        MidgeError::ResourceLimit("transaction spill frame exceeds u32 length".to_string())
    })?;
    file.write_all(&length.to_le_bytes())?;
    file.write_all(&crc32c::crc32c(payload).to_le_bytes())?;
    file.write_all(payload)?;
    Ok(())
}

pub(super) fn write_frame_parts(file: &mut File, parts: &[&[u8]]) -> MidgeResult<()> {
    let payload_len = parts.iter().try_fold(0_usize, |total, part| {
        total.checked_add(part.len()).ok_or_else(|| {
            MidgeError::ResourceLimit("transaction spill frame length overflow".to_string())
        })
    })?;
    if payload_len > MAX_FRAME_BYTES {
        return Err(MidgeError::ResourceLimit(format!(
            "transaction spill frame length {payload_len} exceeds WAL frame limit"
        )));
    }
    let payload_len_u32 = u32::try_from(payload_len).map_err(|_| {
        MidgeError::ResourceLimit("transaction spill frame exceeds u32 length".to_string())
    })?;
    let crc = parts
        .iter()
        .fold(0_u32, |crc, part| crc32c::crc32c_append(crc, part));
    file.write_all(&payload_len_u32.to_le_bytes())?;
    file.write_all(&crc.to_le_bytes())?;
    for part in parts {
        file.write_all(part)?;
    }
    Ok(())
}

fn write_sparse_frame(file: &mut File, key: &[u8], offset: u64) -> MidgeResult<()> {
    let key_len = field_len_bytes(key.len())?;
    let offset = offset.to_le_bytes();
    write_frame_parts(file, &[&key_len, key, &offset])
}

pub(super) fn field_len_bytes(length: usize) -> MidgeResult<[u8; 4]> {
    u32::try_from(length)
        .map(u32::to_le_bytes)
        .map_err(|_| MidgeError::ResourceLimit("transaction spill field is too large".to_string()))
}

pub(super) fn read_frame_header(file: &mut RunFile) -> MidgeResult<(usize, u32)> {
    let mut frame_header = [0_u8; 8];
    file.read_exact(&mut frame_header)?;
    let length = read_u32_at(&frame_header, 0)? as usize;
    if length > MAX_FRAME_BYTES {
        return Err(MidgeError::Corruption(format!(
            "transaction spill frame length {length} exceeds limit"
        )));
    }
    Ok((length, read_u32_at(&frame_header, 4)?))
}

pub(super) fn read_crc_exact(file: &mut RunFile, dst: &mut [u8], crc: &mut u32) -> MidgeResult<()> {
    file.read_exact(dst)?;
    *crc = crc32c::crc32c_append(*crc, dst);
    Ok(())
}

pub(super) fn read_crc_vec(
    file: &mut RunFile,
    length: usize,
    crc: &mut u32,
) -> MidgeResult<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(length).map_err(|_| {
        MidgeError::ResourceLimit(format!(
            "transaction spill cannot allocate {length} bytes for one operation field"
        ))
    })?;
    bytes.resize(length, 0);
    read_crc_exact(file, &mut bytes, crc)?;
    Ok(bytes)
}

fn read_frame(file: &mut RunFile) -> MidgeResult<(Vec<u8>, u64)> {
    let (length, expected_crc) = read_frame_header(file)?;
    let mut payload = Vec::new();
    payload.try_reserve_exact(length).map_err(|_| {
        MidgeError::ResourceLimit(format!(
            "transaction spill cannot allocate {length} bytes for an index frame"
        ))
    })?;
    payload.resize(length, 0);
    file.read_exact(&mut payload)?;
    if crc32c::crc32c(&payload) != expected_crc {
        return Err(MidgeError::Corruption(
            "transaction spill frame checksum mismatch".to_string(),
        ));
    }
    Ok((payload, file.position()))
}

pub(super) fn read_u32_at(bytes: &[u8], offset: usize) -> MidgeResult<u32> {
    let end = offset.checked_add(4).ok_or_else(|| {
        MidgeError::Corruption("transaction spill u32 offset overflow".to_string())
    })?;
    let slice = bytes
        .get(offset..end)
        .ok_or_else(|| MidgeError::Corruption("transaction spill u32 is truncated".to_string()))?;
    Ok(u32::from_le_bytes(
        slice.try_into().expect("validated u32 slice"),
    ))
}

pub(super) fn read_u64_at(bytes: &[u8], offset: usize) -> MidgeResult<u64> {
    let end = offset.checked_add(8).ok_or_else(|| {
        MidgeError::Corruption("transaction spill u64 offset overflow".to_string())
    })?;
    let slice = bytes
        .get(offset..end)
        .ok_or_else(|| MidgeError::Corruption("transaction spill u64 is truncated".to_string()))?;
    Ok(u64::from_le_bytes(
        slice.try_into().expect("validated u64 slice"),
    ))
}

pub(super) fn usize_to_u64(value: usize) -> MidgeResult<u64> {
    u64::try_from(value)
        .map_err(|_| MidgeError::ResourceLimit("transaction spill count exceeds u64".to_string()))
}

pub(super) fn u64_to_usize(value: u64) -> MidgeResult<usize> {
    usize::try_from(value).map_err(|_| {
        MidgeError::Corruption("transaction spill count exceeds platform size".to_string())
    })
}

pub(super) fn remove_run(path: &Path) {
    match fs::remove_file(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            tracing::warn!(path = %path.display(), %error, "failed to remove transaction spill run");
        }
    }
}
