use super::mutation::{Mutation, MutationOp};
use crate::api::ColumnFamilyId;
use crate::error::MidgeError;
use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::time::Instant;

/// Synchronous transaction object that stages mutations in-memory.
///
/// This is a lightweight, crate-root Transaction intended to be used where
/// a full internal engine transaction API is not available. It implements
/// the common semantics: staged mutation list, last-write-wins local reads,
/// commit/rollback lifecycle, with optional conflict detection.
pub struct Transaction {
    txn_id: u64,
    begin_sequence: u64,
    commit_sequence: Option<u64>,
    staged: Vec<Mutation>,
    completed: bool,

    // ACID enhancements
    // Use composite (cf_id, key) to avoid cross-CF conflicts
    read_set: HashSet<(u32, Bytes)>,
    write_set: HashSet<(u32, Bytes)>,
    read_versions: HashMap<(u32, Bytes), u64>,
    #[allow(dead_code)]
    created_at: Instant,
    deadline: Option<Instant>,

    // Spill-to-disk tracking
    memory_threshold: usize,
    current_memory: usize,
    spill_files: Vec<std::path::PathBuf>,
}

impl Transaction {
    pub fn new(txn_id: u64, begin_sequence: u64) -> Self {
        Self::with_options(txn_id, begin_sequence, None, 100 * 1024 * 1024)
    }

    pub fn with_options(
        txn_id: u64,
        begin_sequence: u64,
        timeout: Option<std::time::Duration>,
        memory_threshold: usize,
    ) -> Self {
        let created_at = Instant::now();
        let deadline = timeout.map(|d| created_at + d);

        Self {
            txn_id,
            begin_sequence,
            commit_sequence: None,
            staged: Vec::new(),
            completed: false,
            read_set: HashSet::new(),
            write_set: HashSet::new(),
            read_versions: HashMap::new(),
            created_at,
            deadline,
            memory_threshold,
            current_memory: 0,
            spill_files: Vec::new(),
        }
    }

    pub fn txn_id(&self) -> u64 {
        self.txn_id
    }

    pub fn begin_sequence(&self) -> u64 {
        self.begin_sequence
    }

    #[inline]
    pub fn commit_sequence(&self) -> Option<u64> {
        self.commit_sequence
    }

    /// Check if transaction has exceeded deadline
    pub fn is_expired(&self) -> bool {
        if let Some(deadline) = self.deadline {
            Instant::now() > deadline
        } else {
            false
        }
    }

    /// Track a read operation for conflict detection
    pub fn track_read(&mut self, cf_id: u32, key: Bytes, version: u64) {
        self.read_set.insert((cf_id, key.clone()));
        self.read_versions.insert((cf_id, key), version);
    }

    /// Get the write set (keys modified by this transaction)
    pub fn write_set(&self) -> &HashSet<(u32, Bytes)> {
        &self.write_set
    }

    /// Get the read set (keys read by this transaction)
    pub fn read_set(&self) -> &HashSet<(u32, Bytes)> {
        &self.read_set
    }

    /// Get the read versions map (keys -> sequence numbers)
    pub fn read_versions(&self) -> &HashMap<(u32, Bytes), u64> {
        &self.read_versions
    }

    /// Get read version for a key
    pub fn read_version(&self, cf_id: u32, key: &[u8]) -> Option<u64> {
        self.read_versions
            .get(&(cf_id, Bytes::copy_from_slice(key)))
            .copied()
    }

    /// Check if there's a write-write conflict with given write set
    pub fn has_write_conflict(&self, other_writes: &HashSet<(u32, Bytes)>) -> bool {
        !self.write_set.is_disjoint(other_writes)
    }

    fn update_memory_usage_and_spill(&mut self) -> Result<(), MidgeError> {
        // Calculate size of last staged mutation
        if let Some(mutation) = self.staged.last() {
            let size = mutation.key.len()
                + mutation.value.as_ref().map(|v| v.len()).unwrap_or(0)
                + mutation.range_end.as_ref().map(|v| v.len()).unwrap_or(0);
            self.current_memory += size;

            // Check if we need to spill to disk
            if self.current_memory >= self.memory_threshold {
                self.spill_to_disk()?;
            }
        }

        Ok(())
    }

    /// Spill currently staged mutations to a temporary file and clear memory
    fn spill_to_disk(&mut self) -> Result<(), MidgeError> {
        if self.staged.is_empty() {
            return Ok(());
        }

        // Create temporary spill file with unique name using process ID and timestamp
        let pid = std::process::id();
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("System time before UNIX_EPOCH")
            .as_nanos();
        let spill_path = std::env::temp_dir().join(format!(
            "midge_txn_{}_{}_{:x}_{}.spill",
            self.txn_id,
            self.spill_files.len(),
            pid,
            timestamp
        ));

        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&spill_path)
            .map_err(|e| MidgeError::internal(format!("Failed to create spill file: {}", e)))?;

        let mut writer = BufWriter::new(file);

        // Write staged mutations to file
        for mutation in &self.staged {
            // Serialize mutation: format is:
            // [op_type: u8][key_len: u32][key][value_len: u32][value_or_range_end]
            let op_byte = match mutation.op {
                MutationOp::Put => 0u8,
                MutationOp::Insert => 1u8,
                MutationOp::Delete => 2u8,
                MutationOp::DeleteRange => 3u8,
                MutationOp::CompareAndSwap => 4u8,
                MutationOp::Merge => 5u8,
            };

            writer
                .write_all(&[op_byte])
                .map_err(|e| MidgeError::internal(format!("Failed to write op: {}", e)))?;

            // Write column family id (u32 little-endian)
            let cf_bytes = mutation.cf_id.as_u32().to_le_bytes();
            writer
                .write_all(&cf_bytes)
                .map_err(|e| MidgeError::internal(format!("Failed to write cf id: {}", e)))?;

            // Write key
            let key_len = mutation.key.len() as u32;
            writer
                .write_all(&key_len.to_le_bytes())
                .map_err(|e| MidgeError::internal(format!("Failed to write key len: {}", e)))?;
            writer
                .write_all(&mutation.key)
                .map_err(|e| MidgeError::internal(format!("Failed to write key: {}", e)))?;

            // Write value or range_end
            match &mutation.op {
                MutationOp::Put | MutationOp::Insert | MutationOp::Merge => {
                    if let Some(ref value) = mutation.value {
                        let value_len = value.len() as u32;
                        writer.write_all(&value_len.to_le_bytes()).map_err(|e| {
                            MidgeError::internal(format!("Failed to write value len: {}", e))
                        })?;
                        writer.write_all(value).map_err(|e| {
                            MidgeError::internal(format!("Failed to write value: {}", e))
                        })?;
                    } else {
                        writer.write_all(&0u32.to_le_bytes()).map_err(|e| {
                            MidgeError::internal(format!("Failed to write value len: {}", e))
                        })?;
                    }
                }
                MutationOp::Delete => {
                    writer.write_all(&0u32.to_le_bytes()).map_err(|e| {
                        MidgeError::internal(format!("Failed to write value len: {}", e))
                    })?;
                }
                MutationOp::DeleteRange => {
                    if let Some(ref end) = mutation.range_end {
                        let end_len = end.len() as u32;
                        writer.write_all(&end_len.to_le_bytes()).map_err(|e| {
                            MidgeError::internal(format!("Failed to write range end len: {}", e))
                        })?;
                        writer.write_all(end).map_err(|e| {
                            MidgeError::internal(format!("Failed to write range end: {}", e))
                        })?;
                    } else {
                        writer.write_all(&0u32.to_le_bytes()).map_err(|e| {
                            MidgeError::internal(format!("Failed to write range end len: {}", e))
                        })?;
                    }
                }
                MutationOp::CompareAndSwap => {
                    // Write value (new_value)
                    if let Some(ref value) = mutation.value {
                        let value_len = value.len() as u32;
                        writer.write_all(&value_len.to_le_bytes()).map_err(|e| {
                            MidgeError::internal(format!("Failed to write value len: {}", e))
                        })?;
                        writer.write_all(value).map_err(|e| {
                            MidgeError::internal(format!("Failed to write value: {}", e))
                        })?;
                    } else {
                        writer.write_all(&0u32.to_le_bytes()).map_err(|e| {
                            MidgeError::internal(format!("Failed to write value len: {}", e))
                        })?;
                    }
                    // Write expected value (stored in range_end)
                    if let Some(ref expected) = mutation.range_end {
                        let expected_len = expected.len() as u32;
                        writer.write_all(&expected_len.to_le_bytes()).map_err(|e| {
                            MidgeError::internal(format!("Failed to write expected len: {}", e))
                        })?;
                        writer.write_all(expected).map_err(|e| {
                            MidgeError::internal(format!("Failed to write expected: {}", e))
                        })?;
                    } else {
                        writer.write_all(&0u32.to_le_bytes()).map_err(|e| {
                            MidgeError::internal(format!("Failed to write expected len: {}", e))
                        })?;
                    }
                }
            }
        }

        writer
            .flush()
            .map_err(|e| MidgeError::internal(format!("Failed to flush spill file: {}", e)))?;

        // Clear staged mutations and reset memory counter
        self.staged.clear();
        self.current_memory = 0;
        self.spill_files.push(spill_path);

        Ok(())
    }

    /// Read mutations from spill files
    fn read_spill_files(&self) -> Result<Vec<Mutation>, MidgeError> {
        let mut all_mutations = Vec::new();

        for spill_path in &self.spill_files {
            let file = File::open(spill_path)
                .map_err(|e| MidgeError::internal(format!("Failed to open spill file: {}", e)))?;

            let mut reader = BufReader::new(file);

            loop {
                // Read operation type
                let mut op_byte = [0u8; 1];
                match reader.read_exact(&mut op_byte) {
                    Ok(_) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                    Err(e) => {
                        return Err(MidgeError::internal(format!("Failed to read op: {}", e)))
                    }
                }

                // Read column family id
                let mut cf_id_bytes = [0u8; 4];
                reader
                    .read_exact(&mut cf_id_bytes)
                    .map_err(|e| MidgeError::internal(format!("Failed to read cf id: {}", e)))?;
                let cf_id = u32::from_le_bytes(cf_id_bytes);

                // Read key
                let mut key_len_bytes = [0u8; 4];
                reader
                    .read_exact(&mut key_len_bytes)
                    .map_err(|e| MidgeError::internal(format!("Failed to read key len: {}", e)))?;
                let key_len = u32::from_le_bytes(key_len_bytes) as usize;

                let mut key_bytes = vec![0u8; key_len];
                reader
                    .read_exact(&mut key_bytes)
                    .map_err(|e| MidgeError::internal(format!("Failed to read key: {}", e)))?;
                let key = Bytes::from(key_bytes);

                // Read value/range_end
                let mut value_len_bytes = [0u8; 4];
                reader.read_exact(&mut value_len_bytes).map_err(|e| {
                    MidgeError::internal(format!("Failed to read value len: {}", e))
                })?;
                let value_len = u32::from_le_bytes(value_len_bytes) as usize;

                let value_or_end = if value_len > 0 {
                    let mut value_bytes = vec![0u8; value_len];
                    reader.read_exact(&mut value_bytes).map_err(|e| {
                        MidgeError::internal(format!("Failed to read value: {}", e))
                    })?;
                    Some(Bytes::from(value_bytes))
                } else {
                    None
                };

                // Reconstruct mutation using stored cf_id
                let cf_id_obj = crate::api::ColumnFamilyId::from(cf_id);
                let mutation = match op_byte[0] {
                    0 => Mutation::put_cf(cf_id_obj, key, value_or_end.unwrap_or_default(), None),
                    1 => {
                        Mutation::insert_cf(cf_id_obj, key, value_or_end.unwrap_or_default(), None)
                    }
                    2 => Mutation::delete_cf(cf_id_obj, key),
                    3 => Mutation::delete_range_cf(cf_id_obj, key, value_or_end),
                    4 => {
                        // CompareAndSwap: read expected value (second field)
                        let mut expected_len_bytes = [0u8; 4];
                        reader.read_exact(&mut expected_len_bytes).map_err(|e| {
                            MidgeError::internal(format!("Failed to read expected len: {}", e))
                        })?;
                        let expected_len = u32::from_le_bytes(expected_len_bytes) as usize;

                        let expected = if expected_len > 0 {
                            let mut expected_bytes = vec![0u8; expected_len];
                            reader.read_exact(&mut expected_bytes).map_err(|e| {
                                MidgeError::internal(format!("Failed to read expected: {}", e))
                            })?;
                            Some(Bytes::from(expected_bytes))
                        } else {
                            None
                        };

                        Mutation::compare_and_swap_cf(
                            cf_id_obj,
                            key,
                            expected,
                            value_or_end.unwrap_or_default(),
                        )
                    }
                    5 => Mutation::merge_cf(cf_id_obj, key, value_or_end.unwrap_or_default()),
                    _ => {
                        return Err(MidgeError::internal(format!(
                            "Unknown mutation op: {}",
                            op_byte[0]
                        )))
                    }
                };

                all_mutations.push(mutation);
            }
        }

        Ok(all_mutations)
    }

    /// Cleanup all spill files
    fn cleanup_spill_files(&mut self) {
        for spill_path in &self.spill_files {
            let _ = std::fs::remove_file(spill_path);
        }
        self.spill_files.clear();
    }

    fn track_write(&mut self, cf_id: u32, key: Bytes) {
        self.write_set.insert((cf_id, key));
    }

    #[inline]
    pub fn put(
        &mut self,
        key: Bytes,
        value: Bytes,
        ttl: Option<std::time::Duration>,
    ) -> Result<(), MidgeError> {
        self.put_cf(crate::api::DEFAULT_CF_ID, key, value, ttl)
    }

    #[inline]
    pub fn put_cf(
        &mut self,
        cf_id: crate::api::ColumnFamilyId,
        key: Bytes,
        value: Bytes,
        ttl: Option<std::time::Duration>,
    ) -> Result<(), MidgeError> {
        self.track_write(cf_id.as_u32(), key.clone());
        let mutation = Mutation::put_cf(cf_id, key, value, ttl);
        self.staged.push(mutation);
        self.update_memory_usage_and_spill()?;
        Ok(())
    }

    #[inline]
    pub fn insert(
        &mut self,
        key: Bytes,
        value: Bytes,
        ttl: Option<std::time::Duration>,
    ) -> Result<(), MidgeError> {
        self.insert_cf(crate::api::DEFAULT_CF_ID, key, value, ttl)
    }

    #[inline]
    pub fn insert_cf(
        &mut self,
        cf_id: crate::api::ColumnFamilyId,
        key: Bytes,
        value: Bytes,
        ttl: Option<std::time::Duration>,
    ) -> Result<(), MidgeError> {
        self.track_write(cf_id.as_u32(), key.clone());
        let mutation = Mutation::insert_cf(cf_id, key, value, ttl);
        self.staged.push(mutation);
        self.update_memory_usage_and_spill()?;
        Ok(())
    }

    #[inline]
    pub fn delete(&mut self, key: Bytes) -> Result<(), MidgeError> {
        self.delete_cf(crate::api::DEFAULT_CF_ID, key)
    }

    #[inline]
    pub fn delete_cf(
        &mut self,
        cf_id: crate::api::ColumnFamilyId,
        key: Bytes,
    ) -> Result<(), MidgeError> {
        self.track_write(cf_id.as_u32(), key.clone());
        let mutation = Mutation::delete_cf(cf_id, key);
        self.staged.push(mutation);
        self.update_memory_usage_and_spill()?;
        Ok(())
    }

    #[inline]
    pub fn delete_range(&mut self, start: Bytes, end: Bytes) -> Result<(), MidgeError> {
        self.delete_range_cf(crate::api::DEFAULT_CF_ID, start, end)
    }

    #[inline]
    pub fn delete_range_cf(
        &mut self,
        cf_id: crate::api::ColumnFamilyId,
        start: Bytes,
        end: Bytes,
    ) -> Result<(), MidgeError> {
        self.track_write(cf_id.as_u32(), start.clone());
        let mutation = Mutation::delete_range_cf(cf_id, start, end);
        self.staged.push(mutation);
        self.update_memory_usage_and_spill()?;
        Ok(())
    }

    #[inline]
    pub fn compare_and_swap(
        &mut self,
        key: Bytes,
        expected: Option<Bytes>,
        new_value: Bytes,
    ) -> Result<(), MidgeError> {
        self.compare_and_swap_cf(crate::api::DEFAULT_CF_ID, key, expected, new_value)
    }

    #[inline]
    pub fn compare_and_swap_cf(
        &mut self,
        cf_id: crate::api::ColumnFamilyId,
        key: Bytes,
        expected: Option<Bytes>,
        new_value: Bytes,
    ) -> Result<(), MidgeError> {
        self.track_write(cf_id.as_u32(), key.clone());
        let mutation = Mutation::compare_and_swap_cf(cf_id, key, expected, new_value);
        self.staged.push(mutation);
        self.update_memory_usage_and_spill()?;
        Ok(())
    }

    #[inline]
    pub fn merge(&mut self, key: Bytes, value: Bytes) -> Result<(), MidgeError> {
        self.merge_cf(crate::api::DEFAULT_CF_ID, key, value)
    }

    #[inline]
    pub fn merge_cf(
        &mut self,
        cf_id: crate::api::ColumnFamilyId,
        key: Bytes,
        value: Bytes,
    ) -> Result<(), MidgeError> {
        self.track_write(cf_id.as_u32(), key.clone());
        let mutation = Mutation::merge_cf(cf_id, key, value);
        self.staged.push(mutation);
        self.update_memory_usage_and_spill()?;
        Ok(())
    }

    /// Local get resolves staged mutations only: returns Some(Some(value)) if
    /// a staged Put/Insert is present, Some(None) if a staged Delete applies,
    /// or None if the key is not present in the staged set and caller should
    /// consult the underlying DB.
    ///
    /// Note: This only checks in-memory staged mutations for performance.
    /// Spill files are only read during commit.
    #[inline]
    pub fn get_local(&self, cf_id: u32, key: &[u8]) -> Option<Option<Bytes>> {
        for m in self.staged.iter().rev() {
            // Only consider staged mutations for the requested column family
            if m.cf_id.as_u32() != cf_id {
                continue;
            }
            match &m.op {
                MutationOp::DeleteRange => {
                    if let Some(end) = &m.range_end {
                        if m.key.as_ref() <= key && key < end.as_ref() {
                            return Some(None);
                        }
                    }
                }
                _ => {
                    if m.key.as_ref() == key {
                        return Some(m.value.clone());
                    }
                }
            }
        }
        None
    }

    pub fn exists_local(&self, cf_id: u32, key: &[u8]) -> bool {
        matches!(self.get_local(cf_id, key), Some(Some(_)))
    }

    /// Consume the transaction and return staged mutations for the caller
    /// to apply to the underlying engine. Marks the transaction completed.
    /// Merges mutations from spill files with in-memory staged mutations.
    pub fn commit(mut self) -> Result<Vec<Mutation>, MidgeError> {
        if self.completed {
            return Err(MidgeError::internal("transaction already completed"));
        }
        self.completed = true;

        // Read mutations from spill files if any exist
        let mut all_mutations = if !self.spill_files.is_empty() {
            self.read_spill_files()?
        } else {
            Vec::new()
        };

        // Append in-memory staged mutations
        all_mutations.extend(std::mem::take(&mut self.staged));

        // Cleanup spill files after successful read
        self.cleanup_spill_files();

        Ok(all_mutations)
    }

    /// Rollback clears staged mutations and marks completed.
    pub fn rollback(&mut self) {
        if !self.completed {
            self.staged.clear();
            self.cleanup_spill_files();
            self.completed = true;
        }
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if !self.completed {
            // Auto-rollback on drop for safety
            self.staged.clear();
            self.cleanup_spill_files();
            self.completed = true;
        } else {
            // Still cleanup spill files even if completed (defensive programming)
            self.cleanup_spill_files();
        }
    }
}

// Re-export common types so downstream code can `use cntryl_midge::Transaction;`
pub use Transaction as Tx;

/// Transaction wrapper that provides read access via engine reference.
/// Used internally when transactions are created through the KvStore trait.
pub(crate) struct EngineTransaction {
    txn: Transaction,
    engine: std::sync::Arc<crate::core::engine::MidgeEngine>,
}

impl EngineTransaction {
    pub(crate) fn new(
        txn: Transaction,
        engine: std::sync::Arc<crate::core::engine::MidgeEngine>,
    ) -> Self {
        Self { txn, engine }
    }

    pub(crate) fn into_inner(self) -> Transaction {
        self.txn
    }
}

// Implement the public KvTransaction trait for EngineTransaction
impl super::kv_store::KvTransaction for EngineTransaction {
    fn put(&mut self, key: &[u8], value: &[u8]) -> crate::MidgeResult<()> {
        self.txn.put(
            Bytes::copy_from_slice(key),
            Bytes::copy_from_slice(value),
            None,
        )
    }

    fn get(&mut self, key: &[u8]) -> crate::MidgeResult<Option<Bytes>> {
        let cf = self.engine.default_column_family();
        self.engine.transaction_get(&mut self.txn, &cf, key)
    }

    fn delete(&mut self, key: &[u8]) -> crate::MidgeResult<()> {
        self.txn.delete(Bytes::copy_from_slice(key))
    }

    fn scan(&mut self, start: &[u8], end: &[u8]) -> crate::MidgeResult<Vec<(Bytes, Bytes)>> {
        // Use engine's scan with transaction's snapshot
        let q = crate::api::query::Query::new()
            .start_key(Bytes::copy_from_slice(start))
            .end_key(Bytes::copy_from_slice(end));

        // TODO: Implement transaction-aware scan in engine
        // For now, run a column-family scoped scan on the engine's default CF
        let cf = self.engine.default_column_family();
        self.engine.scan(&cf, q)
    }

    fn delete_range(&mut self, start: &[u8], end: &[u8]) -> crate::MidgeResult<()> {
        self.txn
            .delete_range(Bytes::copy_from_slice(start), Bytes::copy_from_slice(end))
    }

    fn compare_and_swap(
        &mut self,
        key: &[u8],
        expected: Option<&[u8]>,
        new_value: &[u8],
    ) -> crate::MidgeResult<bool> {
        self.txn.compare_and_swap(
            Bytes::copy_from_slice(key),
            expected.map(Bytes::copy_from_slice),
            Bytes::copy_from_slice(new_value),
        )?;
        // For now, always return true since validation happens at commit time
        Ok(true)
    }

    fn merge(&mut self, key: &[u8], value: &[u8]) -> crate::MidgeResult<()> {
        self.txn
            .merge(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value))
    }
}

// Implement the public KvTransaction trait for the crate Transaction so that the
// crate-local Transaction can be used wherever the generic KvTransaction trait
// is expected by external integrations (though reads won't work without engine reference).
impl super::kv_store::KvTransaction for Transaction {
    fn put(&mut self, key: &[u8], value: &[u8]) -> crate::MidgeResult<()> {
        Transaction::put(
            self,
            Bytes::copy_from_slice(key),
            Bytes::copy_from_slice(value),
            None,
        )
    }

    fn get(&mut self, _key: &[u8]) -> crate::MidgeResult<Option<Bytes>> {
        Err(crate::MidgeError::internal(
            "Transaction reads require engine context. Use KvStore::begin_transaction() instead.",
        ))
    }

    fn delete(&mut self, key: &[u8]) -> crate::MidgeResult<()> {
        Transaction::delete(self, Bytes::copy_from_slice(key))
    }

    fn scan(&mut self, _start: &[u8], _end: &[u8]) -> crate::MidgeResult<Vec<(Bytes, Bytes)>> {
        Err(crate::MidgeError::internal(
            "Transaction scans require engine context. Use KvStore::begin_transaction() instead.",
        ))
    }

    fn delete_range(&mut self, start: &[u8], end: &[u8]) -> crate::MidgeResult<()> {
        Transaction::delete_range(
            self,
            Bytes::copy_from_slice(start),
            Bytes::copy_from_slice(end),
        )
    }

    fn compare_and_swap(
        &mut self,
        key: &[u8],
        expected: Option<&[u8]>,
        new_value: &[u8],
    ) -> crate::MidgeResult<bool> {
        Transaction::compare_and_swap(
            self,
            Bytes::copy_from_slice(key),
            expected.map(Bytes::copy_from_slice),
            Bytes::copy_from_slice(new_value),
        )?;
        // For now, always return true since validation happens at commit time
        Ok(true)
    }

    fn merge(&mut self, key: &[u8], value: &[u8]) -> crate::MidgeResult<()> {
        Transaction::merge(
            self,
            Bytes::copy_from_slice(key),
            Bytes::copy_from_slice(value),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // Transaction Write-Set Tracking Tests
    // ========================================================================

    #[test]
    fn should_track_write_set_given_put_operation() {
        // Arrange
        let mut txn = Transaction::new(1, 100);

        // Act
        txn.put(Bytes::from("key1"), Bytes::from("value1"), None)
            .unwrap();
        txn.put(Bytes::from("key2"), Bytes::from("value2"), None)
            .unwrap();

        // Assert
        assert!(txn
            .write_set()
            .contains(&(crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("key1"))));
        assert!(txn
            .write_set()
            .contains(&(crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("key2"))));
        assert_eq!(txn.write_set().len(), 2);
    }

    #[test]
    fn should_track_write_set_given_delete_operation() {
        // Arrange
        let mut txn = Transaction::new(1, 100);

        // Act
        txn.delete(Bytes::from("deleted_key")).unwrap();

        // Assert
        assert!(txn.write_set().contains(&(
            crate::api::DEFAULT_CF_ID.as_u32(),
            Bytes::from("deleted_key")
        )));
    }

    #[test]
    fn should_track_write_set_given_delete_range_operation() {
        // Arrange
        let mut txn = Transaction::new(1, 100);

        // Act
        txn.delete_range(Bytes::from("start"), Bytes::from("end"))
            .unwrap();

        // Assert
        assert!(txn
            .write_set()
            .contains(&(crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("start"))));
    }

    #[test]
    fn should_not_duplicate_keys_in_write_set_given_multiple_puts() {
        // Arrange
        let mut txn = Transaction::new(1, 100);

        // Act
        txn.put(Bytes::from("key"), Bytes::from("v1"), None)
            .unwrap();
        txn.put(Bytes::from("key"), Bytes::from("v2"), None)
            .unwrap();
        txn.put(Bytes::from("key"), Bytes::from("v3"), None)
            .unwrap();

        // Assert
        assert_eq!(txn.write_set().len(), 1);
    }

    // ========================================================================
    // Transaction Read-Set Tracking Tests
    // ========================================================================

    #[test]
    fn should_track_read_set_given_track_read_called() {
        // Arrange
        let mut txn = Transaction::new(1, 100);

        // Act
        txn.track_read(crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("key1"), 50);
        txn.track_read(crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("key2"), 75);

        // Assert
        assert!(txn
            .read_set()
            .contains(&(crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("key1"))));
        assert!(txn
            .read_set()
            .contains(&(crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("key2"))));
    }

    #[test]
    fn should_store_read_version_given_track_read_called() {
        // Arrange
        let mut txn = Transaction::new(1, 100);

        // Act
        txn.track_read(crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("key"), 42);

        // Assert
        assert_eq!(
            txn.read_version(crate::api::DEFAULT_CF_ID.as_u32(), b"key"),
            Some(42)
        );
    }

    #[test]
    fn should_update_read_version_given_multiple_reads() {
        // Arrange
        let mut txn = Transaction::new(1, 100);

        // Act
        txn.track_read(crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("key"), 10);
        txn.track_read(crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("key"), 20);

        // Assert
        assert_eq!(
            txn.read_version(crate::api::DEFAULT_CF_ID.as_u32(), b"key"),
            Some(20)
        );
    }

    #[test]
    fn should_return_none_given_key_not_read() {
        // Arrange
        let txn = Transaction::new(1, 100);

        // Act
        let version = txn.read_version(crate::api::DEFAULT_CF_ID.as_u32(), b"never_read");

        // Assert
        assert_eq!(version, None);
    }

    // ========================================================================
    // Conflict Detection Tests
    // ========================================================================

    #[test]
    fn should_detect_write_conflict_given_overlapping_write_sets() {
        // Arrange
        let mut txn1 = Transaction::new(1, 100);
        let mut txn2 = Transaction::new(2, 100);

        txn1.put(Bytes::from("key1"), Bytes::from("v1"), None)
            .unwrap();
        txn1.put(Bytes::from("key2"), Bytes::from("v2"), None)
            .unwrap();

        txn2.put(Bytes::from("key2"), Bytes::from("v3"), None)
            .unwrap();
        txn2.put(Bytes::from("key3"), Bytes::from("v4"), None)
            .unwrap();

        // Act
        let has_conflict = txn1.has_write_conflict(txn2.write_set());

        // Assert
        assert!(has_conflict, "Should detect conflict on key2");
    }

    #[test]
    fn should_not_detect_conflict_given_disjoint_write_sets() {
        // Arrange
        let mut txn1 = Transaction::new(1, 100);
        let mut txn2 = Transaction::new(2, 100);

        txn1.put(Bytes::from("key1"), Bytes::from("v1"), None)
            .unwrap();
        txn2.put(Bytes::from("key2"), Bytes::from("v2"), None)
            .unwrap();

        // Act
        let has_conflict = txn1.has_write_conflict(txn2.write_set());

        // Assert
        assert!(!has_conflict);
    }

    #[test]
    fn should_detect_conflict_given_delete_and_put_same_key() {
        // Arrange
        let mut txn1 = Transaction::new(1, 100);
        let mut txn2 = Transaction::new(2, 100);

        txn1.delete(Bytes::from("key")).unwrap();
        txn2.put(Bytes::from("key"), Bytes::from("value"), None)
            .unwrap();

        // Act
        let has_conflict = txn1.has_write_conflict(txn2.write_set());

        // Assert
        assert!(has_conflict);
    }

    // ========================================================================
    // Timeout Tests
    // ========================================================================

    #[test]
    fn should_not_expire_given_no_deadline_set() {
        // Arrange
        let txn = Transaction::new(1, 100);

        // Act
        let expired = txn.is_expired();

        // Assert
        assert!(!expired);
    }

    #[test]
    fn should_not_expire_given_deadline_not_reached() {
        // Arrange
        let txn = Transaction::with_options(1, 100, Some(std::time::Duration::from_secs(10)), 1024);

        // Act
        let expired = txn.is_expired();

        // Assert
        assert!(!expired);
    }

    #[test]
    fn should_expire_given_deadline_exceeded() {
        // Arrange
        let txn = Transaction::with_options(1, 100, Some(std::time::Duration::from_nanos(1)), 1024);

        // Act
        std::thread::sleep(std::time::Duration::from_millis(1));
        let expired = txn.is_expired();

        // Assert
        assert!(expired);
    }

    // ========================================================================
    // Memory Tracking Tests
    // ========================================================================

    #[test]
    fn should_track_memory_usage_given_put_operations() {
        // Arrange
        let mut txn = Transaction::new(1, 100);

        // Act
        txn.put(Bytes::from("key"), Bytes::from("value"), None)
            .unwrap();

        // Assert
        assert!(txn.current_memory > 0);
    }

    #[test]
    fn should_accumulate_memory_given_multiple_operations() {
        // Arrange
        let mut txn = Transaction::new(1, 100);

        // Act
        txn.put(Bytes::from("k1"), Bytes::from("v1"), None).unwrap();
        let mem1 = txn.current_memory;

        txn.put(Bytes::from("k2"), Bytes::from("v2"), None).unwrap();
        let mem2 = txn.current_memory;

        // Assert
        assert!(mem2 > mem1);
    }

    #[test]
    fn should_count_delete_memory_usage_given_delete_operation() {
        // Arrange
        let mut txn = Transaction::new(1, 100);

        // Act
        txn.delete(Bytes::from("deleted_key")).unwrap();

        // Assert
        assert!(txn.current_memory > 0);
    }

    // ========================================================================
    // Transaction Lifecycle Tests
    // ========================================================================

    #[test]
    fn should_mark_completed_given_commit() {
        // Arrange
        let txn = Transaction::new(1, 100);

        // Act
        let result = txn.commit();

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_fail_given_double_commit() {
        // Arrange
        let mut txn = Transaction::new(1, 100);
        txn.rollback();

        // Act
        let result = txn.commit();

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_clear_staged_mutations_given_rollback() {
        // Arrange
        let mut txn = Transaction::new(1, 100);
        txn.put(Bytes::from("key"), Bytes::from("value"), None)
            .unwrap();

        // Act
        txn.rollback();
        let result = txn.commit();

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_return_staged_mutations_given_successful_commit() {
        // Arrange
        let mut txn = Transaction::new(1, 100);
        txn.put(Bytes::from("k1"), Bytes::from("v1"), None).unwrap();
        txn.put(Bytes::from("k2"), Bytes::from("v2"), None).unwrap();

        // Act
        let mutations = txn.commit().expect("commit");

        // Assert
        assert_eq!(mutations.len(), 2);
    }

    #[test]
    fn should_have_correct_sequence_numbers_given_new_transaction() {
        // Arrange
        let begin_seq = 42;

        // Act
        let txn = Transaction::new(1, begin_seq);

        // Assert
        assert_eq!(txn.begin_sequence(), begin_seq);
        assert_eq!(txn.commit_sequence(), None);
    }

    #[test]
    fn should_return_read_versions_given_tracked_reads() {
        // Arrange
        let mut txn = Transaction::new(1, 100);

        // Act
        txn.track_read(crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("key1"), 50);
        txn.track_read(crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("key2"), 75);

        // Assert
        let read_versions = txn.read_versions();
        assert_eq!(read_versions.len(), 2);
        assert_eq!(
            read_versions.get(&(crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("key1"))),
            Some(&50)
        );
        assert_eq!(
            read_versions.get(&(crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("key2"))),
            Some(&75)
        );
    }

    // ========================================================================
    // Spill-to-Disk Tests
    // ========================================================================

    #[test]
    fn should_spill_to_disk_given_exceed_threshold_when_staging_writes() {
        // Arrange
        let memory_threshold = 100; // Very small threshold to trigger spill
        let mut txn = Transaction::with_options(1, 100, None, memory_threshold);
        let large_value = Bytes::from(vec![b'x'; 200]); // Larger than threshold

        // Act
        txn.put(Bytes::from("key1"), large_value.clone(), None)
            .unwrap();

        // Assert
        assert_eq!(
            txn.spill_files.len(),
            1,
            "Should have created one spill file"
        );
        assert_eq!(txn.current_memory, 0, "Memory should be reset after spill");
        assert_eq!(
            txn.staged.len(),
            0,
            "Staged mutations should be cleared after spill"
        );

        // Verify spill file exists
        assert!(
            txn.spill_files[0].exists(),
            "Spill file should exist on disk"
        );
    }

    #[test]
    fn should_read_from_spill_file_given_large_transaction_when_get() {
        // Arrange
        let memory_threshold = 50;
        let mut txn = Transaction::with_options(1, 100, None, memory_threshold);
        let value1 = Bytes::from(vec![b'a'; 100]);
        let value2 = Bytes::from(vec![b'b'; 20]);

        // Act
        txn.put(Bytes::from("spilled_key"), value1.clone(), None)
            .unwrap();
        txn.put(Bytes::from("memory_key"), value2.clone(), None)
            .unwrap();
        let mutations = txn.commit().unwrap();

        // Assert
        assert_eq!(
            mutations.len(),
            2,
            "Should have both spilled and in-memory mutations"
        );

        // Verify mutations are in correct order
        assert_eq!(mutations[0].key, Bytes::from("spilled_key"));
        assert_eq!(mutations[0].value, Some(value1));
        assert_eq!(mutations[1].key, Bytes::from("memory_key"));
        assert_eq!(mutations[1].value, Some(value2));
    }

    #[test]
    fn should_cleanup_spill_file_given_transaction_commit_when_completed() {
        // Arrange
        let memory_threshold = 50;
        let mut txn = Transaction::with_options(1, 100, None, memory_threshold);
        let large_value = Bytes::from(vec![b'x'; 100]);

        // Act
        txn.put(Bytes::from("key"), large_value, None).unwrap();

        let spill_path = txn.spill_files[0].clone();
        assert!(spill_path.exists(), "Spill file should exist before commit");

        txn.commit().unwrap();

        // Assert
        assert!(
            !spill_path.exists(),
            "Spill file should be cleaned up after commit"
        );
    }

    #[test]
    fn should_cleanup_spill_file_given_transaction_abort_when_rolled_back() {
        // Arrange
        let memory_threshold = 50;
        let mut txn = Transaction::with_options(1, 100, None, memory_threshold);
        let large_value = Bytes::from(vec![b'x'; 100]);

        // Act
        txn.put(Bytes::from("key"), large_value, None).unwrap();

        let spill_path = txn.spill_files[0].clone();
        assert!(
            spill_path.exists(),
            "Spill file should exist before rollback"
        );

        txn.rollback();

        // Assert
        assert!(
            !spill_path.exists(),
            "Spill file should be cleaned up after rollback"
        );
        assert_eq!(txn.spill_files.len(), 0, "Spill files list should be empty");
    }

    #[test]
    fn should_handle_multiple_spill_files_given_very_large_transaction() {
        // Arrange
        let memory_threshold = 100;
        let mut txn = Transaction::with_options(1, 100, None, memory_threshold);
        let large_value = Bytes::from(vec![b'x'; 150]);

        // Act
        txn.put(Bytes::from("key1"), large_value.clone(), None)
            .unwrap();
        assert_eq!(
            txn.spill_files.len(),
            1,
            "Should have one spill file after first write"
        );

        txn.put(Bytes::from("key2"), large_value.clone(), None)
            .unwrap();
        assert_eq!(
            txn.spill_files.len(),
            2,
            "Should have two spill files after second write"
        );

        txn.put(Bytes::from("key3"), large_value.clone(), None)
            .unwrap();

        // Assert
        assert_eq!(txn.spill_files.len(), 3, "Should have three spill files");

        // Verify all spill files exist
        for spill_path in &txn.spill_files {
            assert!(spill_path.exists(), "Each spill file should exist");
        }

        // Commit and verify all data is merged
        let mutations = txn.commit().unwrap();
        assert_eq!(
            mutations.len(),
            3,
            "Should have all mutations from all spill files"
        );
    }

    #[test]
    fn should_preserve_mutation_order_given_spill_and_memory_mutations() {
        // Arrange
        let memory_threshold = 50;
        let mut txn = Transaction::with_options(1, 100, None, memory_threshold);

        // Act
        txn.put(Bytes::from("key1"), Bytes::from(vec![b'a'; 100]), None)
            .unwrap(); // Spill
        txn.put(Bytes::from("key2"), Bytes::from("small"), None)
            .unwrap(); // Memory
        txn.put(Bytes::from("key3"), Bytes::from(vec![b'b'; 100]), None)
            .unwrap(); // Spill
        txn.put(Bytes::from("key4"), Bytes::from("tiny"), None)
            .unwrap(); // Memory

        let mutations = txn.commit().unwrap();

        // Assert
        assert_eq!(mutations.len(), 4);
        assert_eq!(mutations[0].key, Bytes::from("key1"));
        assert_eq!(mutations[1].key, Bytes::from("key2"));
        assert_eq!(mutations[2].key, Bytes::from("key3"));
        assert_eq!(mutations[3].key, Bytes::from("key4"));
    }

    #[test]
    fn should_cleanup_spill_files_on_drop_given_incomplete_transaction() {
        // Arrange
        let memory_threshold = 50;
        let spill_path;

        // Act
        {
            let mut txn = Transaction::with_options(1, 100, None, memory_threshold);
            let large_value = Bytes::from(vec![b'x'; 100]);
            txn.put(Bytes::from("key"), large_value, None).unwrap();

            spill_path = txn.spill_files[0].clone();
            assert!(spill_path.exists(), "Spill file should exist before drop");

            // Transaction dropped here without commit or rollback
        }

        // Assert
        assert!(
            !spill_path.exists(),
            "Spill file should be cleaned up on drop"
        );
    }

    #[test]
    fn should_handle_delete_operations_in_spill_file() {
        // Arrange
        let memory_threshold = 50;
        let mut txn = Transaction::with_options(1, 100, None, memory_threshold);

        // Act
        txn.put(Bytes::from("key1"), Bytes::from(vec![b'a'; 100]), None)
            .unwrap();
        txn.delete(Bytes::from("key2")).unwrap(); // Small, stays in memory
        txn.put(Bytes::from("key3"), Bytes::from(vec![b'b'; 100]), None)
            .unwrap();

        let mutations = txn.commit().unwrap();

        // Assert
        assert_eq!(mutations.len(), 3);
        assert!(matches!(mutations[0].op, MutationOp::Put));
        assert!(matches!(mutations[1].op, MutationOp::Delete));
        assert!(matches!(mutations[2].op, MutationOp::Put));
    }

    #[test]
    fn should_handle_delete_range_in_spill_file() {
        // Arrange
        let memory_threshold = 50;
        let mut txn = Transaction::with_options(1, 100, None, memory_threshold);

        // Act
        txn.put(Bytes::from("key1"), Bytes::from(vec![b'a'; 100]), None)
            .unwrap();
        txn.delete_range(Bytes::from("start"), Bytes::from("end"))
            .unwrap();

        let mutations = txn.commit().unwrap();

        // Assert
        assert_eq!(mutations.len(), 2);
        assert!(matches!(mutations[0].op, MutationOp::Put));
        assert!(matches!(mutations[1].op, MutationOp::DeleteRange));
        assert_eq!(mutations[1].range_end, Some(Bytes::from("end")));
    }
}
