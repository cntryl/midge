//! Spill-to-disk functionality for large transactions.
//!
//! When a transaction's staged mutations exceed a memory threshold, the staged
//! mutations are serialized to temporary files on disk. This allows transactions
//! to handle arbitrarily large write sets without consuming excessive memory.

use crate::api::mutation::{Mutation, MutationOp};
use crate::api::ColumnFamilyId;
use crate::error::MidgeError;
use bytes::Bytes;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::PathBuf;

/// Manages spill files for transaction mutations.
pub struct SpillManager {
    txn_id: u64,
    spill_files: Vec<PathBuf>,
}

impl SpillManager {
    /// Create a new spill manager for the given transaction ID.
    pub fn new(txn_id: u64) -> Self {
        Self {
            txn_id,
            spill_files: Vec::new(),
        }
    }

    /// Spill staged mutations to disk, clearing the in-memory staging area.
    ///
    /// Creates a unique temporary file and serializes all mutations in binary format.
    /// The serialization format is:
    /// - op_type: u8 (0=Put, 1=Insert, 2=Delete, 3=DeleteRange, 4=CompareAndSwap, 5=Merge)
    /// - cf_id: u32 (little-endian)
    /// - key_len: u32 (little-endian)
    /// - key: [u8; key_len]
    /// - value_len: u32 (little-endian)
    /// - value/range_end: [u8; value_len]
    /// - For CompareAndSwap: additional expected_len: u32 + expected: [u8; expected_len]
    pub fn spill_to_disk(&mut self, staged: &[Mutation]) -> Result<(), MidgeError> {
        if staged.is_empty() {
            return Ok(());
        }

        // Create temporary spill file with unique name using process ID and timestamp
        let pid = std::process::id();
        let timestamp = match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
            Ok(d) => d.as_nanos(),
            Err(e) => {
                return Err(MidgeError::internal(format!(
                    "System time error when creating spill file: {}",
                    e
                )));
            }
        };
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
        for mutation in staged {
            // Serialize mutation: format is:
            // [op_type: u8][cf_id: u32][key_len: u32][key][value_len: u32][value_or_range_end]
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

        self.spill_files.push(spill_path);

        Ok(())
    }

    /// Read all mutations from spill files.
    ///
    /// Returns mutations in the order they were spilled across all files.
    pub fn read_spill_files(&self) -> Result<Vec<Mutation>, MidgeError> {
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
                let cf_id_obj = ColumnFamilyId::from(cf_id);
                let mutation = match op_byte[0] {
                    0 => Mutation::put_cf(cf_id_obj, key, value_or_end.unwrap_or_default(), None),
                    1 => {
                        Mutation::insert_cf(cf_id_obj, key, value_or_end.unwrap_or_default(), None)
                    }
                    2 => Mutation::delete_cf(cf_id_obj, key),
                    3 => {
                        Mutation::delete_range_cf(cf_id_obj, key, value_or_end.unwrap_or_default())
                    }
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

    /// Cleanup all spill files from disk.
    ///
    /// Called when the transaction is committed, aborted, or dropped.
    pub fn cleanup_spill_files(&mut self) {
        for spill_path in &self.spill_files {
            let _ = std::fs::remove_file(spill_path);
        }
        self.spill_files.clear();
    }

    /// Check if there are any spill files.
    pub fn has_spill_files(&self) -> bool {
        !self.spill_files.is_empty()
    }

    /// Get the number of spill files.
    pub fn spill_file_count(&self) -> usize {
        self.spill_files.len()
    }

    /// Get the spill file paths (for testing).
    #[cfg(test)]
    pub fn spill_file_paths(&self) -> &[PathBuf] {
        &self.spill_files
    }
}

impl Drop for SpillManager {
    fn drop(&mut self) {
        self.cleanup_spill_files();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_spill_manager_successfully() {
        // Arrange

        // Act
        let manager = SpillManager::new(100);

        // Assert
        assert_eq!(manager.txn_id, 100);
        assert!(!manager.has_spill_files());
        assert_eq!(manager.spill_file_count(), 0);
    }

    #[test]
    fn should_spill_put_mutations_to_disk() {
        // Arrange
        let mut manager = SpillManager::new(1);
        let mutations = vec![Mutation::put(
            Bytes::from("key1"),
            Bytes::from("value1"),
            None,
        )];

        // Act
        let result = manager.spill_to_disk(&mutations);

        // Assert
        assert!(result.is_ok());
        assert!(manager.has_spill_files());
        assert_eq!(manager.spill_file_count(), 1);
    }

    #[test]
    fn should_read_spilled_put_mutations() {
        // Arrange
        let mut manager = SpillManager::new(2);
        let mutations = vec![
            Mutation::put(Bytes::from("key1"), Bytes::from("value1"), None),
            Mutation::put(Bytes::from("key2"), Bytes::from("value2"), None),
        ];
        manager.spill_to_disk(&mutations).unwrap();

        // Act
        let result = manager.read_spill_files();

        // Assert
        assert!(result.is_ok());
        let read_mutations = result.unwrap();
        assert_eq!(read_mutations.len(), 2);
        assert_eq!(read_mutations[0].key, Bytes::from("key1"));
        assert_eq!(read_mutations[0].value, Some(Bytes::from("value1")));
    }

    #[test]
    fn should_handle_delete_mutations_in_spill() {
        // Arrange
        let mut manager = SpillManager::new(3);
        let mutations = vec![Mutation::delete(Bytes::from("key1"))];
        manager.spill_to_disk(&mutations).unwrap();

        // Act
        let result = manager.read_spill_files();

        // Assert
        assert!(result.is_ok());
        let read_mutations = result.unwrap();
        assert_eq!(read_mutations.len(), 1);
        assert_eq!(read_mutations[0].key, Bytes::from("key1"));
        assert!(matches!(read_mutations[0].op, MutationOp::Delete));
    }

    #[test]
    fn should_handle_delete_range_mutations_in_spill() {
        // Arrange
        let mut manager = SpillManager::new(4);
        let mutations = vec![Mutation::delete_range(
            Bytes::from("key1"),
            Bytes::from("key9"),
        )];
        manager.spill_to_disk(&mutations).unwrap();

        // Act
        let result = manager.read_spill_files();

        // Assert
        assert!(result.is_ok());
        let read_mutations = result.unwrap();
        assert_eq!(read_mutations.len(), 1);
        assert!(matches!(read_mutations[0].op, MutationOp::DeleteRange));
        assert_eq!(read_mutations[0].range_end, Some(Bytes::from("key9")));
    }

    #[test]
    fn should_handle_merge_mutations_in_spill() {
        // Arrange
        let mut manager = SpillManager::new(5);
        let mutations = vec![Mutation::merge(Bytes::from("counter"), Bytes::from("1"))];
        manager.spill_to_disk(&mutations).unwrap();

        // Act
        let result = manager.read_spill_files();

        // Assert
        assert!(result.is_ok());
        let read_mutations = result.unwrap();
        assert_eq!(read_mutations.len(), 1);
        assert!(matches!(read_mutations[0].op, MutationOp::Merge));
    }

    #[test]
    fn should_preserve_mutation_order_across_spills() {
        // Arrange
        let mut manager = SpillManager::new(6);
        let batch1 = vec![
            Mutation::put(Bytes::from("a"), Bytes::from("1"), None),
            Mutation::put(Bytes::from("b"), Bytes::from("2"), None),
        ];
        let batch2 = vec![
            Mutation::put(Bytes::from("c"), Bytes::from("3"), None),
            Mutation::put(Bytes::from("d"), Bytes::from("4"), None),
        ];
        manager.spill_to_disk(&batch1).unwrap();
        manager.spill_to_disk(&batch2).unwrap();

        // Act
        let result = manager.read_spill_files();

        // Assert
        assert!(result.is_ok());
        let mutations = result.unwrap();
        assert_eq!(mutations.len(), 4);
        assert_eq!(mutations[0].key, Bytes::from("a"));
        assert_eq!(mutations[1].key, Bytes::from("b"));
        assert_eq!(mutations[2].key, Bytes::from("c"));
        assert_eq!(mutations[3].key, Bytes::from("d"));
    }

    #[test]
    fn should_cleanup_spill_files_successfully() {
        // Arrange
        let mut manager = SpillManager::new(7);
        let mutations = vec![Mutation::put(
            Bytes::from("key1"),
            Bytes::from("value1"),
            None,
        )];
        manager.spill_to_disk(&mutations).unwrap();
        let spill_paths = manager.spill_file_paths().to_vec();
        assert!(spill_paths[0].exists());

        // Act
        manager.cleanup_spill_files();

        // Assert
        assert!(!manager.has_spill_files());
        assert!(!spill_paths[0].exists());
    }

    #[test]
    fn should_return_ok_when_spilling_empty_mutations() {
        // Arrange
        let mut manager = SpillManager::new(8);
        let mutations: Vec<Mutation> = vec![];

        // Act
        let result = manager.spill_to_disk(&mutations);

        // Assert
        assert!(result.is_ok());
        assert!(!manager.has_spill_files());
    }

    #[test]
    fn should_handle_large_values_in_spill() {
        // Arrange
        let mut manager = SpillManager::new(9);
        let large_value = vec![0u8; 100_000];
        let mutations = vec![Mutation::put(
            Bytes::from("large_key"),
            Bytes::from(large_value.clone()),
            None,
        )];
        manager.spill_to_disk(&mutations).unwrap();

        // Act
        let result = manager.read_spill_files();

        // Assert
        assert!(result.is_ok());
        let read_mutations = result.unwrap();
        assert_eq!(read_mutations[0].value.as_ref().unwrap().len(), 100_000);
    }

    #[test]
    fn should_cleanup_spill_files_on_drop() {
        // Arrange
        let mut manager = SpillManager::new(10);
        let mutations = vec![Mutation::put(
            Bytes::from("key1"),
            Bytes::from("value1"),
            None,
        )];
        manager.spill_to_disk(&mutations).unwrap();
        let spill_paths = manager.spill_file_paths().to_vec();
        assert!(spill_paths[0].exists());

        // Act
        drop(manager);

        // Assert
        assert!(!spill_paths[0].exists());
    }

    #[test]
    fn should_handle_mixed_mutation_types_in_spill() {
        // Arrange
        let mut manager = SpillManager::new(11);
        let mutations = vec![
            Mutation::put(Bytes::from("k1"), Bytes::from("v1"), None),
            Mutation::delete(Bytes::from("k2")),
            Mutation::merge(Bytes::from("k3"), Bytes::from("v3")),
            Mutation::delete_range(Bytes::from("k4"), Bytes::from("k9")),
        ];
        manager.spill_to_disk(&mutations).unwrap();

        // Act
        let result = manager.read_spill_files();

        // Assert
        assert!(result.is_ok());
        let read_mutations = result.unwrap();
        assert_eq!(read_mutations.len(), 4);
        assert!(matches!(read_mutations[0].op, MutationOp::Put));
        assert!(matches!(read_mutations[1].op, MutationOp::Delete));
        assert!(matches!(read_mutations[2].op, MutationOp::Merge));
        assert!(matches!(read_mutations[3].op, MutationOp::DeleteRange));
    }

    #[test]
    fn should_handle_mutations_with_different_column_families() {
        // Arrange
        let mut manager = SpillManager::new(12);
        let cf1 = ColumnFamilyId::new(0);
        let cf2 = ColumnFamilyId::new(1);
        let mutations = vec![
            Mutation::put_cf(cf1, Bytes::from("key1"), Bytes::from("value1"), None),
            Mutation::put_cf(cf2, Bytes::from("key2"), Bytes::from("value2"), None),
        ];
        manager.spill_to_disk(&mutations).unwrap();

        // Act
        let result = manager.read_spill_files();

        // Assert
        assert!(result.is_ok());
        let read_mutations = result.unwrap();
        assert_eq!(read_mutations.len(), 2);
        assert_eq!(read_mutations[0].cf_id, cf1);
        assert_eq!(read_mutations[1].cf_id, cf2);
    }
}
