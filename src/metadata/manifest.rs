//! Manifest data structures and operations
//!
//! The manifest is the source of truth for all SST files, column families,
//! and database metadata.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Core manifest structure tracking all SSTs and metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// Last persisted sequence number
    pub last_persisted_sequence: u64,
    /// List of all SST file names
    pub ssts: Vec<String>,
    /// SST file metadata
    #[serde(default)]
    pub files: Vec<FileMeta>,
    /// Column families
    #[serde(default)]
    pub column_families: Vec<ColumnFamilyMeta>,
    /// Cloud checkpoint info for WAL coordination
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cloud_checkpoint: Option<CloudCheckpoint>,
    /// Next WAL sequence number
    #[serde(default = "default_next_wal_seq")]
    pub next_wal_seq: u64,
    /// Next SST sequence numbers per CF
    #[serde(default)]
    pub next_sst_seqs: HashMap<u32, u64>,
}

impl Default for Manifest {
    fn default() -> Self {
        Self {
            last_persisted_sequence: 0,
            ssts: Vec::new(),
            files: Vec::new(),
            column_families: Vec::new(),
            cloud_checkpoint: None,
            next_wal_seq: 1,
            next_sst_seqs: HashMap::new(),
        }
    }
}

fn default_next_wal_seq() -> u64 {
    1
}

/// Cloud checkpoint for WAL coordination
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudCheckpoint {
    /// Highest WAL sequence fully materialized to cloud
    pub checkpoint_sequence: u64,
    /// SST files covering the checkpoint
    pub covering_ssts: Vec<String>,
}

/// Column family metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnFamilyMeta {
    pub id: u32,
    pub name: String,
}

/// File metadata for an SST
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FileMeta {
    pub name: String,
    pub level: u32,
    pub size_bytes: u64,
    #[serde(default)]
    pub cf_id: u32,
    #[serde(default)]
    pub sst_seq: u64,
    #[serde(default)]
    pub smallest_key: Option<Vec<u8>>,
    #[serde(default)]
    pub largest_key: Option<Vec<u8>>,
    #[serde(default)]
    pub smallest_seq: Option<u64>,
    #[serde(default)]
    pub largest_seq: Option<u64>,
    #[serde(default)]
    pub sublevel: u32,
}

impl Manifest {
    /// Create a new manifest
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the next WAL sequence
    pub fn next_wal_seq(&self) -> u64 {
        self.next_wal_seq
    }

    /// Increment WAL sequence
    pub fn increment_wal_seq(&mut self) {
        self.next_wal_seq += 1;
    }

    /// Get all files at a specific level
    pub fn files_at_level(&self, level: u32) -> Vec<&FileMeta> {
        self.files
            .iter()
            .filter(|f| f.level == level)
            .collect()
    }

    /// Add a file to the manifest
    pub fn add_file(&mut self, file: FileMeta) {
        self.files.push(file);
    }

    /// Remove a file from the manifest
    pub fn remove_file(&mut self, name: &str) {
        self.files.retain(|f| f.name != name);
    }
}
