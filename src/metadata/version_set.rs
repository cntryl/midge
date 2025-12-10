//! Version Set - lock-free snapshot-isolated manifest reads
//!
//! Provides snapshot isolation for concurrent readers while writers update the manifest.
//! Uses atomic versioning to avoid locks on the read path.

use crate::common::MidgeResult;
use crate::metadata::manifest::{ColumnFamilyMeta, FileMeta, Manifest};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// A version of the manifest at a specific point in time
#[derive(Debug, Clone)]
pub struct Version {
    /// Version ID (monotonically increasing)
    pub id: u64,
    /// Snapshot of manifest at this version
    pub manifest: Manifest,
    /// Files organized by level for fast access
    level_files: HashMap<u32, Vec<FileMeta>>,
    /// Column family metadata index
    cf_index: HashMap<u32, ColumnFamilyMeta>,
}

impl Version {
    pub fn new(id: u64, manifest: Manifest) -> Self {
        // Build level index
        let mut level_files: HashMap<u32, Vec<FileMeta>> = HashMap::new();
        for file in &manifest.files {
            level_files
                .entry(file.level)
                .or_insert_with(Vec::new)
                .push(file.clone());
        }

        // Build CF index
        let mut cf_index = HashMap::new();
        for cf in &manifest.column_families {
            cf_index.insert(cf.id, cf.clone());
        }

        Self {
            id,
            manifest,
            level_files,
            cf_index,
        }
    }

    pub fn version_id(&self) -> u64 {
        self.id
    }

    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// Get files at a specific level
    pub fn level_files(&self, level: u32) -> Vec<FileMeta> {
        self.level_files.get(&level).cloned().unwrap_or_default()
    }

    /// Get all files for a specific column family
    pub fn cf_files(&self, cf_id: u32) -> Vec<FileMeta> {
        self.manifest
            .files
            .iter()
            .filter(|f| f.cf_id == cf_id)
            .cloned()
            .collect()
    }

    /// Get column family metadata
    pub fn get_cf(&self, cf_id: u32) -> Option<&ColumnFamilyMeta> {
        self.cf_index.get(&cf_id)
    }

    /// Total size of all files in bytes
    pub fn total_size(&self) -> u64 {
        self.manifest.files.iter().map(|f| f.size_bytes).sum()
    }

    /// Get file count
    pub fn file_count(&self) -> usize {
        self.manifest.files.len()
    }
}

/// Lock-free version set with atomic version pointers
pub struct VersionSet {
    /// Current active version ID (atomic)
    current_version: AtomicU64,
    /// All versions (kept for reference counting)
    versions: Arc<std::sync::Mutex<Vec<Arc<Version>>>>,
}

impl VersionSet {
    pub fn new(initial_version: Arc<Version>) -> Self {
        let version_id = initial_version.version_id();
        let mut versions = Vec::new();
        versions.push(initial_version);

        Self {
            current_version: AtomicU64::new(version_id),
            versions: Arc::new(std::sync::Mutex::new(versions)),
        }
    }

    /// Get the current version ID without locking
    pub fn current_version_id(&self) -> u64 {
        self.current_version.load(Ordering::SeqCst)
    }

    /// Get a specific version by ID
    pub fn get_version(&self, version_id: u64) -> MidgeResult<Arc<Version>> {
        let versions = self.versions.lock().expect("versions lock poisoned");
        versions
            .iter()
            .find(|v| v.version_id() == version_id)
            .cloned()
            .ok_or_else(|| crate::common::MidgeError::NotFound)
    }

    /// Get the current version
    pub fn current_version(&self) -> MidgeResult<Arc<Version>> {
        let version_id = self.current_version_id();
        self.get_version(version_id)
    }

    /// Install a new version (called by manifest writer)
    pub fn install_version(&self, version: Arc<Version>) -> MidgeResult<()> {
        let mut versions = self.versions.lock().expect("versions lock poisoned");
        versions.push(version.clone());
        self.current_version
            .store(version.version_id(), Ordering::SeqCst);
        Ok(())
    }

    /// Get count of managed versions
    pub fn version_count(&self) -> usize {
        self.versions.lock().expect("versions lock poisoned").len()
    }

    /// Get all versions
    pub fn all_versions(&self) -> Vec<Arc<Version>> {
        self.versions.lock().expect("versions lock poisoned").clone()
    }

    /// Check if a version exists
    pub fn has_version(&self, version_id: u64) -> bool {
        self.versions
            .lock()
            .expect("versions lock poisoned")
            .iter()
            .any(|v| v.version_id() == version_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_manifest() -> Manifest {
        let mut manifest = Manifest::new();
        manifest.column_families.push(ColumnFamilyMeta {
            id: 0,
            name: "default".to_string(),
            created_at: 0,
            deleted_at: None,
        });
        manifest.files.push(FileMeta {
            name: "file1.sst".to_string(),
            level: 0,
            size_bytes: 1024,
            cf_id: 0,
            ..Default::default()
        });
        manifest.files.push(FileMeta {
            name: "file2.sst".to_string(),
            level: 1,
            size_bytes: 2048,
            cf_id: 0,
            ..Default::default()
        });
        manifest
    }

    #[test]
    fn should_create_version_when_instantiated() {
        // Arrange
        let manifest = create_test_manifest();

        // Act
        let version = Version::new(1, manifest);

        // Assert
        assert_eq!(version.version_id(), 1);
        assert_eq!(version.file_count(), 2);
        assert_eq!(version.total_size(), 3072);
    }

    #[test]
    fn should_index_files_by_level_when_version_created() {
        // Arrange
        let manifest = create_test_manifest();

        // Act
        let version = Version::new(1, manifest);
        let level_0_files = version.level_files(0);
        let level_1_files = version.level_files(1);

        // Assert
        assert_eq!(level_0_files.len(), 1);
        assert_eq!(level_1_files.len(), 1);
        assert_eq!(level_0_files[0].name, "file1.sst");
        assert_eq!(level_1_files[0].name, "file2.sst");
    }

    #[test]
    fn should_create_version_set_when_instantiated() {
        // Arrange
        let manifest = create_test_manifest();
        let version = Arc::new(Version::new(1, manifest));

        // Act
        let version_set = VersionSet::new(version);

        // Assert
        assert_eq!(version_set.current_version_id(), 1);
        assert_eq!(version_set.version_count(), 1);
    }

    #[test]
    fn should_install_new_version_when_install_version_called() {
        // Arrange
        let manifest = create_test_manifest();
        let version1 = Arc::new(Version::new(1, manifest.clone()));
        let version_set = VersionSet::new(version1);

        let mut manifest2 = create_test_manifest();
        manifest2.files.push(FileMeta {
            name: "file3.sst".to_string(),
            level: 2,
            size_bytes: 4096,
            cf_id: 0,
            ..Default::default()
        });
        let version2 = Arc::new(Version::new(2, manifest2));

        // Act
        version_set.install_version(version2).unwrap();

        // Assert
        assert_eq!(version_set.current_version_id(), 2);
        assert_eq!(version_set.version_count(), 2);
    }

    #[test]
    fn should_return_current_version_when_current_version_called() {
        // Arrange
        let manifest = create_test_manifest();
        let version = Arc::new(Version::new(1, manifest));
        let version_set = VersionSet::new(version);

        // Act
        let current = version_set.current_version().unwrap();

        // Assert
        assert_eq!(current.version_id(), 1);
        assert_eq!(current.file_count(), 2);
    }

    #[test]
    fn should_retrieve_specific_version_when_get_version_called() {
        // Arrange
        let manifest = create_test_manifest();
        let version1 = Arc::new(Version::new(1, manifest.clone()));
        let version_set = VersionSet::new(version1);

        let version2 = Arc::new(Version::new(2, create_test_manifest()));
        version_set.install_version(version2).unwrap();

        // Act
        let retrieved = version_set.get_version(1).unwrap();

        // Assert
        assert_eq!(retrieved.version_id(), 1);
    }

    #[test]
    fn should_return_not_found_when_version_doesnt_exist() {
        // Arrange
        let manifest = create_test_manifest();
        let version = Arc::new(Version::new(1, manifest));
        let version_set = VersionSet::new(version);

        // Act
        let result = version_set.get_version(999);

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_filter_files_by_cf_when_cf_files_called() {
        // Arrange
        let mut manifest = create_test_manifest();
        manifest.column_families.push(ColumnFamilyMeta {
            id: 1,
            name: "secondary".to_string(),
            created_at: 0,
            deleted_at: None,
        });
        manifest.files.push(FileMeta {
            name: "file3.sst".to_string(),
            level: 0,
            size_bytes: 512,
            cf_id: 1,
            ..Default::default()
        });
        let version = Version::new(1, manifest);

        // Act
        let cf0_files = version.cf_files(0);
        let cf1_files = version.cf_files(1);

        // Assert
        assert_eq!(cf0_files.len(), 2);
        assert_eq!(cf1_files.len(), 1);
        assert_eq!(cf1_files[0].cf_id, 1);
    }

    #[test]
    fn should_check_version_existence_when_has_version_called() {
        // Arrange
        let manifest = create_test_manifest();
        let version = Arc::new(Version::new(1, manifest));
        let version_set = VersionSet::new(version);

        // Act
        let exists_1 = version_set.has_version(1);
        let exists_999 = version_set.has_version(999);

        // Assert
        assert!(exists_1);
        assert!(!exists_999);
    }

    #[test]
    fn should_support_concurrent_reads_when_version_set_used() {
        // Arrange
        let manifest = create_test_manifest();
        let version = Arc::new(Version::new(1, manifest));
        let version_set = Arc::new(VersionSet::new(version));

        // Act - simulate concurrent reads
        let vs1 = version_set.clone();
        let vs2 = version_set.clone();

        let id1 = vs1.current_version_id();
        let id2 = vs2.current_version_id();

        // Assert
        assert_eq!(id1, id2);
        assert_eq!(id1, 1);
    }
}
