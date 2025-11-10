//! Column family management operations.

use crate::api::column_family::{ColumnFamilyConfig, ColumnFamilyId};

use super::types::{ColumnFamilyMeta, Manifest};

impl Manifest {
    /// Add a new column family to the manifest.
    pub fn add_cf(
        &mut self,
        cf_id: ColumnFamilyId,
        name: String,
        config: Option<ColumnFamilyConfig>,
    ) {
        if !self.has_cf(cf_id) {
            self.column_families.push(ColumnFamilyMeta {
                id: cf_id.as_u32(),
                name,
                config,
            });
        }
    }

    /// Remove a column family and all its files from the manifest.
    pub fn remove_cf(&mut self, cf_id: ColumnFamilyId) {
        let cf_id_u32 = cf_id.as_u32();
        // Remove CF metadata
        self.column_families.retain(|cf| cf.id != cf_id_u32);
        // Remove all files for this CF
        self.files.retain(|f| f.cf_id != cf_id_u32);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::column_family::{CompactionStyle, CompressionType, DEFAULT_CF_ID};
    use super::super::types::FileMeta;

    #[test]
    fn should_add_column_family_with_config() {
        // Arrange
        let mut manifest = Manifest::default();
        let cf_id = ColumnFamilyId::new(1);
        let config = ColumnFamilyConfig {
            memtable_max_bytes: 32 * 1024 * 1024,
            compaction_style: CompactionStyle::SizeTiered,
            compression: CompressionType::Zstd,
            ..Default::default()
        };

        // Act
        manifest.add_cf(cf_id, "test_cf".to_string(), Some(config.clone()));

        // Assert
        assert!(manifest.has_cf(cf_id));
    }

    #[test]
    fn should_retrieve_column_family_with_config() {
        // Arrange
        let mut manifest = Manifest::default();
        let cf_id = ColumnFamilyId::new(1);
        let config = ColumnFamilyConfig {
            memtable_max_bytes: 32 * 1024 * 1024,
            compaction_style: CompactionStyle::SizeTiered,
            compression: CompressionType::Zstd,
            ..Default::default()
        };
        manifest.add_cf(cf_id, "test_cf".to_string(), Some(config.clone()));

        // Act
        let cf_meta = manifest.get_cf(cf_id).expect("cf should exist");

        // Assert
        assert_eq!(cf_meta.name, "test_cf");
        assert!(cf_meta.config.is_some());
    }

    #[test]
    fn should_retrieve_column_family_by_name() {
        // Arrange
        let mut manifest = Manifest::default();
        manifest.add_cf(ColumnFamilyId::new(1), "alpha".to_string(), None);
        manifest.add_cf(ColumnFamilyId::new(2), "beta".to_string(), None);

        // Act
        let cf = manifest.get_cf_by_name("beta").expect("beta should exist");

        // Assert
        assert_eq!(cf.id, 2);
        assert_eq!(cf.name, "beta");
    }

    #[test]
    fn should_remove_column_family() {
        // Arrange
        let mut manifest = Manifest::default();
        let cf_id = ColumnFamilyId::new(5);
        manifest.add_cf(cf_id, "to_remove".to_string(), None);

        // Act
        manifest.remove_cf(cf_id);

        // Assert
        assert!(!manifest.has_cf(cf_id));
    }

    #[test]
    fn should_remove_associated_files_when_removing_column_family() {
        // Arrange
        let mut manifest = Manifest::default();
        let cf_id = ColumnFamilyId::new(5);
        manifest.add_cf(cf_id, "to_remove".to_string(), None);
        manifest.files.push(FileMeta {
            name: "cf_5_L0_001.sst".to_string(),
            level: 0,
            cf_id: 5,
            ..Default::default()
        });
        manifest.files.push(FileMeta {
            name: "cf_5_L1_002.sst".to_string(),
            level: 1,
            cf_id: 5,
            ..Default::default()
        });

        // Act
        manifest.remove_cf(cf_id);

        // Assert
        assert_eq!(manifest.files.len(), 0);
    }

    #[test]
    fn should_return_files_for_default_column_family_at_level_zero() {
        // Arrange
        let mut manifest = Manifest::default();
        manifest.files.push(FileMeta {
            name: "cf_0_L0_001.sst".to_string(),
            level: 0,
            cf_id: 0,
            ..Default::default()
        });
        manifest.files.push(FileMeta {
            name: "cf_1_L0_002.sst".to_string(),
            level: 0,
            cf_id: 1,
            ..Default::default()
        });

        // Act
        let result = manifest.files_for_cf_at_level(DEFAULT_CF_ID, 0);

        // Assert
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "cf_0_L0_001.sst");
    }

    #[test]
    fn should_return_files_for_custom_column_family_at_level_zero() {
        // Arrange
        let mut manifest = Manifest::default();
        manifest.files.push(FileMeta {
            name: "cf_0_L0_001.sst".to_string(),
            level: 0,
            cf_id: 0,
            ..Default::default()
        });
        manifest.files.push(FileMeta {
            name: "cf_1_L0_002.sst".to_string(),
            level: 0,
            cf_id: 1,
            ..Default::default()
        });

        // Act
        let result = manifest.files_for_cf_at_level(ColumnFamilyId::new(1), 0);

        // Assert
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "cf_1_L0_002.sst");
    }

    #[test]
    fn should_return_files_for_custom_column_family_at_level_one() {
        // Arrange
        let mut manifest = Manifest::default();
        manifest.files.push(FileMeta {
            name: "cf_1_L0_002.sst".to_string(),
            level: 0,
            cf_id: 1,
            ..Default::default()
        });
        manifest.files.push(FileMeta {
            name: "cf_1_L1_003.sst".to_string(),
            level: 1,
            cf_id: 1,
            ..Default::default()
        });

        // Act
        let result = manifest.files_for_cf_at_level(ColumnFamilyId::new(1), 1);

        // Assert
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].name, "cf_1_L1_003.sst");
    }

    #[test]
    fn should_prevent_removal_of_default_column_family() {
        // Arrange
        let mut manifest = Manifest::default();
        manifest.add_cf(DEFAULT_CF_ID, "default".to_string(), None);
        manifest.add_cf(ColumnFamilyId::new(1), "cf1".to_string(), None);

        // Act
        manifest.remove_cf(DEFAULT_CF_ID);

        // Assert
        assert!(!manifest.has_cf(DEFAULT_CF_ID));
    }
}
