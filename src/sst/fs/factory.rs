use std::path::Path;

use crate::error::MidgeResult;
use crate::sst::traits::{SstReaderFactory, SstStateReader};

use super::reader::SstFile;
use super::writer::FsDynWriter;
use crate::common::codec::CompressionType;
use crate::sst::traits::SstFactory;

pub struct FsSstReaderFactory {
    paranoid_checksums: bool,
}

impl FsSstReaderFactory {
    pub fn new(paranoid_checksums: bool) -> Self {
        Self { paranoid_checksums }
    }
}

impl SstReaderFactory for FsSstReaderFactory {
    fn open(&self, path: &Path) -> MidgeResult<Box<dyn SstStateReader>> {
        let sst = SstFile::open_with_paranoid(path, self.paranoid_checksums)?;
        Ok(Box::new(sst))
    }
}

/// Filesystem-backed SstFactory producing streaming writers.
#[derive(Clone)]
pub struct FsSstFactory {
    pub temp_dir: std::path::PathBuf,
}

impl FsSstFactory {
    pub fn new(temp_dir: std::path::PathBuf) -> Self {
        // Ensure temp directory exists (tests / ephemeral envs may not create it).
        if !temp_dir.exists() {
            if let Err(e) = std::fs::create_dir_all(&temp_dir) {
                tracing::warn!(
                    "failed to create sst temp_dir {}: {}",
                    temp_dir.display(),
                    e
                );
            }
        }

        // Cleanup orphaned temp SST files left by previous crashes.
        // Remove files named "*.sst.tmp" that are older than 60 seconds.
        if temp_dir.exists() && temp_dir.is_dir() {
            if let Ok(entries) = std::fs::read_dir(&temp_dir) {
                for ent in entries.flatten() {
                    if let Some(name) = ent.file_name().to_str() {
                        if name.ends_with(".sst.tmp") {
                            if let Ok(meta) = ent.metadata() {
                                if let Ok(modified) = meta.modified() {
                                    if let Ok(elapsed) = modified.elapsed() {
                                        if elapsed.as_secs() > 60 {
                                            let _ = std::fs::remove_file(ent.path());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Self { temp_dir }
    }
}

impl SstFactory for FsSstFactory {
    fn create(
        &self,
        compression: CompressionType,
        block_size: usize,
        use_internal: bool,
    ) -> Box<dyn crate::sst::DynSstWriter> {
        match FsDynWriter::new(&self.temp_dir, compression, block_size, use_internal) {
            Ok(w) => Box::new(w),
            Err(e) => {
                tracing::error!(
                    "Failed to create FsDynWriter in {}: {}",
                    self.temp_dir.display(),
                    e
                );
                // Try falling back to the OS temp dir as a best-effort recovery for tests
                let sys_tmp = std::env::temp_dir();
                match FsDynWriter::new(&sys_tmp, compression, block_size, use_internal) {
                    Ok(w2) => {
                        tracing::warn!(
                            "FsDynWriter fell back to system temp dir: {}",
                            sys_tmp.display()
                        );
                        Box::new(w2)
                    }
                    Err(e2) => {
                        // If even the system temp fails, panic with both errors for debugging
                        panic!(
                            "Failed to create FsDynWriter in {} and fallback {}: {}",
                            self.temp_dir.display(),
                            sys_tmp.display(),
                            e2
                        );
                    }
                }
            }
        }
    }

    fn create_with_bloom(
        &self,
        compression: CompressionType,
        block_size: usize,
        use_internal: bool,
        _bloom_bits_per_key: u32,
    ) -> Box<dyn crate::sst::DynSstWriter> {
        // FsDynWriter currently ignores bloom bits and uses default of 10
        self.create(compression, block_size, use_internal)
    }
}
