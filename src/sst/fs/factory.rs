use std::path::Path;

use crate::error::MidgeResult;
use crate::sst::traits::{SstReaderFactory, SstStateReader};

use super::reader::SstFile;
use super::writer::FsDynWriter;
use crate::error::{MidgeError};
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
    pub test_hooks: Option<crate::common::test_hooks::TestHooks>,
}

impl FsSstFactory {
    pub fn new(temp_dir: std::path::PathBuf) -> Self {
        Self::new_with_hooks(temp_dir, None)
    }

    /// Create a new FsSstFactory and optionally attach `test_hooks` so SST
    /// writers created by this factory can honor fsync/test instrumentation.
    pub fn new_with_hooks(
        temp_dir: std::path::PathBuf,
        test_hooks: Option<crate::common::test_hooks::TestHooks>,
    ) -> Self {
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

        Self {
            temp_dir,
            test_hooks,
        }
    }
}

impl SstFactory for FsSstFactory {
    fn create(
        &self,
        compression: CompressionType,
        block_size: usize,
        use_internal: bool,
    ) -> crate::error::MidgeResult<Box<dyn crate::sst::DynSstWriter>> {
        // Propagate test hooks through the SST factory if available later.
        // Currently create() is called from engine initialization where
        // test hooks are not readily available; use None by default.
        // Try to create an FS-backed writer. If that fails, attempt to
        // fallback to system temp dir. If that fails as well, return an
        // erroring writer that always returns a MidgeError instead of panicking.
        match FsDynWriter::new(
            &self.temp_dir,
            compression,
            block_size,
            use_internal,
            self.test_hooks.clone(),
        ) {
            Ok(w) => Ok(Box::new(w)),
            Err(e) => {
                tracing::error!(
                    "Failed to create FsDynWriter in {}: {}",
                    self.temp_dir.display(),
                    e
                );
                // Try falling back to the OS temp dir as a best-effort recovery for tests
                let sys_tmp = std::env::temp_dir();
                match FsDynWriter::new(
                    &sys_tmp,
                    compression,
                    block_size,
                    use_internal,
                    self.test_hooks.clone(),
                ) {
                    Ok(w2) => {
                        tracing::warn!(
                            "FsDynWriter fell back to system temp dir: {}",
                            sys_tmp.display()
                        );
                        Ok(Box::new(w2))
                    }
                    Err(e2) => {
                        tracing::error!(
                            "Failed to create FsDynWriter in {} and fallback {}: {}",
                            self.temp_dir.display(),
                            sys_tmp.display(),
                            e2
                        );
                        // Create a writer that fails on operations instead of panicking
                        Err(MidgeError::internal(format!(
                            "Failed to create FsDynWriter in {} or fallback {}: {}",
                            self.temp_dir.display(),
                            sys_tmp.display(),
                            e2
                        )))
                    }
                }
            }
        }
    }

    fn create_with_seq(
        &self,
        compression: CompressionType,
        block_size: usize,
        use_internal: bool,
        sst_seq: u64,
    ) -> crate::error::MidgeResult<Box<dyn crate::sst::DynSstWriter>> {
        match FsDynWriter::new_with_seq(
            &self.temp_dir,
            compression,
            block_size,
            use_internal,
            sst_seq,
            self.test_hooks.clone(),
        ) {
            Ok(w) => Ok(Box::new(w)),
            Err(e) => {
                tracing::error!(
                    "Failed to create FsDynWriter with seq {} in {}: {}",
                    sst_seq,
                    self.temp_dir.display(),
                    e
                );
                // Try falling back to the OS temp dir as a best-effort recovery for tests
                let sys_tmp = std::env::temp_dir();
                match FsDynWriter::new_with_seq(
                    &sys_tmp,
                    compression,
                    block_size,
                    use_internal,
                    sst_seq,
                    self.test_hooks.clone(),
                ) {
                    Ok(w2) => {
                        tracing::warn!(
                            "FsDynWriter fell back to system temp dir: {}",
                            sys_tmp.display()
                        );
                        Ok(Box::new(w2))
                    }
                    Err(e2) => {
                        tracing::error!(
                            "Failed to create FsDynWriter with seq {} in {} and fallback {}: {}",
                            sst_seq,
                            self.temp_dir.display(),
                            sys_tmp.display(),
                            e2
                        );
                        Err(MidgeError::internal(format!(
                            "Failed to create FsDynWriter with seq {} in {} or fallback {}: {}",
                            sst_seq,
                            self.temp_dir.display(),
                            sys_tmp.display(),
                            e2
                        )))
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
    ) -> crate::error::MidgeResult<Box<dyn crate::sst::DynSstWriter>> {
        // FsDynWriter currently ignores bloom bits and uses default of 10
        self.create(compression, block_size, use_internal)
    }
}

// Previously we allowed constructing an ErrorDynWriter fallback; now factory
// methods return a `MidgeResult` directly, so a separate error writer is
// unnecessary and has been removed.

