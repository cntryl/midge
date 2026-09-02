use crate::sst::compression::CompressionPolicy;

pub(crate) const DEFAULT_TARGET_SST_SIZE: usize = 256 * 1024 * 1024;
pub(crate) const DEFAULT_COMPACTION_MEMORY_LIMIT: usize = 64 * 1024 * 1024;

/// Engine-open compaction policy owned by the compaction subsystem.
#[derive(Debug, Clone)]
pub(crate) struct OpenCompactionConfig {
    pub(crate) target_sst_size: usize,
    pub(crate) memory_pool_size: usize,
    pub(crate) l0_trigger: usize,
    pub(crate) background_enabled: bool,
    pub(crate) compression: CompressionPolicy,
}

impl OpenCompactionConfig {
    pub(crate) fn new(
        target_sst_size: usize,
        memory_pool_size: usize,
        l0_trigger: usize,
        background_enabled: bool,
        compression: CompressionPolicy,
    ) -> Self {
        Self {
            target_sst_size,
            memory_pool_size,
            l0_trigger,
            background_enabled,
            compression,
        }
    }

    pub(crate) fn set_background_enabled(&mut self, enabled: bool) {
        self.background_enabled = enabled;
    }
}
