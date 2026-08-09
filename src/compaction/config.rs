use crate::sst::compression::CompressionPolicy;

/// Engine-open compaction policy owned by the compaction subsystem.
#[derive(Debug, Clone)]
pub(crate) struct OpenCompactionConfig {
    pub(crate) target_sst_size: usize,
    pub(crate) l0_trigger: usize,
    pub(crate) background_enabled: bool,
    pub(crate) compression: CompressionPolicy,
}

impl OpenCompactionConfig {
    pub(crate) fn new(
        target_sst_size: usize,
        l0_trigger: usize,
        background_enabled: bool,
        compression: CompressionPolicy,
    ) -> Self {
        Self {
            target_sst_size,
            l0_trigger,
            background_enabled,
            compression,
        }
    }

    pub(crate) fn set_background_enabled(&mut self, enabled: bool) {
        self.background_enabled = enabled;
    }
}
