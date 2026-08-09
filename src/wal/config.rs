/// WAL buffering and batching controls owned by the WAL subsystem.
#[derive(Debug, Clone)]
pub(crate) struct WalBatchingConfig {
    pub(crate) buffer_size: usize,
    pub(crate) batch: Option<super::policy::BatchConfig>,
}

impl WalBatchingConfig {
    pub(crate) fn new(buffer_size: usize, batch: Option<super::policy::BatchConfig>) -> Self {
        Self { buffer_size, batch }
    }
}
