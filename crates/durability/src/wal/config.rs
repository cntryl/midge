/// WAL buffering and batching controls owned by the WAL subsystem.
#[derive(Debug, Clone)]
pub struct WalBatchingConfig {
    pub buffer_size: usize,
    pub batch: Option<super::policy::BatchConfig>,
}

impl WalBatchingConfig {
    pub fn new(buffer_size: usize, batch: Option<super::policy::BatchConfig>) -> Self {
        Self { buffer_size, batch }
    }
}
