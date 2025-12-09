//! Metrics and observability
//!
//! Performance monitoring and statistics collection

/// Performance metrics
#[derive(Default, Clone, Debug)]
pub struct PerformanceMetrics {
    pub read_ops: u64,
    pub write_ops: u64,
    pub delete_ops: u64,
    pub compactions: u64,
}

impl PerformanceMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_read(&mut self) {
        self.read_ops += 1;
    }

    pub fn record_write(&mut self) {
        self.write_ops += 1;
    }

    pub fn record_delete(&mut self) {
        self.delete_ops += 1;
    }

    pub fn record_compaction(&mut self) {
        self.compactions += 1;
    }
}
