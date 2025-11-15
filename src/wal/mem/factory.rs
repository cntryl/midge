use super::shared::NoOpWal;

/// In-memory WAL factory
pub struct MemWalFactory;

impl crate::wal::WalFactory for MemWalFactory {
    fn create_writer(
        &self,
        _dir: &std::path::Path,
    ) -> crate::error::MidgeResult<Box<dyn crate::wal::WalWriter>> {
        // For in-memory mode, use NoOpWal since durability is impossible anyway
        Ok(Box::new(NoOpWal::new()))
    }

    fn create_reader(
        &self,
        _dir: &std::path::Path,
    ) -> crate::error::MidgeResult<Box<dyn crate::wal::WalReaderDyn>> {
        // For in-memory mode, use NoOpWal since no records were persisted
        Ok(Box::new(NoOpWal::new()))
    }

    fn rotate_writer(
        &self,
        _dir: &std::path::Path,
        _seq: u64,
    ) -> crate::error::MidgeResult<Box<dyn crate::wal::WalWriter>> {
        // For in-memory WAL, rotation is a no-op: just create a new no-op writer
        Ok(Box::new(NoOpWal::new()))
    }
}
