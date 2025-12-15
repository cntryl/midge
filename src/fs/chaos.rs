//! Chaos wrapper for an `EngineFs` implementation.
//!
//! This is a very small stub that forwards all calls to an inner
//! `EngineFs`. In future we will add rules for injecting delays, failures
//! and corruptions for testing recovery and robustness.

use super::*;
use bytes::Bytes;

/// A thin wrapper that delegates to an inner `EngineFs` instance.
pub struct ChaosFs<F: EngineFs> {
    inner: F,
    // future: injection rules, RNG seed, latency map, failure rates
}

impl<F: EngineFs> ChaosFs<F> {
    /// Create a new `ChaosFs` wrapping `inner`.
    pub fn new(inner: F) -> Self {
        Self { inner }
    }

    /// Access the inner filesystem for tests.
    pub fn inner(&self) -> &F {
        &self.inner
    }
}

impl<F: EngineFs> EngineFs for ChaosFs<F> {
    fn wal_open(&self, cf: CfId, wal: WalId) -> FsResult<Box<dyn WalWriter>> {
        self.inner.wal_open(cf, wal)
    }

    fn wal_read(&self, cf: CfId, wal: WalId) -> FsResult<Box<dyn WalReader>> {
        self.inner.wal_read(cf, wal)
    }

    fn wal_list(&self, cf: CfId) -> FsResult<Vec<WalId>> {
        self.inner.wal_list(cf)
    }

    fn wal_delete(&self, cf: CfId, wal: WalId) -> FsResult<()> {
        self.inner.wal_delete(cf, wal)
    }

    fn sst_create(&self, cf: CfId, sst: SstId) -> FsResult<Box<dyn SstWriter>> {
        self.inner.sst_create(cf, sst)
    }

    fn sst_open(&self, cf: CfId, sst: SstId) -> FsResult<Box<dyn SstReader>> {
        self.inner.sst_open(cf, sst)
    }

    fn sst_list(&self, cf: CfId) -> FsResult<Vec<SstId>> {
        self.inner.sst_list(cf)
    }

    fn sst_delete(&self, cf: CfId, sst: SstId) -> FsResult<()> {
        self.inner.sst_delete(cf, sst)
    }

    fn manifest_read(&self, cf: CfId) -> FsResult<Bytes> {
        self.inner.manifest_read(cf)
    }

    fn manifest_replace_atomic(
        &self,
        cf: CfId,
        new_contents: Bytes,
        dur: Durability,
    ) -> FsResult<()> {
        self.inner.manifest_replace_atomic(cf, new_contents, dur)
    }

    fn sync_dir_if_supported(&self, cf: CfId) -> FsResult<()> {
        self.inner.sync_dir_if_supported(cf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct DummyFs;

    impl EngineFs for DummyFs {
        fn wal_open(&self, _cf: CfId, _wal: WalId) -> FsResult<Box<dyn WalWriter>> {
            Err(FsError::Unsupported("dummy".into()))
        }
        fn wal_read(&self, _cf: CfId, _wal: WalId) -> FsResult<Box<dyn WalReader>> {
            Err(FsError::Unsupported("dummy".into()))
        }
        fn wal_list(&self, _cf: CfId) -> FsResult<Vec<WalId>> {
            Ok(vec![])
        }
        fn wal_delete(&self, _cf: CfId, _wal: WalId) -> FsResult<()> {
            Ok(())
        }
        fn sst_create(&self, _cf: CfId, _sst: SstId) -> FsResult<Box<dyn SstWriter>> {
            Err(FsError::Unsupported("dummy".into()))
        }
        fn sst_open(&self, _cf: CfId, _sst: SstId) -> FsResult<Box<dyn SstReader>> {
            Err(FsError::Unsupported("dummy".into()))
        }
        fn sst_list(&self, _cf: CfId) -> FsResult<Vec<SstId>> {
            Ok(vec![])
        }
        fn sst_delete(&self, _cf: CfId, _sst: SstId) -> FsResult<()> {
            Ok(())
        }
        fn manifest_read(&self, _cf: CfId) -> FsResult<Bytes> {
            Ok(Bytes::new())
        }
        fn manifest_replace_atomic(
            &self,
            _cf: CfId,
            _new_contents: Bytes,
            _dur: Durability,
        ) -> FsResult<()> {
            Ok(())
        }
        fn sync_dir_if_supported(&self, _cf: CfId) -> FsResult<()> {
            Ok(())
        }
    }

    #[test]
    fn chaos_forwards_calls() {
        let d = DummyFs;
        let chaos = ChaosFs::new(d);
        // wal_list should be forwarded and return empty vector
        let list = chaos.wal_list(CfId(0)).unwrap();
        assert!(list.is_empty());
    }
}
