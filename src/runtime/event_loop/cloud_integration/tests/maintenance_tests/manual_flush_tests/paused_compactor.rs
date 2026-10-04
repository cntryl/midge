use super::*;
use crate::common::resource_budget::ResourceBudget;
use crate::sst::traits::{DynSstWriter, SstReaderExt};
use crate::sst::SstFactory;

struct FinalizeGate {
    reached: crossbeam::channel::Sender<()>,
    release: crossbeam::channel::Receiver<()>,
}

pub(super) struct ComputeRelease(Option<crossbeam::channel::Sender<()>>);

impl ComputeRelease {
    pub(super) fn release(&mut self) {
        if let Some(release) = self.0.take() {
            let _ = release.send(());
        }
    }
}

impl Drop for ComputeRelease {
    fn drop(&mut self) {
        self.release();
    }
}

pub(super) fn install(
    el: &mut EventLoop,
) -> MidgeResult<(ComputeRelease, crossbeam::channel::Receiver<()>)> {
    let reads = Arc::new(crate::storage::remote_sst::RemoteSstFs::new(
        Arc::new(crate::io::RealFs::new(&el.state.db_path).map_err(FsError::into_midge)?),
        el.cloud_coordinator
            .hybrid_storage
            .as_ref()
            .expect("cloud storage")
            .remote_sst_backend(),
        Duration::from_secs(3),
    ));
    let delegate = Arc::new(
        crate::sst::FsSstFactoryIo::new(reads, 64 * 1024)
            .with_compaction_scratch_directory(el.state.sst_dir.join(".flush-staging")),
    );
    let (reached, arrived) = crossbeam::channel::bounded(1);
    let (release, resumed) = crossbeam::channel::bounded(1);
    el.compaction_actor = crate::runtime::actors::CompactionActor::new(Arc::new(PausedFactory {
        delegate,
        gate: Arc::new(Mutex::new(Some(FinalizeGate {
            reached,
            release: resumed,
        }))),
    }));
    Ok((ComputeRelease(Some(release)), arrived))
}

struct PausedFactory {
    delegate: Arc<dyn SstFactory>,
    gate: Arc<Mutex<Option<FinalizeGate>>>,
}

impl PausedFactory {
    fn wrap(&self, inner: Box<dyn DynSstWriter>) -> Box<dyn DynSstWriter> {
        Box::new(PausedWriter {
            inner,
            gate: Arc::clone(&self.gate),
        })
    }
}

impl SstFactory for PausedFactory {
    fn output_fs(&self) -> Arc<dyn crate::io::Fs> {
        self.delegate.output_fs()
    }

    fn compaction_scratch_cleanup_verified(&self) -> bool {
        self.delegate.compaction_scratch_cleanup_verified()
    }

    fn create(&self) -> MidgeResult<Box<dyn DynSstWriter>> {
        Ok(self.wrap(self.delegate.create()?))
    }

    fn create_for_compaction(&self, budget: ResourceBudget) -> MidgeResult<Box<dyn DynSstWriter>> {
        Ok(self.wrap(self.delegate.create_for_compaction(budget)?))
    }

    fn open_for_compaction(
        &self,
        path: &Path,
        budget: ResourceBudget,
    ) -> MidgeResult<Box<dyn SstReaderExt>> {
        self.delegate.open_for_compaction(path, budget)
    }

    fn open(&self, path: &Path) -> MidgeResult<Box<dyn SstReaderExt>> {
        self.delegate.open(path)
    }
}

struct PausedWriter {
    inner: Box<dyn DynSstWriter>,
    gate: Arc<Mutex<Option<FinalizeGate>>>,
}

fn pause_once(gate: &Mutex<Option<FinalizeGate>>) -> MidgeResult<()> {
    let gate = gate
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take();
    if let Some(gate) = gate {
        gate.reached.send(()).map_err(|error| {
            MidgeError::Internal(format!("report finalized compaction: {error}"))
        })?;
        gate.release.recv().map_err(|error| {
            MidgeError::Internal(format!("resume finalized compaction: {error}"))
        })?;
    }
    Ok(())
}

impl DynSstWriter for PausedWriter {
    fn estimated_size_bytes(&self) -> usize {
        self.inner.estimated_size_bytes()
    }

    fn encoded_size_upper_bound(&self) -> Option<usize> {
        self.inner.encoded_size_upper_bound()
    }

    fn encoded_size_upper_bound_after_sorted_entry(
        &self,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> Option<usize> {
        self.inner
            .encoded_size_upper_bound_after_sorted_entry(key, value)
    }

    fn additional_range_tombstone_size_upper_bound(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Option<usize> {
        self.inner
            .additional_range_tombstone_size_upper_bound(start, end)
    }

    fn add_with_meta(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        seq: u64,
        op_type: EntryType,
        expiration: Option<u64>,
    ) -> MidgeResult<()> {
        self.inner
            .add_with_meta(key, value, seq, op_type, expiration)
    }

    fn add_sorted_with_meta(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        seq: u64,
        op_type: EntryType,
        expiration: Option<u64>,
    ) -> MidgeResult<()> {
        self.inner
            .add_sorted_with_meta(key, value, seq, op_type, expiration)
    }

    fn add_range_tombstone(&mut self, start: &[u8], end: &[u8], seq: u64) -> MidgeResult<()> {
        self.inner.add_range_tombstone(start, end, seq)
    }

    fn finish_to_path(self: Box<Self>, path: &Path) -> MidgeResult<()> {
        self.inner.finish_to_path(path)?;
        pause_once(&self.gate)
    }

    fn finish_bytes(self: Box<Self>) -> MidgeResult<Vec<u8>> {
        let bytes = self.inner.finish_bytes()?;
        pause_once(&self.gate)?;
        Ok(bytes)
    }
}
