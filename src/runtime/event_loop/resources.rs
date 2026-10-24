use std::sync::Arc;

use super::super::{RuntimeConfig, RuntimeState};
use crate::runtime::read_resources::ReadResources;

/// SST dependencies assembled before the event loop starts dispatching.
pub(super) struct SstRuntimeResources {
    pub(super) factory: Arc<dyn crate::sst::SstFactory>,
    pub(super) reads: Option<Arc<ReadResources>>,
}

/// SSTs are read through the database root, or through the cloud read view
/// layered over it.
pub(super) fn assemble_sst_resources(
    state: &RuntimeState,
    memory_mode: bool,
    config: &RuntimeConfig,
) -> SstRuntimeResources {
    let fs = config
        .sst_read_fs
        .clone()
        .unwrap_or_else(|| Arc::clone(&state.fs));
    let fs = if memory_mode {
        fs
    } else {
        fs.with_read_observer(
            Arc::clone(&state.diagnostics) as Arc<dyn crate::io::traits::ReadObserver>
        )
        .unwrap_or(fs)
    };
    let factory: Arc<dyn crate::sst::SstFactory> = Arc::new(
        compaction_sst_factory(fs, config.compression_policy.clone())
            .with_write_limiter(state.write_pressure.admission.limiter()),
    );
    let reads = if memory_mode {
        None
    } else {
        let sst_path_prefix = std::path::PathBuf::from(crate::sst::fs::SST_DIR);
        Some(Arc::new(ReadResources::new_with_diagnostics(
            config
                .sst_read_fs
                .clone()
                .unwrap_or_else(|| Arc::clone(&state.fs)),
            sst_path_prefix,
            config.block_cache_size,
            config.block_cache_policy,
            Arc::clone(&state.diagnostics),
        )))
    };
    SstRuntimeResources { factory, reads }
}

/// The compaction SST factory over `fs`, the database root or the cloud read
/// view layered over it. The factory is rooted at its `sst/` directory, so
/// compaction names inputs, outputs and build scratch relative to it.
pub(super) fn compaction_sst_factory(
    fs: Arc<dyn crate::io::Fs>,
    compression_policy: crate::codec::CompressionPolicy,
) -> crate::sst::FsSstFactoryIo {
    crate::sst::FsSstFactoryIo::new(
        crate::io::subdirectory(fs, crate::sst::fs::SST_DIR),
        64 * 1024,
    )
    .with_compaction_scratch_directory(std::path::PathBuf::from(crate::sst::fs::STAGING_DIR_NAME))
    .with_compression_policy(compression_policy)
}
