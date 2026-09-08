use std::sync::Arc;

use super::super::{RuntimeConfig, RuntimeState};
use crate::runtime::read_resources::ReadResources;

/// SST dependencies assembled before the event loop starts dispatching.
pub(super) struct SstRuntimeResources {
    pub(super) factory: Arc<dyn crate::sst::SstFactory>,
    pub(super) reads: Option<Arc<ReadResources>>,
}

pub(super) fn assemble_sst_resources(
    state: &RuntimeState,
    sst_dir: &std::path::Path,
    memory_mode: bool,
    config: &RuntimeConfig,
) -> crate::common::MidgeResult<SstRuntimeResources> {
    let factory: Arc<dyn crate::sst::SstFactory> = if memory_mode {
        let fs = Arc::new(crate::io::MockFs::new());
        Arc::new(
            crate::sst::FsSstFactoryIo::new(fs, 64 * 1024)
                .with_compression_policy(config.compression_policy.clone()),
        )
    } else {
        let fs: Arc<dyn crate::io::Fs> = match &config.sst_read_fs {
            Some(fs) => Arc::clone(fs),
            None => Arc::new(crate::io::RealFs::new(sst_dir)?),
        };
        let fs = fs
            .with_read_observer(
                Arc::clone(&state.diagnostics) as Arc<dyn crate::io::traits::ReadObserver>
            )
            .unwrap_or(fs);
        Arc::new(
            crate::sst::FsSstFactoryIo::new(fs, 64 * 1024)
                .with_compaction_scratch_directory(sst_dir.join(".flush-staging"))
                .with_compression_policy(config.compression_policy.clone()),
        )
    };
    let reads = if memory_mode {
        None
    } else {
        let sst_path_prefix = sst_dir
            .strip_prefix(&state.db_path)
            .unwrap_or_else(|_| std::path::Path::new("sst"))
            .to_path_buf();
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
    Ok(SstRuntimeResources { factory, reads })
}
