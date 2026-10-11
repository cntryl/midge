use std::sync::Arc;

use super::super::{RuntimeConfig, RuntimeState};
use crate::runtime::read_resources::ReadResources;

/// SST dependencies assembled before the event loop starts dispatching.
pub(super) struct SstRuntimeResources {
    pub(super) factory: Arc<dyn crate::sst::SstFactory>,
    pub(super) reads: Option<Arc<ReadResources>>,
}

/// SSTs are read through the database root, or through the cloud read view
/// layered over it. The compaction factory is rooted at its `sst/` directory,
/// so compaction names inputs, outputs and scratch relative to it.
pub(super) fn assemble_sst_resources(
    state: &RuntimeState,
    memory_mode: bool,
    config: &RuntimeConfig,
) -> SstRuntimeResources {
    let fs = crate::io::subdirectory(
        config
            .sst_read_fs
            .clone()
            .unwrap_or_else(|| Arc::clone(&state.fs)),
        crate::sst::fs::SST_DIR,
    );
    let factory: Arc<dyn crate::sst::SstFactory> = if memory_mode {
        Arc::new(
            crate::sst::FsSstFactoryIo::new(fs, 64 * 1024)
                .with_compression_policy(config.compression_policy.clone()),
        )
    } else {
        let fs = fs
            .with_read_observer(
                Arc::clone(&state.diagnostics) as Arc<dyn crate::io::traits::ReadObserver>
            )
            .unwrap_or(fs);
        Arc::new(
            crate::sst::FsSstFactoryIo::new(fs, 64 * 1024)
                .with_compaction_scratch_directory(std::path::PathBuf::from(
                    crate::sst::fs::STAGING_DIR_NAME,
                ))
                .with_compression_policy(config.compression_policy.clone()),
        )
    };
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
