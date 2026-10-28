//! Multipart outputs must not replace committed authority before publication.

use cntryl_midge::{
    CloudProviderConfig, CloudStorageLocation, Engine, MemoryBudget, OpenOptions, TransactionMode,
    WriteOptions,
};
use std::collections::BTreeSet;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

const CHILD_ENV: &str = "MIDGE_STREAMED_CRASH_ROOT";
const DIALECT_ENV: &str = "MIDGE_STREAMED_CRASH_DIALECT";
const CHILD_TEST: &str = "streamed_cloud_compaction_crash::should_abort_child_after_streamed_cloud_output_before_manifest_publication";
const HOOK: &str = "slice7::after_compaction_output_durable_before_manifest_publish";
const SCENARIO: &str = "streamed-cloud-output-before-manifest";
const ROWS: u32 = 192;

fn provider(dialect: &str) -> CloudProviderConfig {
    let bucket = format!("midge-streamed-crash-{dialect}");
    match dialect {
        "s3" => {
            let mut provider = CloudProviderConfig::sqrzl_s3(bucket);
            if let CloudProviderConfig::S3Compatible(config) = &mut provider {
                *config = config.clone().with_checksum_receipts(true);
            }
            provider
        }
        "azure" => CloudProviderConfig::sqrzl_azure(bucket),
        "gcs" => CloudProviderConfig::sqrzl_gcs(bucket),
        "gcs-json" => CloudProviderConfig::sqrzl_gcs_json(bucket),
        _ => panic!("unknown crash fixture dialect"),
    }
}

fn open(root: &Path, dialect: &str) -> cntryl_midge::MidgeResult<Engine> {
    let prefix = format!("crash/{}/", root.file_name().unwrap().to_str().unwrap());
    Engine::open(
        OpenOptions::cloud(
            root.join("cache"),
            CloudStorageLocation::new(provider(dialect), prefix),
        )
        .memory_budget(MemoryBudget::Bytes(256 * 1024 * 1024))
        .background_compaction(false)
        .target_sst_size_for_testing(64 * 1024 * 1024)
        .lease_ttl(Duration::from_secs(18))
        .lease_clock_skew_tolerance(Duration::ZERO)
        .build()?,
    )
}

fn value(row: u32, batch: u32) -> Vec<u8> {
    let mut state = (u64::from(row) << 32) | u64::from(batch + 1);
    (0..64 * 1024)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            u8::try_from(state & 255).unwrap()
        })
        .collect()
}

fn names(engine: &Engine) -> BTreeSet<String> {
    engine
        .metrics()
        .get_storage_layout()
        .unwrap()
        .levels
        .into_iter()
        .flat_map(|level| level.files.into_iter().map(|file| file.name))
        .collect()
}

fn seed(engine: &Engine) {
    let cf = engine.get_column_family("default").unwrap();
    for batch in 0..2 {
        for first in (0..ROWS).step_by(16) {
            let mut write = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .unwrap();
            for row in first..first + 16 {
                write
                    .put(row.to_be_bytes().to_vec(), value(row, batch), None)
                    .unwrap();
            }
            write.commit(WriteOptions::cloud_strict()).unwrap();
        }
        engine.flush_cf(&cf).unwrap();
    }
}

fn crash_child(root: &Path, dialect: &str) {
    let engine = open(root, dialect).unwrap();
    seed(&engine);
    let inputs = names(&engine);
    std::fs::write(
        root.join("inputs.json"),
        serde_json::to_vec(&inputs).unwrap(),
    )
    .unwrap();
    let directory = root.join("cache");
    fail::cfg_callback(HOOK, move || {
        use cntryl_midge::__internal::io::{Fs, RealFs};
        use cntryl_midge::__internal::recovery::{IntentLogEntry, IntentPersistence};
        let fs: Arc<dyn Fs> = Arc::new(RealFs::new(&directory).unwrap());
        let intents =
            IntentPersistence::load_with_fs_and_policy(&fs, cntryl_midge::RecoveryPolicy::Strict)
                .unwrap();
        // Cloud publication may evict the local output. Its durable intent
        // retains the finalized physical identity and byte length.
        let outputs: Vec<_> = intents
            .into_iter()
            .filter_map(|entry| match entry {
                IntentLogEntry::CompactionPublish { added, .. } => Some(added),
                _ => None,
            })
            .flatten()
            .filter(|file| !inputs.contains(&file.name))
            .map(|file| file.size_bytes)
            .collect();
        // A metadata-only move can precede the physical merge. It has no
        // new upload and therefore is not the selected crash boundary.
        if outputs.is_empty() {
            return;
        }
        assert!(
            outputs.iter().any(|size| *size > 10 * 1024 * 1024),
            "the durable output must require multiple streamed parts: {outputs:?}"
        );
        crate::crash::abort_at_trigger(SCENARIO, HOOK);
    })
    .unwrap();
    engine.compact_all().unwrap();
    panic!("expected exact publication-boundary abort");
}

#[test]
fn should_abort_child_after_streamed_cloud_output_before_manifest_publication() {
    // Arrange: the parent alone supplies this child invocation's fixture.
    let Some(root) = std::env::var_os(CHILD_ENV) else {
        return;
    };
    let dialect = std::env::var(DIALECT_ENV).unwrap();
    let _scenario = fail::FailScenario::setup();
    // Act
    crash_child(Path::new(&root), &dialect);
    // Assert: the child must terminate at the independently recorded trigger.
}

#[test]
fn should_recover_committed_inputs_when_streamed_cloud_output_aborts_before_manifest() {
    // Arrange: each provider uses an isolated namespace and real child process.
    for dialect in ["s3", "azure", "gcs", "gcs-json"] {
        super::require_sqrzl("streamed-compaction-crash");
        super::ensure_sqrzl_namespace(&provider(dialect)).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let mut child = Command::new(std::env::current_exe().unwrap());
        child
            .args(["--exact", CHILD_TEST, "--nocapture"])
            .env(CHILD_ENV, root)
            .env(DIALECT_ENV, dialect);
        // Act: abort after remote output durability but before manifest authority.
        crate::crash::run_child_expect_abort(&mut child, SCENARIO, HOOK, &root.join("cache"));
        let inputs: BTreeSet<String> =
            serde_json::from_slice(&std::fs::read(root.join("inputs.json")).unwrap()).unwrap();
        std::fs::remove_dir_all(root.join("cache")).unwrap();
        std::thread::sleep(Duration::from_secs(19));
        let mut recovered = open(root, dialect).unwrap();
        // Assert: only committed inputs supply all latest values after cache loss.
        assert_eq!(
            names(&recovered),
            inputs,
            "{dialect}: unpublished output gained authority"
        );
        let cf = recovered.get_column_family("default").unwrap();
        let read = recovered
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .unwrap();
        for row in 0..ROWS {
            assert_eq!(
                read.get(&row.to_be_bytes()).unwrap().as_deref(),
                Some(value(row, 1).as_slice()),
                "{dialect}: row {row}"
            );
        }
        drop(read);
        recovered.shutdown(Duration::from_secs(10)).unwrap();
    }
}
