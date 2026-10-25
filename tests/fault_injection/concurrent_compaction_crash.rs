//! Process-abort recovery while two independent compaction jobs are active.

use cntryl_midge::{
    Engine, EngineHealth, MidgeResult, OpenOptions, Query, TransactionMode, WriteOptions,
};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

const CHILD_ENV: &str = "MIDGE_CONCURRENT_COMPACTION_CRASH_DB";
const CHILD_TEST: &str =
    "concurrent_compaction_crash::should_abort_child_when_two_compactions_are_active";
const SCENARIO: &str = "two_compactions_before_manifest_publish";
const PUBLICATION_HOOK: &str = "slice7::after_compaction_output_durable_before_manifest_publish";
const BATCHES: usize = 4;
const ROWS_PER_BATCH: usize = 16;

fn open_engine(path: &Path) -> MidgeResult<Engine> {
    Engine::open(
        OpenOptions::local(path)
            .background_compaction(false)
            .compaction_concurrency(2)
            .build()?,
    )
}

fn key(prefix: &str, batch: usize, row: usize) -> Vec<u8> {
    format!("{prefix}-{batch:02}-{row:02}").into_bytes()
}

fn value(prefix: &str, batch: usize, row: usize) -> Vec<u8> {
    format!("{prefix}-value-{batch:02}-{row:02}").into_bytes()
}

fn seed_family(
    engine: &Engine,
    family: &cntryl_midge::ColumnFamilyHandle,
    prefix: &str,
) -> MidgeResult<()> {
    for batch in 0..BATCHES {
        let mut transaction = engine.begin_tx(family.id(), TransactionMode::ReadWrite)?;
        for row in 0..ROWS_PER_BATCH {
            transaction.put(key(prefix, batch, row), value(prefix, batch, row), None)?;
        }
        transaction.commit(WriteOptions::sync())?;
        engine.flush_cf(family)?;
    }
    Ok(())
}

fn assert_family_rows(
    engine: &Engine,
    family: &cntryl_midge::ColumnFamilyHandle,
    prefix: &str,
) -> MidgeResult<()> {
    let transaction = engine.begin_tx(family.id(), TransactionMode::ReadOnly)?;
    let rows: Vec<_> = transaction.scan(&Query::new())?.try_collect()?;
    assert_eq!(rows.len(), BATCHES * ROWS_PER_BATCH);
    for batch in 0..BATCHES {
        for row in 0..ROWS_PER_BATCH {
            assert_eq!(
                transaction.get(&key(prefix, batch, row))?,
                Some(bytes::Bytes::from(value(prefix, batch, row)))
            );
        }
    }
    Ok(())
}

fn manifest_sst_names(engine: &Engine) -> MidgeResult<BTreeSet<String>> {
    Ok(engine
        .metrics()
        .get_storage_layout()?
        .levels
        .iter()
        .flat_map(|level| &level.files)
        .map(|file| file.name.clone())
        .collect())
}

fn sst_names_on_disk(path: &Path) -> BTreeSet<String> {
    fs::read_dir(path.join("sst"))
        .expect("read SST directory")
        .map(|entry| entry.expect("read SST entry"))
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "sst"))
        .map(|entry| entry.file_name().into_string().expect("UTF-8 SST name"))
        .collect()
}

fn expire_crashed_process_lease(path: &Path) {
    let lease = path.join(".midge_leader");
    if !lease.exists() {
        return;
    }
    let contents = fs::read_to_string(&lease).expect("read crashed process lease");
    let body = contents
        .lines()
        .filter(|line| !line.starts_with("checksum: "))
        .map(|line| {
            if line.starts_with("acquired_at: ") {
                "acquired_at: 1970-01-01T00:00:00Z"
            } else {
                line
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    let checksum = crc32c::crc32c(body.as_bytes());
    fs::write(lease, format!("{body}checksum: {checksum}\n"))
        .expect("expire crashed process lease");
}

#[test]
fn should_recover_both_families_when_two_compactions_abort_before_manifest_publish(
) -> MidgeResult<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path();
    let mut engine = open_engine(path)?;
    let first = engine.create_column_family("first")?;
    let second = engine.create_column_family("second")?;
    seed_family(&engine, &first, "first")?;
    seed_family(&engine, &second, "second")?;

    let before = engine.metrics().get_storage_layout()?;
    let mut files_per_family = BTreeMap::new();
    for file in before.levels.iter().flat_map(|level| &level.files) {
        if file.level == 0 {
            *files_per_family.entry(file.cf_id).or_insert(0_usize) += 1;
        }
    }
    assert_eq!(files_per_family.get(&first.id()), Some(&BATCHES));
    assert_eq!(files_per_family.get(&second.id()), Some(&BATCHES));
    engine.shutdown(Duration::from_secs(10))?;
    drop(engine);

    let mut child = Command::new(std::env::current_exe()?);
    child
        .args(["--exact", CHILD_TEST, "--nocapture", "--test-threads=1"])
        .env(CHILD_ENV, path);
    crate::common::crash::run_child_expect_abort(&mut child, SCENARIO, PUBLICATION_HOOK, path);
    expire_crashed_process_lease(path);

    let mut reopened = open_engine(path)?;
    let recovered_first = reopened.get_column_family("first").unwrap();
    let recovered_second = reopened.get_column_family("second").unwrap();
    assert_family_rows(&reopened, &recovered_first, "first")?;
    assert_family_rows(&reopened, &recovered_second, "second")?;
    assert_eq!(
        reopened.metrics().get_storage_layout()?.health,
        EngineHealth::Healthy
    );
    let manifest_ssts = manifest_sst_names(&reopened)?;
    assert_eq!(manifest_ssts, sst_names_on_disk(path));
    reopened.shutdown(Duration::from_secs(10))?;
    Ok(())
}

#[test]
fn should_abort_child_when_two_compactions_are_active() {
    let Some(path) = std::env::var_os(CHILD_ENV) else {
        return;
    };
    let engine = open_engine(Path::new(&path)).expect("open concurrent compaction fixture");
    let metrics = engine.metrics();
    let fail_scenario = fail::FailScenario::setup();
    fail::cfg_callback(PUBLICATION_HOOK, move || {
        let active = metrics
            .get_runtime_metrics_with_timeout(Duration::from_secs(2))
            .expect("read active jobs at publication crash boundary")
            .active_compactions;
        assert_eq!(
            active, 2,
            "both compaction jobs must be active at the crash point"
        );
        crate::common::crash::abort_at_trigger(SCENARIO, PUBLICATION_HOOK);
    })
    .expect("configure crash at durable-output boundary");
    std::mem::forget(fail_scenario);

    engine
        .compact_all()
        .expect("compaction should abort at configured publication boundary");
    panic!("named concurrent-compaction crash boundary was not reached");
}
