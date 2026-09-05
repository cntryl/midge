//! Resource-ratio qualification through the native S3 provider, in child processes.

#[path = "operational/fixture.rs"]
mod fixture;
#[path = "operational/observe.rs"]
mod observe;
#[path = "operational/workload.rs"]
mod workload;

use cntryl_midge::{Engine, Query, TransactionMode, WriteOptions};
use fixture::{Campaign, Profile};
use observe::Observation;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const CHILD_ENV: &str = "MIDGE_OPERATIONAL_CHILD_CONFIG";
const PHASE_ENV: &str = "MIDGE_OPERATIONAL_CHILD_PHASE";
const CHILD_TEST: &str = "operational::should_execute_cloud_recovery_phase_in_child";

#[test]
#[ignore = "requires Sqrzl; Cloud Qualification runs this configurable recovery campaign"]
fn should_recover_cloud_backlog_after_complete_local_disk_loss() {
    // Arrange
    super::require_sqrzl("operational-recovery");
    for profile in Profile::selected() {
        let directory = tempfile::tempdir().expect("campaign directory");
        let campaign = Campaign::prepare(directory.path(), profile);
        let config = directory.path().join("campaign.json");
        std::fs::write(
            &config,
            serde_json::to_vec_pretty(&campaign).expect("config JSON"),
        )
        .expect("write child config");

        // Act: remove every local file, interrupt a durable checkpoint, then
        // remove every local file again before a fresh-process recovery.
        std::fs::remove_dir_all(&campaign.cache).expect("lose entire initial disk");
        run_child(&campaign, &config, "interrupted", 73);
        assert!(campaign.artifacts.join("checkpoint-reached").exists());
        std::fs::remove_dir_all(&campaign.cache).expect("lose interrupted recovery disk");
        run_child(&campaign, &config, "recovered", 0);
        std::fs::remove_dir_all(&campaign.cache).expect("lose recovered working disk");
        run_child(&campaign, &config, "verified", 0);

        // Assert
        for phase in ["interrupted", "recovered", "verified"] {
            let report = campaign.artifacts.join(format!("{phase}.json"));
            let bytes = std::fs::read(&report).expect("phase report");
            let report: serde_json::Value = serde_json::from_slice(&bytes).expect("report JSON");
            assert!(
                report["peak_local_file_bytes"].as_u64().expect("peak")
                    <= campaign.profile.local_bytes
            );
            println!("MIDGE_OPERATIONAL_EVIDENCE {report}");
        }
    }
}

#[test]
#[ignore = "child entry point for native cloud recovery qualification"]
fn should_execute_cloud_recovery_phase_in_child() {
    // Arrange
    let Some(config) = std::env::var_os(CHILD_ENV) else {
        return;
    };
    let campaign: Campaign = serde_json::from_slice(&std::fs::read(config).expect("child config"))
        .expect("decode campaign");
    let phase = std::env::var(PHASE_ENV).expect("phase");
    cntryl_midge::init_benchmark_telemetry().expect("telemetry before open");
    let observation = Observation::start(&campaign, &phase);
    let scenario = fail::FailScenario::setup();
    observation.install_publication_probes(&campaign, &phase);

    // Act
    let started = Instant::now();
    let mut engine = open_after_expired_owner(&campaign);
    let recovery_ms = started.elapsed().as_millis();
    eprintln!("MIDGE_OPERATIONAL_PHASE {phase} opened in {recovery_ms} ms");
    observation.record_opened(&campaign, &phase, recovery_ms);
    assert_complete_state(&engine, &campaign, phase != "recovered");
    if phase == "recovered" {
        exercise_accepted_writes(&engine, &campaign);
    } else {
        assert_accepted_writes(&engine);
    }
    let metrics = engine.get_runtime_metrics().expect("runtime metrics");
    engine
        .shutdown(Duration::from_secs(60))
        .expect("shutdown qualified engine");

    // Assert
    observation.finish(&campaign, &phase, recovery_ms, Some(&metrics));
    scenario.teardown();
}

fn open_after_expired_owner(campaign: &Campaign) -> Engine {
    let deadline = Instant::now() + Duration::from_secs(campaign.profile.timeout_seconds);
    let mut reported_lease = false;
    loop {
        match Engine::open(campaign.options()) {
            Ok(engine) => return engine,
            Err(cntryl_midge::MidgeError::LeaseHeld(message)) if Instant::now() < deadline => {
                if !reported_lease {
                    eprintln!("waiting for the crashed owner's lease: {message}");
                    reported_lease = true;
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(error) => panic!("native cloud recovery failed: {error}"),
        }
    }
}

fn assert_complete_state(engine: &Engine, campaign: &Campaign, has_workload: bool) {
    let cf = super::default_cf(engine);
    let tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("verification transaction");
    for index in 0..campaign.records {
        assert_eq!(
            tx.get(&fixture::key(index))
                .expect("recovered key")
                .as_deref(),
            Some(fixture::value(index, campaign.profile.value_bytes).as_slice()),
            "recovered value {index}"
        );
        if (index + 1).is_multiple_of(16_384) || index + 1 == campaign.records {
            eprintln!("MIDGE_OPERATIONAL_VERIFIED {} point-read values", index + 1);
        }
    }
    assert_eq!(
        tx.get(b"qualification-seed").expect("seed"),
        Some(b"acknowledged".as_slice().into())
    );
    let mut auxiliary_keys = vec![b"qualification-seed".to_vec()];
    if has_workload {
        auxiliary_keys.extend(workload::expected_keys());
        auxiliary_keys.push(b"qualification-after-recovery".to_vec());
    }
    auxiliary_keys.sort();
    let mut expected = (0..campaign.records)
        .map(fixture::key)
        .chain(auxiliary_keys);
    for entry in tx.scan(&Query::new()).expect("complete keyset scan") {
        let (actual, _) = entry.expect("complete keyset item");
        assert_eq!(
            Some(actual.as_ref()),
            expected.next().as_deref(),
            "unexpected, duplicated, or reordered recovered key"
        );
    }
    assert!(expected.next().is_none(), "missing recovered key");
    eprintln!("MIDGE_OPERATIONAL_VERIFIED complete recovered keyset");
    assert!(
        !campaign.cache.join("cloud_recovery/wal").exists(),
        "no complete WAL staging"
    );
    assert!(
        engine
            .get_runtime_metrics()
            .expect("recovered inventory")
            .sst_bytes
            > campaign.profile.local_bytes,
        "recovered remote SST inventory must exceed the local working disk"
    );
}

fn exercise_accepted_writes(engine: &Engine, campaign: &Campaign) {
    workload::exercise(engine, campaign);
    let cf = super::default_cf(engine);
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("post-recovery write");
    tx.put(
        b"qualification-after-recovery".to_vec(),
        b"durable".to_vec(),
        None,
    )
    .expect("put");
    tx.commit(WriteOptions::cloud_strict())
        .expect("accepted cloud write");
    engine.flush_cf(&cf).expect("flush accepted write");
    // Maintenance must also operate with inventory larger than the working disk.
    engine.compact_all().expect("bounded cloud compaction");
    assert_complete_state(engine, campaign, true);
    assert_accepted_writes(engine);
}

fn assert_accepted_writes(engine: &Engine) {
    let cf = super::default_cf(engine);
    let tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("read accepted write");
    assert_eq!(
        tx.get(b"qualification-after-recovery")
            .expect("accepted key"),
        Some(b"durable".as_slice().into())
    );
    workload::verify(engine);
}

fn run_child(campaign: &Campaign, config: &Path, phase: &str, expected_code: i32) {
    let log = campaign.artifacts.join(format!("{phase}.log"));
    let output = std::fs::File::create(&log).expect("child log");
    let mut child = Command::new(std::env::current_exe().expect("test binary"))
        .args(["--exact", CHILD_TEST, "--ignored", "--nocapture"])
        .env(CHILD_ENV, config)
        .env(PHASE_ENV, phase)
        .stdout(Stdio::from(output.try_clone().expect("clone log")))
        .stderr(Stdio::from(output))
        .spawn()
        .expect("spawn qualification child");
    let deadline = Instant::now() + Duration::from_secs(campaign.profile.timeout_seconds);
    let status = loop {
        if let Some(status) = child.try_wait().expect("poll qualification child") {
            break status;
        }
        if Instant::now() >= deadline {
            child.kill().expect("kill timed-out qualification child");
            let _ = child.wait();
            panic!(
                "{phase} exceeded its campaign deadline; log: {}",
                log.display()
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    assert_eq!(
        status.code(),
        Some(expected_code),
        "{phase}: {}",
        std::fs::read_to_string(log).expect("child output")
    );
}
