use crate::{Engine, OpenOptions, TransactionMode};

fn value(sequence: u64) -> Vec<u8> {
    let mut random = sequence.wrapping_mul(0x9e37_79b9);
    (0..2048)
        .map(|_| {
            random ^= random << 13;
            random ^= random >> 17;
            random ^= random << 5;
            random.to_le_bytes()[0]
        })
        .collect()
}

fn options(path: &std::path::Path, budget: u64) -> OpenOptions {
    OpenOptions::cloud_simulated(path, "bucket", "streaming-recovery")
        .local_storage_budget(budget)
        .with_memtable_size_limit(64 * 1024)
        .background_compaction(false)
        .build()
        .expect("options")
}

fn publish_wal(path: &std::path::Path, records: u64) -> Vec<u8> {
    publish_records(path, |epoch| {
        (1..=records).map(|seq| record(seq, epoch)).collect()
    })
}

fn record(sequence: u64, epoch: u64) -> crate::wal::WalRecord {
    crate::wal::WalRecord::new(
        crate::wal::WalOpKind::Put,
        bytes::Bytes::from(sequence.to_be_bytes().to_vec()),
        Some(bytes::Bytes::from(value(sequence))),
        sequence,
        epoch,
    )
}

fn publish_records(
    path: &std::path::Path,
    make_records: impl FnOnce(u64) -> Vec<crate::wal::WalRecord>,
) -> Vec<u8> {
    let catalog_path = path.join("cloud_store/wal/publication-catalog.v1.json");
    let mut catalog = crate::wal::cloud_catalog::WalPublicationCatalog::decode(
        &std::fs::read(&catalog_path).expect("read catalog"),
    )
    .expect("decode catalog");
    let records = make_records(catalog.fencing_epoch);
    let maximum = records
        .iter()
        .map(|record| record.seq)
        .max()
        .expect("records");
    let mut bytes = Vec::new();
    for record in &records {
        let payload = crate::wal::encoding::encode(record).expect("encode record");
        crate::wal::frame::append_frame(&mut bytes, &payload).expect("frame record");
    }
    let publication = crate::wal::cloud_catalog::PublishedWalSegment::from_validated_bytes(
        1,
        maximum,
        catalog.fencing_epoch,
        &bytes,
    );
    let object = path.join("cloud_store").join(&publication.object_key);
    std::fs::create_dir_all(object.parent().expect("parent")).expect("object directory");
    std::fs::write(object, &bytes).expect("publish WAL");
    catalog.segments.insert(1, publication);
    let encoded = catalog.encode().expect("encode catalog");
    std::fs::write(catalog_path, &encoded).expect("publish catalog");
    std::fs::write(
        path.join("cloud_store/wal/publication-catalog.v1.mirror.json"),
        &encoded,
    )
    .expect("publish catalog mirror");
    bytes
}

#[test]
fn should_recover_single_cloud_wal_larger_than_configured_local_disk() {
    for budget in [256 * 1024_u64, 512 * 1024] {
        // Arrange
        let dir = tempfile::tempdir().expect("database directory");
        let opts = options(dir.path(), budget);
        let mut engine = Engine::open(opts.clone()).expect("initialize metadata");
        engine
            .shutdown(std::time::Duration::from_secs(10))
            .expect("shutdown");
        drop(engine);
        let records = budget * 3 / 2048;
        let source = publish_wal(dir.path(), records);
        assert!(source.len() as u64 > budget);

        // Act
        let mut recovered = Engine::open(opts).expect("stream WAL through bounded checkpoints");
        let cf = recovered
            .get_column_family("default")
            .expect("default column family");
        {
            let tx = recovered
                .begin_tx(cf.id(), TransactionMode::ReadOnly)
                .expect("read transaction");
            for sequence in 1..=records {
                assert_eq!(
                    tx.get(&sequence.to_be_bytes())
                        .expect("recovered read")
                        .as_deref(),
                    Some(value(sequence).as_slice())
                );
            }
        }

        // Assert
        assert!(
            !dir.path().join("cloud_recovery/wal").exists(),
            "replay must not stage the remote backlog"
        );
        assert!(
            std::fs::read_dir(dir.path().join("cloud_store/sst"))
                .expect("checkpoint directory")
                .count()
                > 1
        );
        recovered
            .shutdown(std::time::Duration::from_secs(30))
            .expect("shutdown recovered engine");
    }
}

#[cfg(feature = "failpoints")]
#[test]
fn should_exit_after_durable_recovery_checkpoint_in_child() {
    // Arrange
    let Some(path) = std::env::var_os("MIDGE_RECOVERY_CHECKPOINT_CHILD") else {
        return;
    };
    let path = std::path::PathBuf::from(path);
    let budget = std::env::var("MIDGE_RECOVERY_CHECKPOINT_BUDGET")
        .ok()
        .map_or(256 * 1024, |value| value.parse::<u64>().expect("budget"));
    let observe_only = std::env::var_os("MIDGE_RECOVERY_CHECKPOINT_OBSERVE_ONLY").is_some();
    let peak = observe_publication_disk(&path, budget);
    let marker = path.join("checkpoint-crash-observed");
    let peak_marker = path.join("checkpoint-working-peak");
    let checkpoint_path = path.clone();
    let checkpoint_peak = peak.clone();
    let crash_point = std::env::var("MIDGE_RECOVERY_CRASH_POINT")
        .unwrap_or_else(|_| "midge::recovery::after_checkpoint".into());
    fail::cfg_callback(&crash_point, move || {
        record_working_peak(&checkpoint_path, budget, &checkpoint_peak);
        if !observe_only {
            std::fs::write(
                &peak_marker,
                checkpoint_peak
                    .load(std::sync::atomic::Ordering::SeqCst)
                    .to_string(),
            )
            .expect("peak marker");
            std::fs::write(&marker, b"after durable checkpoint").expect("crash marker");
            std::process::exit(73);
        }
    })
    .expect("configure child checkpoint observation");

    // Act
    let mut engine = Engine::open(options(&path, budget)).expect("recover in child");

    // Assert
    assert!(observe_only, "child did not reach checkpoint");
    engine
        .shutdown(std::time::Duration::from_secs(30))
        .expect("shutdown observed recovery");
    std::fs::write(
        path.join("checkpoint-working-peak"),
        peak.load(std::sync::atomic::Ordering::SeqCst).to_string(),
    )
    .expect("peak marker");
}

#[cfg(feature = "failpoints")]
#[test]
fn should_recover_entire_wal_after_process_exits_between_checkpoints() {
    // Arrange
    let dir = tempfile::tempdir().expect("database directory");
    let opts = options(dir.path(), 256 * 1024);
    let mut engine = Engine::open(opts.clone()).expect("initialize metadata");
    engine
        .shutdown(std::time::Duration::from_secs(10))
        .expect("shutdown");
    drop(engine);
    let records = 384;
    let source = publish_wal(dir.path(), records);
    let catalog_path = dir
        .path()
        .join("cloud_store/wal/publication-catalog.v1.json");
    let before = crate::wal::cloud_catalog::WalPublicationCatalog::decode(
        &std::fs::read(&catalog_path).expect("catalog"),
    )
    .expect("decode");

    // Act
    let output = std::process::Command::new(std::env::current_exe().expect("test binary"))
        .args(["--exact", "engine::startup::streaming_recovery::tests::should_exit_after_durable_recovery_checkpoint_in_child", "--nocapture"])
        .env("MIDGE_RECOVERY_CHECKPOINT_CHILD", dir.path())
        .output().expect("run checkpoint child");
    assert_eq!(
        output.status.code(),
        Some(73),
        "child stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        std::fs::read(dir.path().join("checkpoint-crash-observed")).expect("exact crash marker"),
        b"after durable checkpoint"
    );
    let after = crate::wal::cloud_catalog::WalPublicationCatalog::decode(
        &std::fs::read(&catalog_path).expect("catalog"),
    )
    .expect("decode");
    assert_eq!(
        before.segments, after.segments,
        "checkpoints cannot retire source WAL"
    );
    assert_eq!(
        std::fs::read(
            dir.path()
                .join("cloud_store")
                .join(&after.segments[&1].object_key)
        )
        .expect("source WAL"),
        source
    );
    expire_child_lease(dir.path());
    let mut recovered = Engine::open(opts).expect("restart incremental replay");

    // Assert
    let cf = recovered
        .get_column_family("default")
        .expect("column family");
    {
        let tx = recovered
            .begin_tx(cf.id(), TransactionMode::ReadOnly)
            .expect("read transaction");
        for sequence in 1..=records {
            assert_eq!(
                tx.get(&sequence.to_be_bytes())
                    .expect("recovered read")
                    .as_deref(),
                Some(value(sequence).as_slice())
            );
        }
    }
    recovered
        .shutdown(std::time::Duration::from_secs(30))
        .expect("shutdown");
}

#[cfg(feature = "failpoints")]
fn expire_child_lease(path: &std::path::Path) {
    let lease = path.join("midge_primary_lease.json");
    let text = std::fs::read_to_string(&lease).expect("crashed lease record");
    let text = text
        .lines()
        .map(|line| {
            if line.starts_with("acquired_at: ") {
                "acquired_at: 1970-01-01T00:00:00Z"
            } else if line.starts_with("expires_at: ") {
                "expires_at: 1970-01-01T00:00:00Z"
            } else {
                line
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    std::fs::write(lease, text).expect("expire crashed lease");
    match std::fs::remove_file(path.join(".midge_leader.lock")) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => panic!("remove crashed lease acquisition lock: {error}"),
    }
}

#[cfg(feature = "failpoints")]
fn local_working_bytes(path: &std::path::Path) -> u64 {
    std::fs::read_dir(path)
        .expect("working directory")
        .map(|entry| {
            let entry = entry.expect("working entry");
            if entry.file_name() == "cloud_store" {
                return 0;
            }
            let metadata = entry.metadata().expect("working metadata");
            if metadata.is_dir() {
                local_working_bytes(&entry.path())
            } else {
                metadata.len()
            }
        })
        .sum()
}

#[cfg(feature = "failpoints")]
fn record_working_peak(path: &std::path::Path, budget: u64, peak: &std::sync::atomic::AtomicU64) {
    let working = local_working_bytes(path);
    peak.fetch_max(working, std::sync::atomic::Ordering::SeqCst);
    assert!(
        working <= budget,
        "working bytes {working} exceed configured disk {budget}"
    );
}

#[cfg(feature = "failpoints")]
fn observe_publication_disk(
    path: &std::path::Path,
    budget: u64,
) -> std::sync::Arc<std::sync::atomic::AtomicU64> {
    let peak = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    for name in [
        "midge::flush_worker::before_publication",
        "midge::flush_worker::after_sst_finalization",
        "midge::flush_worker::after_cloud_sst_upload",
        "midge::flush_worker::before_manifest_persist",
        "midge::flush_worker::after_control_metadata_publication",
    ] {
        let path = path.to_path_buf();
        let peak = peak.clone();
        fail::cfg_callback(name, move || record_working_peak(&path, budget, &peak))
            .expect("publication disk observation");
    }
    peak
}

#[cfg(feature = "failpoints")]
fn checkpoint_child(
    path: &std::path::Path,
    budget: u64,
    observe_only: bool,
) -> std::process::Output {
    let mut command = std::process::Command::new(std::env::current_exe().expect("test binary"));
    command.args(["--exact", "engine::startup::streaming_recovery::tests::should_exit_after_durable_recovery_checkpoint_in_child", "--nocapture"])
        .env("MIDGE_RECOVERY_CHECKPOINT_CHILD", path)
        .env("MIDGE_RECOVERY_CHECKPOINT_BUDGET", budget.to_string());
    if observe_only {
        command.env("MIDGE_RECOVERY_CHECKPOINT_OBSERVE_ONLY", "1");
    }
    command.output().expect("run checkpoint child")
}

#[cfg(feature = "failpoints")]
fn assert_child_output(output: &std::process::Output, expected: i32) {
    assert_eq!(
        output.status.code(),
        Some(expected),
        "child stdout: {}\nchild stderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[cfg(feature = "failpoints")]
fn assert_observed_peak(path: &std::path::Path, budget: u64) {
    let peak = std::fs::read_to_string(path.join("checkpoint-working-peak"))
        .expect("measured publication peak")
        .parse::<u64>()
        .expect("peak bytes");
    assert!(
        peak > 0 && peak <= budget,
        "observed peak {peak}, configured disk {budget}"
    );
}

#[cfg(feature = "failpoints")]
#[test]
fn should_bound_working_disk_at_publication_boundaries_when_recovering_large_wal() {
    // Arrange
    let dir = tempfile::tempdir().expect("database directory");
    let budget = 256 * 1024;
    let mut engine = Engine::open(options(dir.path(), budget)).expect("initialize metadata");
    engine
        .shutdown(std::time::Duration::from_secs(10))
        .expect("shutdown");
    drop(engine);
    let source = publish_wal(dir.path(), 384);
    assert!(source.len() as u64 > 3 * budget);

    // Act
    let output = checkpoint_child(dir.path(), budget, true);

    // Assert
    assert_child_output(&output, 0);
    assert_observed_peak(dir.path(), budget);
    assert!(!dir.path().join("cloud_recovery/wal").exists());
}

#[cfg(feature = "failpoints")]
fn publish_cross_family_records(path: &std::path::Path, second_id: u32) -> Vec<u8> {
    publish_records(path, |epoch| {
        let mut operations = vec![record(2, epoch), record(3, epoch)];
        for operation in &mut operations {
            operation.txn_id = Some(1);
            operation.key = bytes::Bytes::from_static(b"atomic");
        }
        operations[1].cf_id = second_id;
        let payload = crate::wal::encoding::encode_txn_batch_payload(1, 1, 4, epoch, &operations)
            .expect("transaction payload");
        let mut batch = crate::wal::WalRecord::new(
            crate::wal::WalOpKind::TxnBatch,
            bytes::Bytes::from_static(b"txn"),
            Some(payload),
            4,
            epoch,
        );
        batch.txn_id = Some(1);
        std::iter::once(batch)
            .chain((5..=384).map(|seq| record(seq, epoch)))
            .collect()
    })
}

#[cfg(feature = "failpoints")]
#[test]
fn should_recover_atomic_cross_family_write_after_only_first_family_checkpoint_is_published() {
    // Arrange
    let dir = tempfile::tempdir().expect("database directory");
    let budget = 256 * 1024;
    let opts = options(dir.path(), budget);
    let mut engine = Engine::open(opts.clone()).expect("initialize metadata");
    let second_id = engine
        .create_column_family("second")
        .expect("second family")
        .id();
    engine
        .shutdown(std::time::Duration::from_secs(10))
        .expect("shutdown");
    drop(engine);
    let source = publish_cross_family_records(dir.path(), second_id);
    let catalog_path = dir
        .path()
        .join("cloud_store/wal/publication-catalog.v1.json");
    let before = crate::wal::cloud_catalog::WalPublicationCatalog::decode(
        &std::fs::read(&catalog_path).expect("catalog"),
    )
    .expect("decode catalog");

    // Act
    let output = checkpoint_child(dir.path(), budget, false);

    // Assert
    assert_child_output(&output, 73);
    assert_observed_peak(dir.path(), budget);
    let manifest =
        crate::metadata::ManifestPersistence::load(dir.path()).expect("checkpoint manifest");
    assert!(manifest.files.iter().any(|file| file.cf_id == 0));
    assert!(
        !manifest.files.iter().any(|file| file.cf_id == second_id),
        "crash must interrupt publication between transaction families"
    );
    let after = crate::wal::cloud_catalog::WalPublicationCatalog::decode(
        &std::fs::read(catalog_path).expect("catalog"),
    )
    .expect("decode catalog");
    assert_eq!(
        before.segments, after.segments,
        "partial family checkpoint cannot retire source WAL"
    );
    assert_eq!(
        std::fs::read(
            dir.path()
                .join("cloud_store")
                .join(&after.segments[&1].object_key)
        )
        .expect("source WAL"),
        source
    );
    expire_child_lease(dir.path());
    let mut recovered = Engine::open(opts).expect("recover unfinished family");
    for (family, sequence) in [(0, 2), (second_id, 3)] {
        let tx = recovered
            .begin_tx(family, TransactionMode::ReadOnly)
            .expect("read transaction");
        assert_eq!(
            tx.get(b"atomic").expect("atomic value").as_deref(),
            Some(value(sequence).as_slice())
        );
    }
    {
        let tx = recovered
            .begin_tx(0, TransactionMode::ReadOnly)
            .expect("read tail");
        for sequence in 5_u64..=384 {
            assert_eq!(
                tx.get(&sequence.to_be_bytes())
                    .expect("tail value")
                    .as_deref(),
                Some(value(sequence).as_slice())
            );
        }
    }
    recovered
        .shutdown(std::time::Duration::from_secs(30))
        .expect("shutdown");
}

#[cfg(feature = "failpoints")]
fn spilled_options(path: &std::path::Path) -> OpenOptions {
    OpenOptions::cloud_simulated(path, "bucket", "streaming-recovery")
        .local_storage_budget(4 * 1024 * 1024)
        .transaction_memory_pool_size(8 * 1024)
        .with_memtable_size_limit(8 * 1024)
        .background_compaction(false)
        .build()
        .expect("spilled options")
}

#[cfg(feature = "failpoints")]
fn spilled_value() -> Vec<u8> {
    value(99).repeat(32)
}

#[cfg(feature = "failpoints")]
#[test]
fn should_exit_after_cloud_durable_spilled_transaction_in_child() {
    // Arrange
    let Some(path) = std::env::var_os("MIDGE_RECOVERY_SPILL_CHILD") else {
        return;
    };
    let path = std::path::PathBuf::from(path);
    let engine = Engine::open(spilled_options(&path)).expect("open spill writer");
    fail::cfg("midge::flush_worker::before_build", "pause").expect("pause SST publication");
    let mut tx = engine
        .begin_tx(0, TransactionMode::ReadWrite)
        .expect("spilled transaction");

    // Act
    tx.put(b"spilled".to_vec(), spilled_value(), None)
        .expect("spill value larger than transaction pool");
    tx.commit(crate::WriteOptions::cloud_strict())
        .expect("accept cloud durable spilled transaction");

    // Assert
    std::fs::write(path.join("spill-commit-observed"), b"cloud durable").expect("commit marker");
    std::process::exit(74);
}

#[cfg(feature = "failpoints")]
#[test]
fn should_recover_accepted_spilled_transaction_above_configured_spill_threshold() {
    // Arrange
    let dir = tempfile::tempdir().expect("database directory");
    let opts = spilled_options(dir.path());

    // Act
    let output = std::process::Command::new(std::env::current_exe().expect("test binary"))
        .args(["--exact", "engine::startup::streaming_recovery::tests::should_exit_after_cloud_durable_spilled_transaction_in_child", "--nocapture"])
        .env("MIDGE_RECOVERY_SPILL_CHILD", dir.path()).output().expect("run spill child");
    assert_child_output(&output, 74);
    assert!(dir.path().join("spill-commit-observed").exists());
    let catalog = crate::wal::cloud_catalog::WalPublicationCatalog::decode(
        &std::fs::read(
            dir.path()
                .join("cloud_store/wal/publication-catalog.v1.json"),
        )
        .expect("catalog"),
    )
    .expect("decode catalog");
    assert!(
        !catalog.segments.is_empty(),
        "accepted transaction must have replayable remote WAL"
    );
    let manifest = crate::metadata::ManifestPersistence::load(dir.path()).expect("manifest");
    assert!(
        manifest.files.is_empty(),
        "test must recover spilled transaction from WAL"
    );
    expire_child_lease(dir.path());
    let mut recovered =
        Engine::open(opts).expect("recover transaction above spill and freeze thresholds");

    // Assert
    {
        let tx = recovered
            .begin_tx(0, TransactionMode::ReadOnly)
            .expect("read transaction");
        assert_eq!(
            tx.get(b"spilled").expect("recovered value").as_deref(),
            Some(spilled_value().as_slice())
        );
    }
    recovered
        .shutdown(std::time::Duration::from_secs(30))
        .expect("shutdown recovered writer");
}

#[cfg(feature = "failpoints")]
#[test]
fn should_abandon_orphan_names_when_checkpoint_publication_is_interrupted() {
    for crash_point in [
        "midge::recovery::before_name_reservation",
        "midge::recovery::after_name_reservation",
        "midge::flush_worker::after_sst_finalization",
        "midge::flush_worker::after_cloud_sst_upload",
        "midge::flush_worker::before_manifest_persist",
        "midge::flush_worker::after_control_metadata_publication",
        "midge::recovery::after_checkpoint",
    ] {
        // Arrange
        let dir = tempfile::tempdir().expect("crash directory");
        let budget = 512 * 1024;
        let opts = options(dir.path(), budget);
        let mut engine = Engine::open(opts.clone()).expect("initialize metadata");
        engine
            .shutdown(std::time::Duration::from_secs(10))
            .expect("shutdown seed");
        drop(engine);
        publish_wal(dir.path(), 384);

        // Act
        let output = std::process::Command::new(std::env::current_exe().expect("test binary"))
            .args(["--exact", "engine::startup::streaming_recovery::tests::should_exit_after_durable_recovery_checkpoint_in_child", "--nocapture"])
            .env("MIDGE_RECOVERY_CHECKPOINT_CHILD", dir.path())
            .env("MIDGE_RECOVERY_CHECKPOINT_BUDGET", budget.to_string())
            .env("MIDGE_RECOVERY_CRASH_POINT", crash_point)
            .output().expect("crash child");
        assert_child_output(&output, 73);
        let before =
            crate::metadata::ManifestPersistence::load(dir.path()).expect("crashed manifest");
        let cloud_ssts = dir.path().join("cloud_store/sst");
        let orphan_names: Vec<_> = std::fs::read_dir(&cloud_ssts)
            .into_iter()
            .flatten()
            .map(|entry| {
                entry
                    .expect("cloud SST")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .filter(|name| !before.files.iter().any(|file| &file.name == name))
            .collect();
        expire_child_lease(dir.path());
        let mut recovered = Engine::open(opts).expect("resume interrupted recovery");

        // Assert
        let after =
            crate::metadata::ManifestPersistence::load(dir.path()).expect("recovered manifest");
        assert!(
            after.next_sst_seqs.get(&0) >= before.next_sst_seqs.get(&0),
            "{crash_point}"
        );
        for orphan in orphan_names {
            assert!(
                !after.files.iter().any(|file| file.name == orphan),
                "orphan name reused after {crash_point}: {orphan}"
            );
        }
        {
            let tx = recovered
                .begin_tx(0, TransactionMode::ReadOnly)
                .expect("verify transaction");
            for sequence in 1_u64..=384 {
                assert_eq!(
                    tx.get(&sequence.to_be_bytes())
                        .expect("recovered value")
                        .as_deref(),
                    Some(value(sequence).as_slice()),
                    "{crash_point}: {sequence}"
                );
            }
        }
        recovered
            .shutdown(std::time::Duration::from_secs(30))
            .expect("shutdown recovered engine");
    }
}
