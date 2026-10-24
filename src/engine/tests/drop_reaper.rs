use super::*;

fn reaper_options(path: &std::path::Path, cloud: bool) -> MidgeResult<OpenOptions> {
    let builder = if cloud {
        OpenOptions::cloud_simulated(path, "drop-reaper", "db")
    } else {
        OpenOptions::local(path)
    };
    builder
        .drop_transaction_wait_timeout(Duration::from_millis(20))
        .build()
}

fn reopen_after_reaper(path: &std::path::Path, cloud: bool) -> MidgeResult<Engine> {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        match Engine::open(reaper_options(path, cloud)?) {
            Ok(engine) => return Ok(engine),
            Err(_) if std::time::Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(5))
            }
            Err(error) => return Err(error),
        }
    }
}

#[test]
fn should_fence_outliving_reads_scans_and_commit_when_drop_transaction_wait_expires(
) -> MidgeResult<()> {
    for cloud in [false, true] {
        // Arrange
        let directory = tempfile::tempdir()?;
        let engine = Engine::open(reaper_options(directory.path(), cloud)?)?;
        let cf = engine.get_column_family("default").expect("default family");
        let mut seed = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
        seed.put(b"a".to_vec(), b"committed".to_vec(), None)?;
        seed.put(b"b".to_vec(), b"second".to_vec(), None)?;
        seed.commit(if cloud {
            WriteOptions::cloud_strict()
        } else {
            WriteOptions::sync()
        })?;
        let frozen = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
        let mut writer = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
        writer.put(b"own".to_vec(), b"uncommitted".to_vec(), None)?;
        let mut scan = frozen.scan(&Query::new())?;
        assert_eq!(scan.next().expect("first row")?.0.as_ref(), b"a");

        // Act: keep both transactions and the cached iterator alive.
        drop(engine);
        let mut successor = reopen_after_reaper(directory.path(), cloud)?;

        // Assert
        assert!(matches!(frozen.get(b"a"), Err(MidgeError::Fenced(_))));
        assert!(matches!(writer.get(b"own"), Err(MidgeError::Fenced(_))));
        assert!(matches!(scan.next(), Some(Err(MidgeError::Fenced(_)))));
        assert!(matches!(
            writer.commit(WriteOptions::buffered()),
            Err(MidgeError::Fenced(_))
        ));
        let current = successor.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
        assert_eq!(current.get(b"a")?.as_deref(), Some(b"committed".as_slice()));
        assert_eq!(current.get(b"own")?, None);
        drop(current);
        drop(scan);
        drop(frozen);
        successor.shutdown(Duration::from_secs(5))?;
    }
    Ok(())
}

#[cfg(feature = "failpoints")]
#[test]
fn should_retain_writer_authority_after_drop_wait_until_admitted_wal_effect_drains(
) -> MidgeResult<()> {
    // Arrange
    let _guard = crate::failpoints::test_failpoint_guard();
    let directory = tempfile::tempdir()?;
    let engine = Engine::open(reaper_options(directory.path(), false)?)?;
    let cf = engine.get_column_family("default").expect("default family");
    let leaked = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
    let handle = engine.runtime_handle.clone();
    let (entered, waiting) = crossbeam::channel::bounded(1);
    let (release, held) = crossbeam::channel::bounded(1);
    fail::cfg_callback(
        "midge::wal::txn_after_commit_append_before_sync",
        move || {
            entered.send(()).expect("report admitted WAL effect");
            let _ = held.recv(); // sender drop also releases the owner during test failure cleanup
        },
    )
    .expect("hold admitted WAL unit");
    let worker = std::thread::spawn(move || {
        handle.send_and_wait(RuntimeMsg::ApplyTransaction {
            request_id: next_request_id().expect("request id"),
            ops: vec![crate::runtime::TransactionOp::Put {
                cf_id: 0,
                key: bytes::Bytes::from_static(b"accepted"),
                value: bytes::Bytes::from_static(b"durable"),
                ttl_seconds: None,
                insert_only: false,
            }],
            assertions: Vec::new(),
            durability_policy: Some(crate::wal::DurabilityPolicy::Strict),
            start_sequence: None,
            conflict_policy: crate::runtime::ConflictPolicy::LastWriteWins,
            response_tx: None,
        })
    });
    waiting
        .recv_timeout(Duration::from_secs(2))
        .expect("WAL unit reached sync boundary");

    // Act
    drop(engine);
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    loop {
        match leaked.get(b"accepted") {
            Err(MidgeError::Fenced(_)) => break,
            Ok(None) => {}
            other => panic!("unexpected outliving read while waiting for fence: {other:?}"),
        }
        assert!(
            std::time::Instant::now() < deadline,
            "drop did not fence leaked transaction"
        );
        std::thread::sleep(Duration::from_millis(1));
    }

    // Assert: the transaction timeout is not permission to abandon effects.
    assert!(matches!(
        leaked.get(b"accepted"),
        Err(MidgeError::Fenced(_))
    ));
    assert!(Engine::open(reaper_options(directory.path(), false)?).is_err());
    release.send(()).expect("release admitted WAL unit");
    assert!(matches!(
        worker.join().expect("request worker")?,
        RuntimeResponse::TransactionApplied { .. }
    ));
    fail::remove("midge::wal::txn_after_commit_append_before_sync");
    let mut successor = reopen_after_reaper(directory.path(), false)?;
    let current = successor.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
    assert_eq!(
        current.get(b"accepted")?.as_deref(),
        Some(b"durable".as_slice())
    );
    drop(current);
    drop(leaked);
    successor.shutdown(Duration::from_secs(5))?;
    Ok(())
}
