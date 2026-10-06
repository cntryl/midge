//! Native independent-cache successor reclamation invalidates predecessor reads.
use super::*;
use cntryl_midge::IteratorState;

// Keep the takeover and exact authority ledger together.
#[allow(clippy::too_many_lines)]
#[test]
#[ignore = "requires Sqrzl; run cloud-integration.yml"]
fn should_fence_predecessor_reads_when_successor_reclaims_remote_snapshot_files() {
    // Arrange: real strict ACKs and a frozen snapshot before lease loss.
    require_sqrzl("native-read-fencing");
    let directory = tempfile::tempdir().unwrap();
    let bucket = format!("midge-read-fencing-{}", uuid::Uuid::new_v4());
    ensure_sqrzl_s3_bucket(&bucket).unwrap();
    let predecessor_proxy = Proxy::start("127.0.0.1:9000".parse().unwrap(), Boundary::BeforePut);
    let successor_proxy = Proxy::start("127.0.0.1:9000".parse().unwrap(), Boundary::BeforePut);
    let (loss_tx, loss_rx) = mpsc::channel();
    let predecessor = Engine::open(
        options(
            &directory.path().join("predecessor"),
            &bucket,
            "db",
            &predecessor_proxy.endpoint,
        )
        .lease_ttl(Duration::from_secs(18))
        .on_lease_loss(move || {
            let _ = loss_tx.send(());
        })
        .build()
        .unwrap(),
    )
    .unwrap();
    let cf = default_cf(&predecessor);
    let mut seed = predecessor
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .unwrap();
    for index in 0..32_u8 {
        seed.put(
            format!("row-{index:03}").into_bytes(),
            vec![index; 8192],
            None,
        )
        .unwrap();
    }
    seed.commit(WriteOptions::cloud_strict()).unwrap();
    predecessor.flush_cf(&cf).unwrap();
    let original_objects: Vec<_> = predecessor_proxy
        .control
        .observations()
        .into_iter()
        .filter(|event| {
            event.method == "PUT"
                && Path::new(&event.path).extension() == Some(std::ffi::OsStr::new("sst"))
                && event.status == Some(200)
        })
        .map(|event| event.path)
        .collect();
    assert_ne!(original_objects.len(), 0);
    for path in &original_objects {
        assert_ne!(signed_s3_request("GET", path, &[]).unwrap().len(), 0);
    }
    let original_catalog: serde_json::Value =
        serde_json::from_slice(&catalog(&bucket, "db")).unwrap();
    let frozen = predecessor
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .unwrap();
    assert_eq!(
        frozen.get(b"row-000").unwrap(),
        Some(Bytes::from(vec![0; 8192]))
    );
    let mut forward = frozen.scan(&Query::new()).unwrap();
    let mut reverse = frozen.scan(&Query::new().reverse()).unwrap();
    let mut old_write = predecessor
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .unwrap();
    old_write
        .put(b"rejected".to_vec(), b"uncommitted".to_vec(), None)
        .unwrap();

    // Act: deny renewal transport, wait for real expiry, then reclaim remotely.
    predecessor_proxy.control.stop_renewals();
    loss_rx.recv_timeout(WAIT).expect("actual authority loss");
    assert!(matches!(
        old_write.commit(WriteOptions::cloud_strict()),
        Err(MidgeError::Fenced(_))
    ));
    let deadline = Instant::now() + WAIT;
    let successor_options = options(
        &directory.path().join("successor"),
        &bucket,
        "db",
        &successor_proxy.endpoint,
    )
    .build()
    .unwrap();
    let mut successor = loop {
        match Engine::open(successor_options.clone()) {
            Ok(engine) => break engine,
            Err(MidgeError::LeaseHeld(_)) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(25));
            }
            Err(error) => panic!("successor open: {error}"),
        }
    };
    let successor_cf = default_cf(&successor);
    for round in 0..2_u8 {
        let mut write = successor
            .begin_tx(successor_cf.id(), TransactionMode::ReadWrite)
            .unwrap();
        for index in 0..32_u8 {
            write
                .put(
                    format!("row-{index:03}").into_bytes(),
                    vec![90 + round; 8192],
                    None,
                )
                .unwrap();
        }
        write.commit(WriteOptions::cloud_strict()).unwrap();
        successor.flush_cf(&successor_cf).unwrap();
    }
    successor.compact_all().unwrap();
    let cleanup_deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let observations = successor_proxy.control.observations();
        if original_objects.iter().all(|path| {
            observations.iter().any(|event| {
                event.method == "DELETE"
                    && event.path == *path
                    && matches!(event.status, Some(200 | 204))
            })
        }) {
            break;
        }
        assert!(
            Instant::now() < cleanup_deadline,
            "all original remote SSTs must actually be deleted"
        );
        std::thread::sleep(Duration::from_millis(10));
    }

    // Assert: explicit sticky Fenced replaces successful or misleading stale reads.
    let successor_catalog: serde_json::Value =
        serde_json::from_slice(&catalog(&bucket, "db")).unwrap();
    assert!(
        successor_catalog["fencing_epoch"].as_u64().unwrap()
            > original_catalog["fencing_epoch"].as_u64().unwrap()
    );
    for key in [b"row-000".as_slice(), b"row-031", b"missing"] {
        assert!(matches!(frozen.get(key), Err(MidgeError::Fenced(_))));
    }
    assert!(matches!(
        frozen.scan(&Query::new()),
        Err(MidgeError::Fenced(_))
    ));
    for scan in [&mut forward, &mut reverse] {
        for _ in 0..2 {
            assert!(matches!(scan.next(), Some(Err(MidgeError::Fenced(_)))));
            assert_eq!(scan.state(), IteratorState::Failed);
        }
    }
    for mode in [TransactionMode::ReadOnly, TransactionMode::ReadWrite] {
        assert!(matches!(
            predecessor.begin_tx(cf.id(), mode),
            Err(MidgeError::Fenced(_))
        ));
    }
    assert_successor_rows(&successor);
    drop(forward);
    drop(reverse);
    drop(frozen);
    drop(predecessor);
    successor.shutdown(WAIT).unwrap();
    let mut reopened = Engine::open(successor_options).unwrap();
    assert_successor_rows(&reopened);
    reopened.shutdown(WAIT).unwrap();
}

fn assert_successor_rows(engine: &Engine) {
    let cf = default_cf(engine);
    let read = engine.begin_tx(cf.id(), TransactionMode::ReadOnly).unwrap();
    let actual = read.scan(&Query::new()).unwrap().try_collect().unwrap();
    let expected: Vec<_> = (0..32_u8)
        .map(|index| {
            (
                Bytes::from(format!("row-{index:03}")),
                Bytes::from(vec![91; 8192]),
            )
        })
        .collect();
    assert_eq!(actual, expected);
    for (key, value) in expected {
        assert_eq!(read.get(&key).unwrap(), Some(value));
    }
    assert_eq!(read.get(b"rejected").unwrap(), None);
}
