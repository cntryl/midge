//! Native independent-cache successor reclamation invalidates predecessor reads.
use super::*;
use cntryl_midge::{ColumnFamilyHandle, IteratorState};

fn seed_predecessor_rows(predecessor: &Engine, cf: &ColumnFamilyHandle) {
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
    predecessor.flush_cf(cf).unwrap();
}

fn persisted_snapshot_objects(proxy: &Proxy) -> Vec<String> {
    let original_objects: Vec<_> = proxy
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
    original_objects
}

fn reclaim_predecessor_objects(successor: &Engine, proxy: &Proxy, original_objects: &[String]) {
    let successor_cf = default_cf(successor);
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
        let observations = proxy.control.observations();
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
}

fn open_read_predecessor(
    cache: &Path,
    bucket: &str,
    proxy: &Proxy,
) -> (Engine, mpsc::Receiver<()>) {
    let (loss_tx, loss_rx) = mpsc::channel();
    let predecessor = Engine::open(
        options(cache, bucket, "db", &proxy.endpoint)
            .lease_ttl(Duration::from_secs(18))
            .on_lease_loss(move || {
                let _ = loss_tx.send(());
            })
            .build()
            .unwrap(),
    )
    .unwrap();
    (predecessor, loss_rx)
}

#[test]
fn should_fence_predecessor_reads_when_successor_reclaims_remote_snapshot_files() {
    // Arrange: real strict ACKs and a frozen snapshot before lease loss.
    require_sqrzl("native-read-fencing");
    let directory = tempfile::tempdir().unwrap();
    let bucket = format!("midge-read-fencing-{}", uuid::Uuid::new_v4());
    ensure_sqrzl_s3_bucket(&bucket).unwrap();
    let predecessor_proxy = Proxy::start("127.0.0.1:9000".parse().unwrap(), Boundary::BeforePut);
    let successor_proxy = Proxy::start("127.0.0.1:9000".parse().unwrap(), Boundary::BeforePut);
    let (predecessor, loss_rx) = open_read_predecessor(
        &directory.path().join("predecessor"),
        &bucket,
        &predecessor_proxy,
    );
    let cf = default_cf(&predecessor);
    seed_predecessor_rows(&predecessor, &cf);
    let original_objects = persisted_snapshot_objects(&predecessor_proxy);
    let original_catalog = WalPublicationCatalog::decode(&catalog(&bucket, "db")).unwrap();
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
    let successor_options = options(
        &directory.path().join("successor"),
        &bucket,
        "db",
        &successor_proxy.endpoint,
    )
    .build()
    .unwrap();
    let mut successor = open_successor(&successor_options);
    reclaim_predecessor_objects(&successor, &successor_proxy, &original_objects);

    // Assert: explicit sticky Fenced replaces successful or misleading stale reads.
    let successor_catalog = WalPublicationCatalog::decode(&catalog(&bucket, "db")).unwrap();
    assert!(successor_catalog.fencing_epoch() > original_catalog.fencing_epoch());
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
