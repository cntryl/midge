use super::*;
use crate::wal::append_completion::{WalAppendCompletion, WalAppendPrefix, WalPendingAppend};
use crate::wal::sequenced::SequencedWalUnit;

fn unit(value_len: usize) -> SequencedWalUnit {
    let mut operations = vec![
        WalRecord::new_cf(
            0,
            WalOpKind::Put,
            Bytes::from_static(b"ttl"),
            Some(Bytes::from(vec![b'x'; value_len])),
            2,
        ),
        WalRecord::new_cf(
            0,
            WalOpKind::Insert,
            Bytes::from_static(b"insert"),
            Some(Bytes::from_static(b"once")),
            3,
        ),
        WalRecord::new_cf(0, WalOpKind::Delete, Bytes::from_static(b"delete"), None, 4),
        WalRecord::new_cf(
            0,
            WalOpKind::DeleteRange,
            Bytes::from_static(b"start"),
            None,
            5,
        ),
    ];
    operations[0].expiration = Some(123_456);
    operations[3].range_end = Some(Bytes::from_static(b"stop"));
    for operation in &mut operations {
        operation.txn_id = Some(42);
    }
    SequencedWalUnit {
        txn_id: 42,
        begin_sequence: 1,
        commit_sequence: 6,
        cf_id: 0,
        operations,
    }
}

fn prefix(writer: &FsWalWriterIo, unit: &SequencedWalUnit) -> WalAppendPrefix {
    WalAppendPrefix {
        barrier_id: 73,
        writer_epoch: writer.header.epoch,
        segment_id: writer.header.id,
        first_sequence: unit.begin_sequence,
        last_sequence: unit.commit_sequence,
    }
}

fn completed(pending: &WalPendingAppend) -> Result<WalAppendCompletion, WalAppendError> {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(completion) = pending.try_complete() {
            return completion;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "writer did not complete sequenced append"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

fn reference_frame(unit: &SequencedWalUnit) -> MidgeResult<Vec<u8>> {
    let nested = crate::wal::encoding::encode_txn_batch_payload(
        unit.txn_id,
        unit.begin_sequence,
        unit.commit_sequence,
        unit.cf_id,
        &unit.operations,
    )?;
    let mut outer = WalRecord::new_cf(
        unit.cf_id,
        WalOpKind::TxnBatch,
        Bytes::from_static(b"txn"),
        Some(nested),
        unit.commit_sequence,
    );
    outer.txn_id = Some(unit.txn_id);
    let mut frame = Vec::new();
    crate::wal::frame::append_frame_encoded(&mut frame, |payload| {
        crate::wal::encoding::encode_into(&outer, payload)
    })?;
    Ok(frame)
}

#[test]
fn should_write_legacy_bytes_when_sequenced_units_encode_on_writer() -> MidgeResult<()> {
    // Arrange: cross compression thresholds with TTL, insert, delete and
    // range operations inside the same independently constructed old frame.
    for value_len in [0, 127, 128, 255, 1024, 4096] {
        let fs: Arc<dyn Fs> = Arc::new(cntryl_midge_core::io::MockFs::new());
        create_test_segment(fs.as_ref(), "wal.log")?;
        let writer = FsWalWriterIo::new("wal.log", Arc::clone(&fs))?;
        let unit = unit(value_len);
        let expected = reference_frame(&unit)?;
        let identity = prefix(&writer, &unit);

        // Act
        let pending = writer
            .submit_sequenced_batch(identity, vec![unit], Duration::from_secs(5), None)
            .expect("sequenced capability")
            .map_err(|failure| failure.error)?;
        let receipt = completed(&pending).map_err(|failure| failure.error)?;
        writer.sync_with_timeout(Duration::from_secs(5))?;
        let bytes = cntryl_midge_core::io::read_all(fs.as_ref(), &FsPath::new("wal.log"))
            .map_err(FsError::into_midge)?;

        // Assert: the unchanged segment parser and decoder replay the bytes.
        assert_eq!(
            &bytes[HEADER_LEN..],
            expected.as_slice(),
            "value length {value_len}"
        );
        assert_eq!(receipt.prefix, identity);
        assert_eq!(receipt.end_offset, bytes.len() as u64);
        let records = crate::wal::format::segment::records(&bytes)?;
        assert_eq!(records.len(), 1);
        let decoded = crate::wal::encoding::decode_txn_batch_payload(
            &records[0],
            records[0].value.as_ref().unwrap(),
        )?;
        assert_eq!(decoded.records.len(), 4);
        assert_eq!(decoded.records[0].expiration, Some(123_456));
        assert_eq!(
            decoded.records[3].range_end.as_deref(),
            Some(b"stop".as_slice())
        );
        assert_eq!(writer.tail().last_seq, Some(6));
    }
    Ok(())
}

fn entropy_value(len: usize) -> Vec<u8> {
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state.to_le_bytes()[3]
        })
        .collect()
}

fn put(key: &'static [u8], value: Vec<u8>, seq: u64, txn_id: u64) -> WalRecord {
    let mut record = WalRecord::new_cf(
        0,
        WalOpKind::Put,
        Bytes::from_static(key),
        Some(value.into()),
        seq,
    );
    record.txn_id = Some(txn_id);
    record
}

fn multi_op_unit(txn_id: u64, begin: u64, values: [Vec<u8>; 3]) -> SequencedWalUnit {
    let [small, entropy, compressible] = values;
    let mut ttl = put(b"ttl", small, begin + 1, txn_id);
    ttl.expiration = Some(777_000);
    let mut delete = WalRecord::new_cf(
        0,
        WalOpKind::Delete,
        Bytes::from_static(b"gone"),
        None,
        begin + 4,
    );
    delete.txn_id = Some(txn_id);
    let mut range = WalRecord::new_cf(
        0,
        WalOpKind::DeleteRange,
        Bytes::from_static(b"r0"),
        None,
        begin + 5,
    );
    range.range_end = Some(Bytes::from_static(b"r9"));
    range.txn_id = Some(txn_id);
    SequencedWalUnit {
        txn_id,
        begin_sequence: begin,
        commit_sequence: begin + 6,
        cf_id: 0,
        operations: vec![
            ttl,
            put(b"noisy", entropy, begin + 2, txn_id),
            put(b"packed", compressible, begin + 3, txn_id),
            delete,
            range,
        ],
    }
}

#[test]
fn should_write_legacy_bytes_when_batch_has_multiple_multi_operation_units() -> MidgeResult<()> {
    // Arrange: two transactions, each mixing TTL/no-TTL puts, a delete and a
    // delete-range, with one value below the threshold, one high-entropy value
    // above it, and one compressible value above it.
    let small = vec![b's'; 16];
    let entropy = entropy_value(4096);
    let compressible = vec![b'z'; 4096];
    assert!(
        cntryl_midge_core::codec::compress_wal_value_borrowed(&small)
            .1
            .is_none()
    );
    assert!(
        cntryl_midge_core::codec::compress_wal_value_borrowed(&entropy)
            .1
            .is_none(),
        "high-entropy value is stored raw"
    );
    assert!(
        cntryl_midge_core::codec::compress_wal_value_borrowed(&compressible)
            .1
            .is_some(),
        "compressible value is stored compressed"
    );
    let units = vec![
        multi_op_unit(
            41,
            1,
            [small.clone(), entropy.clone(), compressible.clone()],
        ),
        multi_op_unit(
            42,
            8,
            [small.clone(), entropy.clone(), compressible.clone()],
        ),
    ];
    let mut expected = Vec::new();
    for unit in &units {
        expected.extend(reference_frame(unit)?);
    }
    let fs: Arc<dyn Fs> = Arc::new(cntryl_midge_core::io::MockFs::new());
    create_test_segment(fs.as_ref(), "wal.log")?;
    let writer = FsWalWriterIo::new("wal.log", Arc::clone(&fs))?;
    let identity = WalAppendPrefix {
        barrier_id: 74,
        writer_epoch: writer.header.epoch,
        segment_id: writer.header.id,
        first_sequence: 1,
        last_sequence: 14,
    };

    // Act
    let pending = writer
        .submit_sequenced_batch(identity, units, Duration::from_secs(5), None)
        .expect("sequenced capability")
        .map_err(|failure| failure.error)?;
    let receipt = completed(&pending).map_err(|failure| failure.error)?;
    writer.sync_with_timeout(Duration::from_secs(5))?;
    let bytes = cntryl_midge_core::io::read_all(fs.as_ref(), &FsPath::new("wal.log"))
        .map_err(FsError::into_midge)?;

    // Assert: byte-identical to the independent frames, and replayable.
    assert_eq!(&bytes[HEADER_LEN..], expected.as_slice());
    assert_eq!(receipt.prefix, identity);
    assert_eq!(receipt.end_offset, bytes.len() as u64);
    let records = crate::wal::format::segment::records(&bytes)?;
    assert_eq!(records.len(), 2);
    for (record, txn_id) in records.iter().zip([41, 42]) {
        let decoded =
            crate::wal::encoding::decode_txn_batch_payload(record, record.value.as_ref().unwrap())?;
        assert_eq!(decoded.records.len(), 5);
        assert_eq!(decoded.records[0].expiration, Some(777_000));
        assert_eq!(decoded.records[1].expiration, None);
        assert_eq!(decoded.records[0].value.as_deref(), Some(small.as_slice()));
        assert_eq!(
            decoded.records[1].value.as_deref(),
            Some(entropy.as_slice())
        );
        assert_eq!(
            decoded.records[2].value.as_deref(),
            Some(compressible.as_slice())
        );
        assert_eq!(decoded.records[3].op, WalOpKind::Delete);
        assert_eq!(
            decoded.records[4].range_end.as_deref(),
            Some(b"r9".as_slice())
        );
        assert_eq!(decoded.txn_id, txn_id);
        assert_eq!(decoded.commit_seq, decoded.begin_seq + 6);
    }
    assert_eq!(writer.tail().last_seq, Some(14));
    Ok(())
}

#[test]
fn should_reject_raw_encoding_before_enqueue_when_memory_budget_is_too_small() -> MidgeResult<()> {
    // Arrange
    let fs: Arc<dyn Fs> = Arc::new(cntryl_midge_core::io::MockFs::new());
    create_test_segment(fs.as_ref(), "wal.log")?;
    let mut writer = FsWalWriterIo::new("wal.log", Arc::clone(&fs))?;
    let unit = unit(4096);
    writer.raw_submission_memory = cntryl_midge_core::common::resource_budget::ResourceBudget::new(
        unit.retained_memory_bound()? - 1,
    );

    // Act
    let result = writer
        .submit_sequenced_batch(
            prefix(&writer, &unit),
            vec![unit],
            Duration::from_secs(5),
            None,
        )
        .unwrap();

    // Assert
    let Err(failure) = result else {
        panic!("oversized raw request was admitted");
    };
    assert!(matches!(failure.error, MidgeError::ResourceLimit(_)));
    assert!(failure.unchanged);
    assert_eq!(writer.current_pos(), HEADER_LEN as u64);
    assert_eq!(writer.tail().last_seq, None);
    assert_eq!(writer.raw_submission_memory.used(), 0);
    Ok(())
}

#[cfg(feature = "failpoints")]
#[test]
fn should_keep_raw_memory_until_terminal_when_append_waiter_is_abandoned() -> MidgeResult<()> {
    struct Release(crossbeam::channel::Sender<()>);
    impl Drop for Release {
        fn drop(&mut self) {
            let _ = self.0.send(());
            fail::remove("midge::wal::before_writer_encode");
        }
    }

    // Arrange
    let _guard = cntryl_midge_core::failpoints::test_failpoint_guard();
    let fs: Arc<dyn Fs> = Arc::new(cntryl_midge_core::io::MockFs::new());
    create_test_segment(fs.as_ref(), "wal.log")?;
    let writer = FsWalWriterIo::new("wal.log", Arc::clone(&fs))?;
    let (entered, reached) = crossbeam::channel::bounded(1);
    let (release, released) = crossbeam::channel::unbounded();
    let release = Release(release);
    fail::cfg_callback("midge::wal::before_writer_encode", move || {
        let _ = entered.send(());
        released
            .recv_timeout(Duration::from_secs(30))
            .expect("release encode");
    })
    .expect("hold writer encode");
    let unit = unit(4096);
    let expected = reference_frame(&unit)?;

    // Act
    let pending = writer
        .submit_sequenced_batch(
            prefix(&writer, &unit),
            vec![unit],
            Duration::from_secs(5),
            None,
        )
        .unwrap()
        .map_err(|failure| failure.error)?;
    reached
        .recv_timeout(Duration::from_secs(5))
        .expect("writer reached encode");
    assert!(writer.raw_submission_memory.used() > 0);
    assert!(pending.try_complete().is_none());
    drop(pending);

    // Assert: caller abandonment cannot release the worker's charge or
    // cancel an admitted append. The raw input remains owned through encode.
    assert!(writer.raw_submission_memory.used() > 0);
    release.0.send(()).expect("release writer");
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while writer.raw_submission_memory.used() != 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "raw charge did not settle"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
    let bytes = cntryl_midge_core::io::read_all(fs.as_ref(), &FsPath::new("wal.log"))
        .map_err(FsError::into_midge)?;
    assert_eq!(&bytes[HEADER_LEN..], expected.as_slice());
    Ok(())
}

#[cfg(feature = "failpoints")]
#[test]
fn should_release_sequenced_memory_after_writer_failure_during_append() -> MidgeResult<()> {
    struct Release {
        encode: crossbeam::channel::Sender<()>,
        write: std::sync::mpsc::Sender<()>,
    }
    impl Drop for Release {
        fn drop(&mut self) {
            let _ = self.encode.send(());
            let _ = self.write.send(());
            fail::remove("midge::wal::before_writer_encode");
        }
    }

    // Arrange
    let _guard = cntryl_midge_core::failpoints::test_failpoint_guard();
    let (encode_entered, encode_reached) = crossbeam::channel::bounded(1);
    let (encode_release, encode_released) = crossbeam::channel::unbounded();
    let (write_entered, write_reached) = std::sync::mpsc::channel();
    let (write_release, write_released) = std::sync::mpsc::channel();
    let _release = Release {
        encode: encode_release.clone(),
        write: write_release.clone(),
    };
    fail::cfg_callback("midge::wal::before_writer_encode", move || {
        let _ = encode_entered.send(());
        let _ = encode_released.recv_timeout(Duration::from_secs(30));
    })
    .expect("hold sequenced writer encode");

    let inner = cntryl_midge_core::io::MockFs::new();
    create_test_segment(&inner, "wal.log")?;
    let fs = Arc::new(GatedNoSpaceFs {
        inner,
        entered: std::sync::Mutex::new(write_entered),
        release: Arc::new(std::sync::Mutex::new(write_released)),
    });
    let writer = FsWalWriterIo::new("wal.log", Arc::clone(&fs) as Arc<dyn Fs>)?;
    let unit = unit(4096);

    // Act: encode is held, then the physical append is held and fails.
    let pending = writer
        .submit_sequenced_batch(
            prefix(&writer, &unit),
            vec![unit],
            Duration::from_secs(5),
            None,
        )
        .expect("sequenced writer capability")
        .map_err(|failure| failure.error)?;
    encode_reached
        .recv_timeout(Duration::from_secs(5))
        .expect("writer reached encode");
    assert!(writer.raw_submission_memory.used() > 0);
    encode_release.send(()).expect("release encoding");
    write_reached
        .recv_timeout(Duration::from_secs(5))
        .expect("writer reached failing append");
    assert!(pending.try_complete().is_none());
    assert!(writer.raw_submission_memory.used() > 0);
    write_release.send(()).expect("release failing append");

    // Assert: terminal failure reaches the waiter and releases raw ownership.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let completion = loop {
        if let Some(completion) = pending.try_complete() {
            break completion;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "append failure did not settle"
        );
        std::thread::sleep(Duration::from_millis(1));
    };
    assert!(completion.is_err());
    assert_eq!(writer.raw_submission_memory.used(), 0);
    assert_eq!(writer.current_pos(), HEADER_LEN as u64);
    Ok(())
}

#[cfg(feature = "failpoints")]
#[test]
fn should_drain_sequenced_append_before_shutdown_given_held_writer_encoding() -> MidgeResult<()> {
    struct Release(crossbeam::channel::Sender<()>);
    impl Drop for Release {
        fn drop(&mut self) {
            let _ = self.0.send(());
            fail::remove("midge::wal::before_writer_encode");
        }
    }

    // Arrange
    let _guard = cntryl_midge_core::failpoints::test_failpoint_guard();
    let fs: Arc<dyn Fs> = Arc::new(cntryl_midge_core::io::MockFs::new());
    create_test_segment(fs.as_ref(), "wal.log")?;
    let writer = FsWalWriterIo::new("wal.log", Arc::clone(&fs))?;
    let (entered, reached) = crossbeam::channel::bounded(1);
    let (release, released) = crossbeam::channel::unbounded();
    let _release = Release(release.clone());
    fail::cfg_callback("midge::wal::before_writer_encode", move || {
        let _ = entered.send(());
        let _ = released.recv_timeout(Duration::from_secs(30));
    })
    .expect("hold sequenced writer encode");
    let unit = unit(4096);
    let expected = reference_frame(&unit)?;
    let memory = writer.raw_submission_memory.clone();
    let pending = writer
        .submit_sequenced_batch(
            prefix(&writer, &unit),
            vec![unit],
            Duration::from_secs(5),
            None,
        )
        .expect("sequenced writer capability")
        .map_err(|failure| failure.error)?;
    reached
        .recv_timeout(Duration::from_secs(5))
        .expect("writer reached encode");

    // Act: shutdown joins the worker and therefore waits for its owned encode.
    let (stopped, stopped_rx) = std::sync::mpsc::channel();
    let shutdown = std::thread::spawn(move || {
        drop(writer);
        let _ = stopped.send(());
    });

    // Assert: the admitted append remains owned until encode is released.
    assert!(matches!(
        stopped_rx.recv_timeout(Duration::from_millis(25)),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    ));
    assert!(pending.try_complete().is_none());
    assert!(memory.used() > 0);
    release.send(()).expect("release writer encode");
    stopped_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("writer shutdown did not drain sequenced append");
    shutdown.join().expect("join writer shutdown");
    completed(&pending).expect("sequenced append should complete before shutdown");
    assert_eq!(memory.used(), 0);
    let bytes = cntryl_midge_core::io::read_all(fs.as_ref(), &FsPath::new("wal.log"))
        .map_err(FsError::into_midge)?;
    assert_eq!(&bytes[HEADER_LEN..], expected.as_slice());
    Ok(())
}
