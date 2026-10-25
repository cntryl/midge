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
        let fs: Arc<dyn Fs> = Arc::new(crate::io::MockFs::new());
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
        let bytes = crate::io::read_all(fs.as_ref(), &FsPath::new("wal.log"))
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

#[test]
fn should_reject_raw_encoding_before_enqueue_when_memory_budget_is_too_small() -> MidgeResult<()> {
    // Arrange
    let fs: Arc<dyn Fs> = Arc::new(crate::io::MockFs::new());
    create_test_segment(fs.as_ref(), "wal.log")?;
    let mut writer = FsWalWriterIo::new("wal.log", Arc::clone(&fs))?;
    let unit = unit(4096);
    writer.raw_submission_memory =
        crate::common::resource_budget::ResourceBudget::new(unit.retained_memory_bound()? - 1);

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
    let _guard = crate::failpoints::test_failpoint_guard();
    let fs: Arc<dyn Fs> = Arc::new(crate::io::MockFs::new());
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
    let bytes =
        crate::io::read_all(fs.as_ref(), &FsPath::new("wal.log")).map_err(FsError::into_midge)?;
    assert_eq!(&bytes[HEADER_LEN..], expected.as_slice());
    Ok(())
}
