use super::*;

struct AppendAfterSyncWriter {
    inner: Box<dyn WalWriter>,
    next: WalRecord,
    appended: std::sync::atomic::AtomicBool,
}

impl WalWriter for AppendAfterSyncWriter {
    fn set_write_authority(&self, authority: Option<crate::wal::traits::WriteAuthority>) {
        self.inner.set_write_authority(authority);
    }
    fn append_record_accounted(
        &self,
        record: &WalRecord,
    ) -> Result<u64, crate::wal::traits::WalAppendError> {
        self.inner.append_record_accounted(record)
    }
    fn append_batch_accounted(
        &self,
        records: &[WalRecord],
    ) -> Result<u64, crate::wal::traits::WalAppendError> {
        self.inner.append_batch_accounted(records)
    }
    fn sync_with_timeout(&self, timeout: Duration) -> MidgeResult<()> {
        self.inner.sync_with_timeout(timeout)?;
        if !self
            .appended
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            self.inner
                .append_record_accounted(&self.next)
                .map_err(|failure| failure.error)?;
        }
        Ok(())
    }
    fn current_pos(&self) -> u64 {
        self.inner.current_pos()
    }
    fn tail(&self) -> crate::wal::format::SegmentTail {
        self.inner.tail()
    }
    fn seal_facts(&self, timeout: Duration) -> MidgeResult<crate::wal::format::SealedFacts> {
        self.inner.seal_facts(timeout)
    }
    fn write_footer(
        &self,
        facts: &crate::wal::format::SealedFacts,
        timeout: Duration,
    ) -> MidgeResult<()> {
        self.inner.write_footer(facts, timeout)
    }
}

#[test]
fn should_record_only_captured_sync_prefix_when_later_frame_arrives_before_receipt(
) -> MidgeResult<()> {
    // Arrange
    let directory = tempfile::tempdir()?;
    let mut state = RuntimeState::new(directory.path().to_path_buf(), false);
    let mut actor = WalActor::open(
        &state,
        DurabilityPolicy::Batched,
        BatchConfig::default(),
        1,
        crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
    )?;
    let first = prepare_put_transaction(
        &mut actor,
        &mut state,
        9401,
        b"synced",
        b"first",
        DurabilityPolicy::Batched,
    )?;
    let first_sequence = first.sequence_plan.commit_seq;
    actor.append_prepared_transactions(&mut state, vec![first])?;
    let mut later = prepare_put_transaction(
        &mut actor,
        &mut state,
        9402,
        b"not-synced",
        b"later",
        DurabilityPolicy::Batched,
    )?;
    let later_record = later
        .wal_batch
        .unit
        .take()
        .expect("sequenced later unit")
        .encode_record()?;
    let expected_floor = actor.writer().expect("writer").current_pos();
    let WalIoState::Open { fs, writer } = std::mem::replace(&mut actor.io, WalIoState::Memory)
    else {
        panic!("open actor");
    };
    actor.io = WalIoState::Open {
        fs,
        writer: Box::new(AppendAfterSyncWriter {
            inner: writer,
            next: later_record,
            appended: std::sync::atomic::AtomicBool::new(false),
        }),
    };
    let ticket = WalSyncTicket::for_test(7);

    // Act: the double performs a real later append after the physical sync,
    // before the actor receives its acknowledgement. Only the first prefix
    // is simulated as surviving the subsequent power loss.
    let receipt = actor.begin_sync_transition(&mut state, &ticket)?;
    assert!(actor.writer().expect("writer").current_pos() > expected_floor);
    actor.commit_sync_transition(&mut state, receipt, &ticket)?;

    // Assert
    assert_eq!(
        state.wal.history.ledger().open_member().durable_bytes,
        Some(expected_floor)
    );
    assert_eq!(state.wal.frontiers.last_synced(), first_sequence);
    let segment = state.wal.open_segment();
    drop(actor);
    drop(state);
    std::fs::OpenOptions::new()
        .write(true)
        .open(
            directory
                .path()
                .join("wal")
                .join(crate::wal::segment_file_name(segment)),
        )?
        .set_len(expected_floor)?;
    let recovered = RuntimeState::try_new(
        directory.path(),
        false,
        crate::config::RecoveryPolicy::Strict,
    )?;
    let table = &recovered.get_cf(0).expect("default family").memtable;
    assert_eq!(table.get(b"synced")?, Some(b"first".to_vec()));
    assert_eq!(table.get(b"not-synced")?, None);
    Ok(())
}

#[test]
fn should_fence_receipt_commit_when_barrier_generation_or_writer_identity_mismatches(
) -> MidgeResult<()> {
    for mismatch in 0..3 {
        // Arrange
        let directory = tempfile::tempdir()?;
        let mut state = RuntimeState::new(directory.path().to_path_buf(), false);
        let mut actor = WalActor::open(
            &state,
            DurabilityPolicy::Batched,
            BatchConfig::default(),
            1,
            crate::config::DEFAULT_STORAGE_IO_TIMEOUT,
        )?;
        let unit = prepare_put_transaction(
            &mut actor,
            &mut state,
            9781,
            b"key",
            b"value",
            DurabilityPolicy::Batched,
        )?;
        actor.append_prepared_transactions(&mut state, vec![unit])?;
        let ticket = WalSyncTicket::for_test(7);
        let mut receipt = actor.begin_sync_transition(&mut state, &ticket)?;
        match mismatch {
            0 => receipt.generation = Some(8),
            1 => receipt.writer_epoch = receipt.writer_epoch.saturating_add(1),
            _ => receipt.segment_id = receipt.segment_id.saturating_add(1),
        }

        // Act
        let result = actor.commit_sync_transition(&mut state, receipt, &ticket);

        // Assert
        assert!(result.is_err());
        assert!(actor.is_fenced());
        assert_eq!(state.wal.frontiers.last_synced(), 0);
    }
    Ok(())
}
