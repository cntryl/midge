use cntryl_midge::storage::hybrid::{actor, policy, state};

/// Test: SBA should reserve space when below high watermark
#[test]
fn should_reserve_space_when_below_high_watermark() {
    let policy = policy::StorageBudgetPolicy::new(1024 * 1024); // 1 MB
    let mut actor = actor::StorageBudgetActor::new(policy);

    let result = actor.handle_event(actor::StorageBudgetEvent::ReserveForFlush { est_size: 100_000 });

    assert_eq!(result, Some(actor::ReservationResult::Ok));
    assert_eq!(actor.disk_state().new_sst_reserve, 100_000);
}

/// Test: SBA should signal wait for compaction at high watermark
#[test]
fn should_return_wait_for_compaction_at_high_watermark() {
    let policy = policy::StorageBudgetPolicy::new(1024 * 1024); // 1 MB = 1,048,576 bytes
    let mut actor = actor::StorageBudgetActor::new(policy);

    // Fill to 91% usage (90% threshold = 943,718 bytes, so set to 960,000)
    actor.handle_event(actor::StorageBudgetEvent::CompactionCompleted {
        output_sizes: vec![960_000],
    });

    let result = actor.handle_event(actor::StorageBudgetEvent::ReserveForFlush { est_size: 150_000 });

    assert_eq!(result, Some(actor::ReservationResult::WaitForCompaction));
}

/// Test: SBA should signal wait for cloud uploads at critical watermark
#[test]
fn should_return_wait_for_cloud_upload_at_critical_watermark() {
    let policy = policy::StorageBudgetPolicy::new(1024 * 1024); // 1 MB = 1,048,576 bytes
    let mut actor = actor::StorageBudgetActor::new(policy);

    // Fill to 96% usage (95% threshold = 996,147 bytes, so set to 1_000_000)
    actor.handle_event(actor::StorageBudgetEvent::CompactionCompleted {
        output_sizes: vec![1_000_000],
    });

    let result = actor.handle_event(actor::StorageBudgetEvent::ReserveForFlush { est_size: 10_000 });

    assert_eq!(result, Some(actor::ReservationResult::WaitForCloudUpload));
}

/// Test: SBA should reject writes at emergency watermark
#[test]
fn should_reject_writes_at_emergency_watermark() {
    let policy = policy::StorageBudgetPolicy::new(1024 * 1024); // 1 MB = 1,048,576 bytes
    let mut actor = actor::StorageBudgetActor::new(policy);

    // Fill to 99% usage (98% threshold = 1,027,607 bytes, so set to 1_040_000)
    actor.handle_event(actor::StorageBudgetEvent::CompactionCompleted {
        output_sizes: vec![1_040_000],
    });

    let result = actor.handle_event(actor::StorageBudgetEvent::ReserveForFlush { est_size: 10_000 });

    assert_eq!(result, Some(actor::ReservationResult::RejectNoSpace));
}

/// Test: SBA should track flush completion
#[test]
fn should_track_flush_completion() {
    let policy = policy::StorageBudgetPolicy::new(1024 * 1024); // 1 MB
    let mut actor = actor::StorageBudgetActor::new(policy);

    actor.handle_event(actor::StorageBudgetEvent::ReserveForFlush { est_size: 100_000 });
    // Complete with actual size equal to reserved size
    actor.handle_event(actor::StorageBudgetEvent::FlushCompleted { actual_size: 100_000 });

    assert_eq!(actor.disk_state().new_sst_reserve, 0);
    assert_eq!(actor.disk_state().sst_bytes, 100_000);
}

/// Test: SBA should queue evictions on cloud upload completion
#[test]
fn should_queue_evictions_on_cloud_upload() {
    let policy = policy::StorageBudgetPolicy::new(1024 * 1024); // 1 MB
    let mut actor = actor::StorageBudgetActor::new(policy);

    actor.handle_event(actor::StorageBudgetEvent::CloudUploadCompleted {
        sst_id: 42,
        actual_size: 50_000,
    });

    let evictions = actor.pending_evictions();
    assert_eq!(evictions.len(), 1);
    assert_eq!(evictions[0], (42, 50_000));
}

/// Test: SBA should handle compaction planning and completion
#[test]
fn should_handle_compaction_lifecycle() {
    let policy = policy::StorageBudgetPolicy::new(1024 * 1024); // 1 MB
    let mut actor = actor::StorageBudgetActor::new(policy);

    // Plan compaction with input sizes
    actor.handle_event(actor::StorageBudgetEvent::CompactionPlanned {
        input_sizes: vec![100_000, 150_000],
    });

    let disk = actor.disk_state();
    assert!(disk.compaction_reserve > 0);

    // Complete compaction
    actor.handle_event(actor::StorageBudgetEvent::CompactionCompleted {
        output_sizes: vec![225_000],
    });

    let disk = actor.disk_state();
    assert_eq!(disk.compaction_reserve, 0);
    assert_eq!(disk.sst_bytes, 225_000);
}

/// Test: SBA should track WAL growth
#[test]
fn should_track_wal_growth() {
    let policy = policy::StorageBudgetPolicy::new(1024 * 1024); // 1 MB
    let mut actor = actor::StorageBudgetActor::new(policy);

    actor.handle_event(actor::StorageBudgetEvent::WalGrew { bytes: 500_000 });

    assert_eq!(actor.disk_state().wal_bytes, 500_000);
}

/// Test: SBA should track local SST purges
#[test]
fn should_track_local_sst_purged() {
    let policy = policy::StorageBudgetPolicy::new(1024 * 1024); // 1 MB
    let mut actor = actor::StorageBudgetActor::new(policy);

    actor.handle_event(actor::StorageBudgetEvent::CompactionCompleted {
        output_sizes: vec![600_000],
    });
    actor.handle_event(actor::StorageBudgetEvent::LocalSSTPurged { bytes: 200_000 });

    assert_eq!(actor.disk_state().sst_bytes, 400_000);
}

/// Test: SBA disk state should compute percentages correctly
#[test]
fn should_compute_usage_percentage() {
    let mut disk = state::DiskState::new();
    disk.sst_bytes = 500_000;

    assert_eq!(disk.usage_percent(1_000_000), 50);
    assert_eq!(disk.usage_percent(500_000), 100);
}

/// Test: SBA should pop next eviction in FIFO order
#[test]
fn should_pop_evictions_in_fifo_order() {
    let policy = policy::StorageBudgetPolicy::new(1024 * 1024); // 1 MB
    let mut actor = actor::StorageBudgetActor::new(policy);

    actor.handle_event(actor::StorageBudgetEvent::CloudUploadCompleted {
        sst_id: 1,
        actual_size: 50_000,
    });
    actor.handle_event(actor::StorageBudgetEvent::CloudUploadCompleted {
        sst_id: 2,
        actual_size: 50_000,
    });

    assert_eq!(actor.next_eviction(), Some((1, 50_000)));
    assert_eq!(actor.next_eviction(), Some((2, 50_000)));
    assert_eq!(actor.next_eviction(), None);
}

