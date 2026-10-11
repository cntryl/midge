use super::*;
use crate::common::MidgeError;
use crate::io::{MockFs, OpenMode, OpenOptions};
use crate::simulation::crash_fs::{CrashDevice, Fault, FaultEffect, FaultError, FaultOp};
use crate::wal::ledger::LedgerEdit;

fn genesis_ledger() -> WalLedger {
    let mut ledger = WalLedger::empty();
    ledger
        .apply(&LedgerEdit::Genesis {
            holder: "holder-a".to_string(),
            open: 1,
        })
        .expect("genesis applies");
    ledger
}

fn rotate(seal: u64) -> LedgerEdit {
    LedgerEdit::Rotate {
        seal,
        open: seal + 1,
    }
}

fn applied(ledger: &WalLedger, edits: &[LedgerEdit]) -> WalLedger {
    let mut ledger = ledger.clone();
    for edit in edits {
        ledger.apply(edit).expect("edit applies");
    }
    ledger
}

fn mock_fs() -> Arc<dyn Fs> {
    Arc::new(MockFs::new())
}

fn crash_fs(device: &CrashDevice) -> Arc<dyn Fs> {
    Arc::new(device.fs())
}

fn ledger_bytes(fs: &Arc<dyn Fs>) -> Vec<u8> {
    crate::io::read_all(fs.as_ref(), &FsPath::new(LEDGER_FILE))
        .expect("read LEDGER")
        .to_vec()
}

/// Replace LEDGER's content with exactly `bytes`.
fn write_ledger_bytes(fs: &Arc<dyn Fs>, bytes: &[u8]) {
    let mut file = fs
        .open(
            &FsPath::new(LEDGER_FILE),
            OpenOptions {
                mode: OpenMode::ReadWrite,
                create: true,
                create_new: false,
                truncate: true,
            },
        )
        .expect("open LEDGER");
    file.write_at(0, Bytes::copy_from_slice(bytes))
        .expect("write LEDGER");
    file.sync(Durability::Durable).expect("sync LEDGER");
}

fn loaded(fs: &Arc<dyn Fs>) -> (WalLedger, LedgerTail) {
    LedgerStore::load(fs)
        .expect("LEDGER loads")
        .expect("LEDGER exists")
}

fn header() -> Vec<u8> {
    let mut bytes = MAGIC.to_vec();
    bytes.extend_from_slice(&VERSION.to_le_bytes());
    bytes
}

fn snapshot_frame(ledger: &WalLedger) -> Vec<u8> {
    encode_frame(&Frame::Snapshot(ledger.clone())).expect("encode snapshot")
}

fn edit_frame(edit: &LedgerEdit) -> Vec<u8> {
    encode_frame(&Frame::Edit(edit.clone())).expect("encode edit")
}

/// A LEDGER image: the header, a snapshot of genesis, then `edits`. Returns
/// the image and the offset where each edit frame starts.
fn image_with(edits: &[LedgerEdit]) -> (Vec<u8>, Vec<usize>) {
    let mut bytes = header();
    bytes.extend(snapshot_frame(&genesis_ledger()));
    let mut starts = Vec::new();
    for edit in edits {
        starts.push(bytes.len());
        bytes.extend(edit_frame(edit));
    }
    (bytes, starts)
}

fn is_corruption(result: &MidgeResult<Option<(WalLedger, LedgerTail)>>) -> bool {
    matches!(result, Err(MidgeError::Corruption(_)))
}

/// Run `check` against every state a power loss can leave on `device`.
fn for_each_power_loss(device: &CrashDevice, check: impl Fn(&Arc<dyn Fs>)) {
    let decisions = device.decisions();
    let total: usize = decisions.iter().map(|decision| decision.options).product();
    assert!(total <= 1 << 14, "too many outcomes: {decisions:?}");
    for outcome in 0..total {
        let mut index = outcome;
        let choices: Vec<usize> = decisions
            .iter()
            .map(|decision| {
                let choice = index % decision.options;
                index /= decision.options;
                choice
            })
            .collect();
        let fork = device.fork();
        fork.power_loss_with(|decision| {
            choices[decisions
                .iter()
                .position(|known| known == decision)
                .expect("decision is enumerated")]
        });
        check(&crash_fs(&fork));
    }
}

/// Every way to fail the first call of each operation in `ops`.
fn interruptions(ops: &[FaultOp]) -> Vec<Fault> {
    ops.iter()
        .flat_map(|op| {
            [FaultEffect::Before, FaultEffect::After].map(|effect| Fault {
                op: *op,
                path: String::new(),
                nth: 1,
                effect,
                error: FaultError::Io,
            })
        })
        .collect()
}

#[test]
fn should_return_none_when_ledger_is_absent() {
    // Arrange
    let fs = mock_fs();

    // Act
    let result = LedgerStore::load(&fs).expect("absent LEDGER loads");

    // Assert
    assert!(result.is_none());
}

#[test]
fn should_load_snapshot_when_ledger_is_created() {
    // Arrange
    let fs = mock_fs();
    let ledger = genesis_ledger();

    // Act
    LedgerStore::create(Arc::clone(&fs), &ledger).expect("create LEDGER");

    // Assert
    assert_eq!(loaded(&fs), (ledger, LedgerTail::Clean));
    assert!(!fs
        .exists(&FsPath::new(LEDGER_TEMP_FILE))
        .expect("stat temp"));
}

#[test]
fn should_replay_appended_edits_when_ledger_reloads() {
    // Arrange
    let fs = mock_fs();
    let ledger = genesis_ledger();
    let edits = [
        rotate(1),
        rotate(2),
        LedgerEdit::AdvanceLogStart { to: 2 },
        LedgerEdit::SetAside { from: 3, open: 7 },
    ];
    let mut store = LedgerStore::create(Arc::clone(&fs), &ledger).expect("create LEDGER");

    // Act
    for edit in &edits {
        store.append(edit).expect("append edit");
    }

    // Assert
    assert_eq!(loaded(&fs), (applied(&ledger, &edits), LedgerTail::Clean));
}

#[test]
fn should_continue_appending_when_store_reopens() {
    // Arrange
    let fs = mock_fs();
    let ledger = genesis_ledger();
    let mut store = LedgerStore::create(Arc::clone(&fs), &ledger).expect("create LEDGER");
    store.append(&rotate(1)).expect("append before reopen");
    drop(store);

    // Act
    let mut reopened = LedgerStore::open(Arc::clone(&fs))
        .expect("open LEDGER")
        .expect("LEDGER exists");
    reopened.append(&rotate(2)).expect("append after reopen");

    // Assert
    assert_eq!(
        loaded(&fs),
        (applied(&ledger, &[rotate(1), rotate(2)]), LedgerTail::Clean)
    );
}

#[test]
fn should_refuse_create_when_ledger_exists() {
    // Arrange
    let fs = mock_fs();
    let ledger = genesis_ledger();
    let mut store = LedgerStore::create(Arc::clone(&fs), &ledger).expect("create LEDGER");
    store.append(&rotate(1)).expect("append edit");
    let before = ledger_bytes(&fs);

    // Act
    let result = LedgerStore::create(Arc::clone(&fs), &ledger);

    // Assert
    assert!(result.is_err());
    assert_eq!(ledger_bytes(&fs), before);
}

#[test]
fn should_refuse_create_when_ledger_is_uninitialized() {
    // Arrange
    let fs = mock_fs();

    // Act
    let result = LedgerStore::create(Arc::clone(&fs), &WalLedger::empty());

    // Assert
    assert!(result.is_err());
    assert!(LedgerStore::load(&fs).expect("load").is_none());
}

#[test]
fn should_return_none_when_opening_absent_ledger() {
    // Arrange
    let fs = mock_fs();

    // Act
    let result = LedgerStore::open(Arc::clone(&fs));

    // Assert
    assert!(matches!(result, Ok(None)));
}

#[test]
fn should_refuse_append_when_edit_does_not_apply() {
    // Arrange
    let fs = mock_fs();
    let ledger = genesis_ledger();
    let mut store = LedgerStore::create(Arc::clone(&fs), &ledger).expect("create LEDGER");
    let before = ledger_bytes(&fs);

    // Act
    let refused = store.append(&rotate(5));
    let accepted = store.append(&rotate(1));

    // Assert
    assert!(matches!(refused, Err(MidgeError::Corruption(_))));
    accepted.expect("a valid edit still appends");
    assert_eq!(&ledger_bytes(&fs)[..before.len()], before);
    assert_eq!(
        loaded(&fs),
        (applied(&ledger, &[rotate(1)]), LedgerTail::Clean)
    );
}

#[test]
fn should_report_torn_when_final_frame_is_truncated() {
    // Arrange
    let fs = mock_fs();
    let (bytes, starts) = image_with(&[rotate(1), rotate(2)]);
    write_ledger_bytes(&fs, &bytes[..bytes.len() - 5]);
    assert!(bytes.len() - 5 > starts[1]);

    // Act
    let result = loaded(&fs);

    // Assert
    assert_eq!(
        result,
        (applied(&genesis_ledger(), &[rotate(1)]), LedgerTail::Torn)
    );
}

#[test]
fn should_report_torn_when_final_frame_header_is_short() {
    // Arrange
    let fs = mock_fs();
    let (bytes, starts) = image_with(&[rotate(1), rotate(2)]);
    write_ledger_bytes(&fs, &bytes[..starts[1] + 3]);

    // Act
    let result = loaded(&fs);

    // Assert
    assert_eq!(
        result,
        (applied(&genesis_ledger(), &[rotate(1)]), LedgerTail::Torn)
    );
}

#[test]
fn should_report_torn_when_final_frame_checksum_is_bad() {
    // Arrange
    let fs = mock_fs();
    let (mut bytes, _) = image_with(&[rotate(1), rotate(2)]);
    let last = bytes.len() - 1;
    bytes[last] ^= 0x40;
    write_ledger_bytes(&fs, &bytes);

    // Act
    let result = loaded(&fs);

    // Assert
    assert_eq!(
        result,
        (applied(&genesis_ledger(), &[rotate(1)]), LedgerTail::Torn)
    );
}

#[test]
fn should_report_torn_when_final_frame_length_is_zeroed() {
    // Arrange: page-granular writeback can keep a frame's body while its
    // length field, on the previous page, still reads zero.
    let fs = mock_fs();
    let (mut bytes, starts) = image_with(&[rotate(1), rotate(2)]);
    bytes[starts[1]..starts[1] + 4].fill(0);
    write_ledger_bytes(&fs, &bytes);

    // Act
    let result = loaded(&fs);

    // Assert
    assert_eq!(
        result,
        (applied(&genesis_ledger(), &[rotate(1)]), LedgerTail::Torn)
    );
}

#[test]
fn should_report_torn_when_tail_is_zero_filled() {
    // Arrange: an unsynced append can persist the new length without its bytes.
    let fs = mock_fs();
    let (mut bytes, _) = image_with(&[rotate(1)]);
    bytes.extend([0; 300]);
    write_ledger_bytes(&fs, &bytes);

    // Act
    let result = loaded(&fs);

    // Assert
    assert_eq!(
        result,
        (applied(&genesis_ledger(), &[rotate(1)]), LedgerTail::Torn)
    );
}

#[test]
fn should_fail_when_middle_frame_is_corrupt() {
    // Arrange
    let fs = mock_fs();
    let (mut bytes, starts) = image_with(&[rotate(1), rotate(2), rotate(3)]);
    bytes[starts[1] + FRAME_HEADER_LEN + 2] ^= 0x01;
    write_ledger_bytes(&fs, &bytes);

    // Act
    let result = LedgerStore::load(&fs);

    // Assert
    assert!(is_corruption(&result), "{result:?}");
}

#[test]
fn should_fail_when_frame_length_hides_later_frames() {
    // Arrange: a corrupt length that overruns EOF must not pass for a torn
    // tail when acknowledged frames follow it.
    let fs = mock_fs();
    let (mut bytes, starts) = image_with(&[rotate(1), rotate(2), rotate(3)]);
    bytes[starts[1]..starts[1] + 4].copy_from_slice(&0x00ff_ffff_u32.to_le_bytes());
    write_ledger_bytes(&fs, &bytes);

    // Act
    let result = LedgerStore::load(&fs);

    // Assert
    assert!(is_corruption(&result), "{result:?}");
}

#[test]
fn should_fail_when_snapshot_frame_is_torn() {
    // Arrange: LEDGER is only ever published whole, so a damaged snapshot is
    // not a torn append.
    let fs = mock_fs();
    let (bytes, starts) = image_with(&[rotate(1)]);
    write_ledger_bytes(&fs, &bytes[..starts[0] - 3]);

    // Act
    let result = LedgerStore::load(&fs);

    // Assert
    assert!(is_corruption(&result), "{result:?}");
}

#[test]
fn should_fail_when_ledger_is_shorter_than_header() {
    // Arrange
    let fs = mock_fs();
    let full = header();
    let images: [&[u8]; 3] = [b"", &MAGIC[..5], &full[..HEADER_LEN - 1]];

    // Act
    let results = images.map(|image| {
        write_ledger_bytes(&fs, image);
        LedgerStore::load(&fs)
    });

    // Assert
    for result in &results {
        assert!(is_corruption(result), "{result:?}");
    }
}

#[test]
fn should_fail_when_header_has_no_snapshot() {
    // Arrange
    let fs = mock_fs();
    write_ledger_bytes(&fs, &header());

    // Act
    let result = LedgerStore::load(&fs);

    // Assert
    assert!(is_corruption(&result), "{result:?}");
}

#[test]
fn should_fail_when_magic_is_wrong() {
    // Arrange
    let fs = mock_fs();
    let (mut bytes, _) = image_with(&[]);
    bytes[0] ^= 0xff;
    write_ledger_bytes(&fs, &bytes);

    // Act
    let result = LedgerStore::load(&fs);

    // Assert
    assert!(is_corruption(&result), "{result:?}");
}

#[test]
fn should_fail_when_version_is_unsupported() {
    // Arrange
    let fs = mock_fs();
    let (mut bytes, _) = image_with(&[]);
    bytes[MAGIC.len()..HEADER_LEN].copy_from_slice(&(VERSION + 1).to_le_bytes());
    write_ledger_bytes(&fs, &bytes);

    // Act
    let result = LedgerStore::load(&fs);

    // Assert
    assert!(
        matches!(result, Err(MidgeError::CompatibilityError(_))),
        "{result:?}"
    );
}

#[test]
fn should_fail_when_first_frame_is_edit() {
    // Arrange
    let fs = mock_fs();
    let mut bytes = header();
    bytes.extend(edit_frame(&LedgerEdit::Genesis {
        holder: "holder-a".to_string(),
        open: 1,
    }));
    write_ledger_bytes(&fs, &bytes);

    // Act
    let result = LedgerStore::load(&fs);

    // Assert
    assert!(is_corruption(&result), "{result:?}");
}

#[test]
fn should_fail_when_snapshot_is_uninitialized() {
    // Arrange
    let fs = mock_fs();
    let mut bytes = header();
    bytes.extend(snapshot_frame(&WalLedger::empty()));
    write_ledger_bytes(&fs, &bytes);

    // Act
    let result = LedgerStore::load(&fs);

    // Assert
    assert!(is_corruption(&result), "{result:?}");
}

#[test]
fn should_fail_when_edit_does_not_apply() {
    // Arrange
    let fs = mock_fs();
    let (bytes, _) = image_with(&[rotate(1), rotate(5)]);
    write_ledger_bytes(&fs, &bytes);

    // Act
    let result = LedgerStore::load(&fs);

    // Assert
    assert!(is_corruption(&result), "{result:?}");
}

#[test]
fn should_fail_when_later_frame_is_snapshot() {
    // Arrange
    let fs = mock_fs();
    let (mut bytes, _) = image_with(&[rotate(1)]);
    bytes.extend(snapshot_frame(&genesis_ledger()));
    write_ledger_bytes(&fs, &bytes);

    // Act
    let result = LedgerStore::load(&fs);

    // Assert
    assert!(is_corruption(&result), "{result:?}");
}

#[test]
fn should_fail_when_checksummed_frame_does_not_decode() {
    // Arrange
    let fs = mock_fs();
    let (mut bytes, _) = image_with(&[]);
    let payload = b"{\"Edit\":\"unknown\"}";
    let length = u32::try_from(payload.len())
        .expect("payload fits")
        .to_le_bytes();
    let checksum = crc32c::crc32c_append(crc32c::crc32c(&length), payload);
    bytes.extend(length);
    bytes.extend(checksum.to_le_bytes());
    bytes.extend(payload);
    write_ledger_bytes(&fs, &bytes);

    // Act
    let result = LedgerStore::load(&fs);

    // Assert
    assert!(is_corruption(&result), "{result:?}");
}

#[test]
fn should_replace_file_with_snapshot_when_compacting() {
    // Arrange
    let fs = mock_fs();
    let ledger = genesis_ledger();
    let edits = [rotate(1), rotate(2), LedgerEdit::AdvanceLogStart { to: 3 }];
    let mut store = LedgerStore::create(Arc::clone(&fs), &ledger).expect("create LEDGER");
    for edit in &edits {
        store.append(edit).expect("append edit");
    }
    let compacted = applied(&ledger, &edits);

    // Act
    store.compact().expect("compact LEDGER");

    // Assert
    let mut expected = header();
    expected.extend(snapshot_frame(&compacted));
    assert_eq!(ledger_bytes(&fs), expected);
    assert_eq!(loaded(&fs), (compacted, LedgerTail::Clean));
}

#[test]
fn should_keep_later_appends_when_compacted_ledger_reloads() {
    // Arrange
    let fs = mock_fs();
    let ledger = genesis_ledger();
    let mut store = LedgerStore::create(Arc::clone(&fs), &ledger).expect("create LEDGER");
    store.append(&rotate(1)).expect("append edit");
    let compacted = applied(&ledger, &[rotate(1)]);
    store.compact().expect("compact LEDGER");

    // Act
    store.append(&rotate(2)).expect("append after compaction");
    store
        .append(&LedgerEdit::AdvanceLogStart { to: 2 })
        .expect("append after compaction");

    // Assert
    assert_eq!(
        loaded(&fs),
        (
            applied(
                &compacted,
                &[rotate(2), LedgerEdit::AdvanceLogStart { to: 2 }]
            ),
            LedgerTail::Clean
        )
    );
}

#[test]
fn should_drop_torn_tail_when_opening() {
    // Arrange
    let fs = mock_fs();
    let (bytes, _) = image_with(&[rotate(1), rotate(2)]);
    write_ledger_bytes(&fs, &bytes[..bytes.len() - 5]);
    let (torn, tail) = loaded(&fs);
    assert_eq!(tail, LedgerTail::Torn);

    // Act
    let mut store = LedgerStore::open(Arc::clone(&fs))
        .expect("open LEDGER")
        .expect("LEDGER exists");
    let replacement = LedgerEdit::SetAside { from: 2, open: 9 };
    store.append(&replacement).expect("append after torn tail");

    // Assert
    assert_eq!(
        loaded(&fs),
        (applied(&torn, &[replacement]), LedgerTail::Clean)
    );
}

#[test]
fn should_heal_before_appending_when_earlier_append_failed() {
    // Arrange
    let device = CrashDevice::new();
    let fs = crash_fs(&device);
    let ledger = genesis_ledger();
    let mut store = LedgerStore::create(Arc::clone(&fs), &ledger).expect("create LEDGER");
    let fault = device.inject(Fault {
        op: FaultOp::Sync,
        path: LEDGER_FILE.to_string(),
        nth: 1,
        effect: FaultEffect::Before,
        error: FaultError::Io,
    });
    let failed = store.append(&rotate(1));

    // Act
    let retried = store.append(&rotate(1));

    // Assert: the retry republished the ledger it knew, then appended.
    assert!(device.fired(fault));
    assert!(failed.is_err());
    assert!(retried.is_ok());
    let expected = applied(&ledger, &[rotate(1)]);
    for_each_power_loss(&device, |fs| {
        assert_eq!(loaded(fs), (expected.clone(), LedgerTail::Clean));
    });
}

#[test]
fn should_append_nothing_when_healing_fails() {
    // Arrange: the append's sync fails, then so does the heal's rename.
    let device = CrashDevice::new();
    let fs = crash_fs(&device);
    let ledger = genesis_ledger();
    let mut store = LedgerStore::create(Arc::clone(&fs), &ledger).expect("create LEDGER");
    device.inject(Fault {
        op: FaultOp::Sync,
        path: LEDGER_FILE.to_string(),
        nth: 1,
        effect: FaultEffect::Before,
        error: FaultError::Io,
    });
    assert!(store.append(&rotate(1)).is_err());
    device.inject(Fault {
        op: FaultOp::Rename,
        path: String::new(),
        nth: 1,
        effect: FaultEffect::Before,
        error: FaultError::Io,
    });

    // Act
    let retried = store.append(&rotate(2));

    // Assert: the refused edit is in no reachable state.
    assert!(retried.is_err());
    for_each_power_loss(&device, |fs| {
        let (state, _) = loaded(fs);
        assert!(
            state == ledger || state == applied(&ledger, &[rotate(1)]),
            "{state:?}"
        );
    });
}

#[test]
fn should_bound_ledger_size_when_many_edits_append() {
    // Arrange: far more edits than one compaction interval.
    let fs = mock_fs();
    let ledger = genesis_ledger();
    let mut store = LedgerStore::create(Arc::clone(&fs), &ledger).expect("create LEDGER");
    let mut expected = ledger.clone();
    let mut edits = Vec::new();
    for seal in 1..=u64::try_from(2 * COMPACT_AFTER_EDITS).expect("edit count") {
        edits.push(rotate(seal));
        edits.push(LedgerEdit::AdvanceLogStart { to: seal + 1 });
    }

    // Act
    for edit in &edits {
        store.append(edit).expect("append edit");
        expected.apply(edit).expect("edit applies");
    }

    // Assert: at most one interval of edits follows a snapshot.
    let largest_edit = edits.iter().map(|edit| edit_frame(edit).len()).max();
    let bound = header().len()
        + snapshot_frame(&expected).len()
        + COMPACT_AFTER_EDITS * largest_edit.expect("edits");
    assert!(ledger_bytes(&fs).len() <= bound);
    assert_eq!(loaded(&fs), (expected, LedgerTail::Clean));
}

#[test]
fn should_publish_snapshot_when_opening() {
    // Arrange
    let fs = mock_fs();
    let ledger = genesis_ledger();
    let mut store = LedgerStore::create(Arc::clone(&fs), &ledger).expect("create LEDGER");
    store.append(&rotate(1)).expect("append edit");
    store.append(&rotate(2)).expect("append edit");
    drop(store);

    // Act
    LedgerStore::open(Arc::clone(&fs))
        .expect("open LEDGER")
        .expect("LEDGER exists");

    // Assert
    let mut expected = header();
    expected.extend(snapshot_frame(&applied(&ledger, &[rotate(1), rotate(2)])));
    assert_eq!(ledger_bytes(&fs), expected);
}

#[test]
fn should_append_again_when_compaction_follows_failed_append() {
    // Arrange
    let device = CrashDevice::new();
    let fs = crash_fs(&device);
    let ledger = genesis_ledger();
    let mut store = LedgerStore::create(Arc::clone(&fs), &ledger).expect("create LEDGER");
    let fault = device.inject(Fault {
        op: FaultOp::Sync,
        path: LEDGER_FILE.to_string(),
        nth: 1,
        effect: FaultEffect::Before,
        error: FaultError::Io,
    });
    assert!(store.append(&rotate(1)).is_err());
    assert!(device.fired(fault));

    // Act
    store.compact().expect("compact LEDGER");
    store.append(&rotate(1)).expect("append after compaction");

    // Assert
    let expected = applied(&ledger, &[rotate(1)]);
    for_each_power_loss(&device, |fs| {
        assert_eq!(loaded(fs), (expected.clone(), LedgerTail::Clean));
    });
}

#[test]
fn should_leave_absent_or_created_ledger_when_power_fails_during_create() {
    // Arrange
    let device = CrashDevice::new();
    let ledger = genesis_ledger();
    let faults = interruptions(&[
        FaultOp::Stat,
        FaultOp::Open,
        FaultOp::Write,
        FaultOp::Sync,
        FaultOp::Rename,
        FaultOp::SyncDir,
    ]);
    let mut scenarios: Vec<Option<Fault>> = vec![None];
    scenarios.extend(faults.into_iter().map(Some));

    for scenario in scenarios {
        let fork = device.fork();
        let fault = scenario.clone().map(|fault| fork.inject(fault));

        // Act
        let created = LedgerStore::create(crash_fs(&fork), &ledger).is_ok();

        // Assert
        assert!(fault.is_none_or(|fault| fork.fired(fault)), "{scenario:?}");
        for_each_power_loss(&fork, |fs| match LedgerStore::load(fs) {
            Ok(None) => assert!(!created, "{scenario:?}: acknowledged create was lost"),
            Ok(Some(state)) => assert_eq!(state, (ledger.clone(), LedgerTail::Clean)),
            Err(error) => panic!("{scenario:?}: unreadable ledger: {error}"),
        });
    }
}

#[test]
fn should_keep_ledger_when_power_fails_during_compact() {
    // Arrange
    let device = CrashDevice::new();
    let ledger = genesis_ledger();
    let edits = [rotate(1), rotate(2), rotate(3)];
    let mut store = LedgerStore::create(crash_fs(&device), &ledger).expect("create LEDGER");
    for edit in &edits {
        store.append(edit).expect("append edit");
    }
    drop(store);
    let kept = applied(&ledger, &edits);
    let faults = interruptions(&[
        FaultOp::Open,
        FaultOp::Write,
        FaultOp::Sync,
        FaultOp::Rename,
        FaultOp::SyncDir,
    ]);
    let mut scenarios: Vec<Option<Fault>> = vec![None];
    scenarios.extend(faults.into_iter().map(Some));

    for scenario in scenarios {
        let fork = device.fork();
        let mut store = LedgerStore::open(crash_fs(&fork))
            .expect("open LEDGER")
            .expect("LEDGER exists");
        let fault = scenario.clone().map(|fault| fork.inject(fault));

        // Act
        let _ = store.compact();

        // Assert: compaction never changes what the ledger holds.
        assert!(fault.is_none_or(|fault| fork.fired(fault)), "{scenario:?}");
        for_each_power_loss(&fork, |fs| {
            let state = LedgerStore::load(fs)
                .unwrap_or_else(|error| panic!("{scenario:?}: unreadable ledger: {error}"))
                .unwrap_or_else(|| panic!("{scenario:?}: LEDGER vanished"));
            assert_eq!(state, (kept.clone(), LedgerTail::Clean), "{scenario:?}");
        });
    }
}

#[test]
fn should_leave_old_or_new_ledger_when_power_fails_during_append() {
    // Arrange
    let device = CrashDevice::new();
    let ledger = genesis_ledger();
    let mut store = LedgerStore::create(crash_fs(&device), &ledger).expect("create LEDGER");
    store.append(&rotate(1)).expect("append edit");
    drop(store);
    let old = applied(&ledger, &[rotate(1)]);
    let new = applied(&old, &[rotate(2)]);
    let faults = interruptions(&[FaultOp::Open, FaultOp::Stat, FaultOp::Write, FaultOp::Sync]);
    let mut scenarios: Vec<Option<Fault>> = vec![None];
    scenarios.extend(faults.into_iter().map(Some));

    for scenario in scenarios {
        let fork = device.fork();
        let mut store = LedgerStore::open(crash_fs(&fork))
            .expect("open LEDGER")
            .expect("LEDGER exists");
        let fault = scenario.clone().map(|fault| fork.inject(fault));

        // Act
        let appended = store.append(&rotate(2)).is_ok();

        // Assert
        assert!(fault.is_none_or(|fault| fork.fired(fault)), "{scenario:?}");
        for_each_power_loss(&fork, |fs| {
            let (state, _) = LedgerStore::load(fs)
                .unwrap_or_else(|error| panic!("{scenario:?}: unreadable ledger: {error}"))
                .unwrap_or_else(|| panic!("{scenario:?}: LEDGER vanished"));
            if appended {
                assert_eq!(state, new, "{scenario:?}: acknowledged append was lost");
            } else {
                assert!(state == old || state == new, "{scenario:?}: {state:?}");
            }
        });
    }
}

#[test]
fn should_keep_appended_edit_when_create_was_interrupted() {
    // Arrange: the rename that published LEDGER is visible but not durable
    // when the creating process dies.
    let device = CrashDevice::new();
    let ledger = genesis_ledger();
    let fault = device.inject(Fault {
        op: FaultOp::SyncDir,
        path: String::new(),
        nth: 1,
        effect: FaultEffect::Before,
        error: FaultError::Io,
    });
    assert!(LedgerStore::create(crash_fs(&device), &ledger).is_err());
    assert!(device.fired(fault));
    device.process_crash();
    let fs = crash_fs(&device);
    assert_eq!(loaded(&fs), (ledger.clone(), LedgerTail::Clean));

    // Act
    let mut store = LedgerStore::open(Arc::clone(&fs))
        .expect("open LEDGER")
        .expect("LEDGER exists");
    store.append(&rotate(1)).expect("append edit");

    // Assert
    let expected = applied(&ledger, &[rotate(1)]);
    for_each_power_loss(&device, |fs| {
        assert_eq!(loaded(fs), (expected.clone(), LedgerTail::Clean));
    });
}

#[test]
fn should_keep_loaded_state_when_power_fails_after_open() {
    // Arrange: an append whose sync failed is visible to the next process
    // even though its bytes never reached the device.
    let device = CrashDevice::new();
    let ledger = genesis_ledger();
    let mut store = LedgerStore::create(crash_fs(&device), &ledger).expect("create LEDGER");
    let fault = device.inject(Fault {
        op: FaultOp::Sync,
        path: LEDGER_FILE.to_string(),
        nth: 1,
        effect: FaultEffect::Before,
        error: FaultError::Io,
    });
    assert!(store.append(&rotate(1)).is_err());
    assert!(device.fired(fault));
    drop(store);
    device.process_crash();
    let fs = crash_fs(&device);
    let seen = loaded(&fs);

    // Act
    LedgerStore::open(Arc::clone(&fs))
        .expect("open LEDGER")
        .expect("LEDGER exists");

    // Assert
    for_each_power_loss(&device, |fs| {
        assert_eq!(loaded(fs), seen);
    });
}
