//! The opt-in amortized WAL sync floor (#904) keeps acknowledged writes and
//! records the floor exactly wherever a segment's history is closed.

use cntryl_midge::__internal::io::{
    Durability, Fs, FsPath, MockFs, OpenMode, OpenOptions as FileOptions, ReadFs,
};
use cntryl_midge::__internal::simulation::{CrashDevice, SimulatedClock};
use cntryl_midge::__internal::wal::frame::{decode_frame_header, WAL_FRAME_HEADER_LEN};
use cntryl_midge::__internal::wal::segment::HEADER_LEN;
use cntryl_midge::{
    AmortizedSyncFloor, Engine, MidgeError, OpenOptions, RecoveryPolicy, TransactionMode,
    WriteOptions,
};
use std::sync::Arc;
use std::time::Duration;

/// A window far larger than the test writes, so only an exact edit moves it.
fn wide_window() -> AmortizedSyncFloor {
    AmortizedSyncFloor {
        max_unrecorded_bytes: 64 * 1024 * 1024,
        max_unrecorded_delay: Duration::from_secs(10),
    }
}

struct Store {
    directory: tempfile::TempDir,
    fs: Arc<MockFs>,
}

impl Store {
    fn new() -> Self {
        Self {
            directory: tempfile::tempdir().expect("database name"),
            fs: Arc::new(MockFs::new()),
        }
    }

    fn options(&self, floor: Option<AmortizedSyncFloor>) -> OpenOptions {
        let mut builder = OpenOptions::local(self.directory.path())
            .background_compaction(false)
            .filesystem_for_testing(self.fs.clone());
        if let Some(floor) = floor {
            builder = builder.amortized_sync_floor(floor);
        }
        builder.build().expect("options")
    }

    fn open_wal_path(&self) -> FsPath {
        let id = self
            .fs
            .list_dir(&FsPath::new("wal"))
            .expect("WAL directory")
            .iter()
            .filter_map(|entry| cntryl_midge::__internal::wal::parse_segment_file_name(&entry.name))
            .max()
            .expect("open WAL");
        FsPath::new(format!(
            "wal/{}",
            cntryl_midge::__internal::wal::segment_file_name(id)
        ))
    }

    fn wal_bytes(&self) -> Vec<u8> {
        let file = self.fs.open_read(&self.open_wal_path()).expect("read WAL");
        file.read_at(0, file.len().expect("WAL length"))
            .expect("WAL bytes")
            .to_vec()
    }

    fn shorten(&self, end: usize) {
        let mut file = self
            .fs
            .open(
                &self.open_wal_path(),
                FileOptions {
                    mode: OpenMode::ReadWrite,
                    create: false,
                    create_new: false,
                    truncate: false,
                },
            )
            .expect("open WAL");
        file.truncate(end as u64).expect("lose suffix");
        file.sync(Durability::Durable).expect("persist damage");
    }
}

/// The end of the first frame of the open segment.
fn first_frame_end(bytes: &[u8]) -> usize {
    let (payload, _) = decode_frame_header(&bytes[HEADER_LEN..HEADER_LEN + WAL_FRAME_HEADER_LEN])
        .expect("first frame");
    HEADER_LEN + WAL_FRAME_HEADER_LEN + payload
}

fn commit_sync(engine: &Engine, key: &[u8]) {
    let cf = engine.get_column_family("default").expect("default");
    let mut tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadWrite)
        .expect("transaction");
    tx.put(key.to_vec(), b"acknowledged".to_vec(), None)
        .expect("put");
    tx.commit(WriteOptions::sync()).expect("sync commit");
}

#[test]
fn should_record_exact_floor_when_shutting_down_cleanly_with_an_open_window() {
    // Arrange: the second commit stays inside the unrecorded window.
    let store = Store::new();
    let options = store.options(Some(wide_window()));
    let mut engine = Engine::open(options).expect("open");
    commit_sync(&engine, b"first");
    let before = engine
        .metrics()
        .get_runtime_metrics()
        .expect("metrics")
        .wal_ledger_file_syncs;
    commit_sync(&engine, b"later");
    let after = engine
        .metrics()
        .get_runtime_metrics()
        .expect("metrics")
        .wal_ledger_file_syncs;
    assert_eq!(after, before, "the window must be open before shutdown");

    // Act
    engine.shutdown(Duration::from_secs(5)).expect("shutdown");
    drop(engine);
    let bytes = store.wal_bytes();
    let end = first_frame_end(&bytes) + 3;
    assert!(bytes.len() > end);
    store.shorten(end);

    // Assert: shutdown recorded the exact end, so losing acknowledged bytes
    // below it fails a strict open exactly as without the option.
    let error = Engine::open(store.options(None))
        .err()
        .expect("strict recovery rejects acknowledged loss");
    assert!(matches!(error, MidgeError::Corruption(_)), "{error:?}");
}

const LEASE_TTL: Duration = Duration::from_secs(1);

/// A database on a crash-simulating device: the process dies without a clean
/// shutdown, so only the floor the running engine had recorded survives.
struct Crashed {
    named: tempfile::TempDir,
    device: CrashDevice,
    clock: SimulatedClock,
}

impl Crashed {
    /// Run strict commits of `keys`, then `buffered` as a buffered tail, then
    /// kill the process. `floor` is the option the dying engine ran with.
    fn run(floor: Option<AmortizedSyncFloor>, keys: &[&[u8]], buffered: Option<&[u8]>) -> Self {
        let crashed = Self {
            named: tempfile::tempdir().expect("named directory"),
            device: CrashDevice::new(),
            clock: SimulatedClock::new(),
        };
        let engine = Engine::open(crashed.options(&crashed.device, floor, RecoveryPolicy::Strict))
            .expect("open");
        for key in keys {
            commit_sync(&engine, key);
        }
        if let Some(key) = buffered {
            let cf = engine.get_column_family("default").expect("default");
            let mut tx = engine
                .begin_tx(cf.id(), TransactionMode::ReadWrite)
                .expect("transaction");
            tx.put(key.to_vec(), b"buffered".to_vec(), None)
                .expect("put");
            tx.commit(WriteOptions::buffered()).expect("buffered");
        }
        crashed.device.process_crash();
        drop(engine);
        // A restarted process's clock runs past the dead one's lease.
        crashed.clock.advance(LEASE_TTL * 2);
        crashed
    }

    fn options(
        &self,
        device: &CrashDevice,
        floor: Option<AmortizedSyncFloor>,
        policy: RecoveryPolicy,
    ) -> OpenOptions {
        let root: Arc<dyn Fs> = Arc::new(device.fs());
        let mut builder = OpenOptions::local(self.named.path())
            .background_compaction(false)
            .recovery_policy(policy)
            .lease_ttl(LEASE_TTL)
            .lease_clock_skew_tolerance(Duration::ZERO)
            .lease_clock_for_testing(Arc::new(self.clock.clone()))
            .filesystem_for_testing(root);
        if let Some(floor) = floor {
            builder = builder.amortized_sync_floor(floor);
        }
        builder.build().expect("open options")
    }

    /// Every power-loss outcome of the crash image, as an independent device.
    fn power_losses(&self) -> Vec<CrashDevice> {
        let decisions = self.device.decisions();
        let outcomes: usize = decisions.iter().map(|decision| decision.options).product();
        assert!((1..=256).contains(&outcomes), "{decisions:?}");
        (0..outcomes)
            .map(|mut index| {
                let choices: Vec<usize> = decisions
                    .iter()
                    .map(|decision| {
                        let choice = index % decision.options;
                        index /= decision.options;
                        choice
                    })
                    .collect();
                let fork = self.device.fork();
                fork.power_loss_with(|decision| {
                    choices[decisions
                        .iter()
                        .position(|known| known == decision)
                        .expect("decision is enumerated")]
                });
                fork
            })
            .collect()
    }
}

fn read_value(engine: &Engine, key: &[u8]) -> Option<Vec<u8>> {
    let cf = engine.get_column_family("default").expect("default");
    let tx = engine
        .begin_tx(cf.id(), TransactionMode::ReadOnly)
        .expect("read");
    tx.get(key).expect("get").map(|value| value.to_vec())
}

/// Flip one byte of the first frame of the open segment on `device`.
fn damage_first_frame(device: &CrashDevice) {
    let fs = device.fs();
    let id = fs
        .list_dir(&FsPath::new("wal"))
        .expect("WAL directory")
        .iter()
        .filter_map(|entry| cntryl_midge::__internal::wal::parse_segment_file_name(&entry.name))
        .max()
        .expect("open WAL");
    let path = FsPath::new(format!(
        "wal/{}",
        cntryl_midge::__internal::wal::segment_file_name(id)
    ));
    let at = (HEADER_LEN + WAL_FRAME_HEADER_LEN) as u64;
    let mut file = fs
        .open(
            &path,
            FileOptions {
                mode: OpenMode::ReadWrite,
                create: false,
                create_new: false,
                truncate: false,
            },
        )
        .expect("open WAL");
    let byte = file.read_at(at, 1).expect("frame byte")[0];
    file.write_at(at, bytes::Bytes::from(vec![byte ^ 0xff]))
        .expect("damage frame");
    file.sync(Durability::Durable).expect("persist damage");
}

#[test]
fn should_replay_intact_tail_above_amortized_floor_after_crash() {
    // Arrange: acknowledged strict commits sit above the recorded floor, and
    // a buffered tail makes the device's power-loss outcomes non-trivial.
    let crashed = Crashed::run(Some(wide_window()), &[b"first", b"later"], Some(b"tail"));

    // Act / Assert: whichever way power is lost, a strict open succeeds and
    // every acknowledged write replays. The floor lags; the bytes do not.
    for fork in crashed.power_losses() {
        let options = crashed.options(&fork, Some(wide_window()), RecoveryPolicy::Strict);
        let mut reopened = Engine::open(options).expect("strict open above the floor");
        for key in [b"first".as_slice(), b"later"] {
            assert_eq!(
                read_value(&reopened, key).as_deref(),
                Some(&b"acknowledged"[..])
            );
        }
        reopened.shutdown(Duration::from_secs(5)).expect("shutdown");
    }
}

#[test]
fn should_open_and_drop_tail_when_damage_is_inside_floor_window() {
    // Arrange: damage lands in the first acknowledged frame, which the
    // amortized floor had not yet recorded.
    let crashed = Crashed::run(Some(wide_window()), &[b"first", b"later"], None);
    let damaged = crashed.device.fork();
    damaged.power_loss_with(|_| 0);
    damage_first_frame(&damaged);

    // Act: damage inside the window is a torn tail, truncated at the verified
    // prefix, and with it every later frame (the documented window loss).
    let mut reopened =
        Engine::open(crashed.options(&damaged, Some(wide_window()), RecoveryPolicy::Strict))
            .expect("strict open drops the damaged window");

    // Assert
    assert_eq!(read_value(&reopened, b"first"), None);
    assert_eq!(read_value(&reopened, b"later"), None);
    commit_sync(&reopened, b"after");
    assert_eq!(
        read_value(&reopened, b"after").as_deref(),
        Some(&b"acknowledged"[..])
    );
    reopened.shutdown(Duration::from_secs(5)).expect("shutdown");
}

#[test]
fn should_fail_strict_when_damaged_acknowledged_frame_is_below_the_exact_floor() {
    // Arrange: the same crash and damage, but the dying engine recorded every
    // strict sync exactly (the default).
    let crashed = Crashed::run(None, &[b"first", b"later"], None);
    let damaged = crashed.device.fork();
    damaged.power_loss_with(|_| 0);
    damage_first_frame(&damaged);

    // Act
    let result = Engine::open(crashed.options(&damaged, None, RecoveryPolicy::Strict));

    // Assert: the control. Exact floors keep detecting this loss.
    let error = result.err().expect("strict recovery rejects the damage");
    assert!(matches!(error, MidgeError::Corruption(_)), "{error:?}");
}

#[test]
fn should_keep_exact_floor_after_salvage_when_amortized() {
    // Arrange: damage inside an unrecorded window, then a salvage open that
    // keeps running under the option and commits again.
    let crashed = Crashed::run(Some(wide_window()), &[b"first", b"later"], None);
    let damaged = crashed.device.fork();
    damaged.power_loss_with(|_| 0);
    damage_first_frame(&damaged);
    let mut salvaged =
        Engine::open(crashed.options(&damaged, Some(wide_window()), RecoveryPolicy::Salvage))
            .expect("salvage open");
    commit_sync(&salvaged, b"next");

    // Act: a clean shutdown closes the new window exactly.
    salvaged.shutdown(Duration::from_secs(5)).expect("shutdown");
    drop(salvaged);

    // Assert: a strict reopen finds the salvaged state and the new commit.
    let mut reopened =
        Engine::open(crashed.options(&damaged, Some(wide_window()), RecoveryPolicy::Strict))
            .expect("strict reopen after salvage");
    assert_eq!(
        read_value(&reopened, b"next").as_deref(),
        Some(&b"acknowledged"[..])
    );
    reopened.shutdown(Duration::from_secs(5)).expect("shutdown");
}
