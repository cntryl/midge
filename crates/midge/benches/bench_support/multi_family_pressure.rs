use cntryl_midge::{
    Engine, MidgeError, MidgeResult, OpenOptions, Query, TransactionMode, WriteOptions,
};
use cntryl_stress::{LogicalUnit, OperationOutcome, ProgressHandle, StressContext};
use std::sync::Barrier;
use std::thread;
use std::time::{Duration, Instant};

const FAMILY_COUNTS: [u32; 3] = [1, 4, 16];
const VALUE_BYTES: usize = 16 * 1024;
const KEYS: [&[u8]; 2] = [b"first", b"second"];

#[derive(Default)]
struct Writes {
    acknowledged: u64,
    stalls: u64,
    resource_limits: u64,
}

pub(super) fn run(ctx: &mut StressContext, duration: Duration) {
    let progress = ctx.progress_handle();
    let stage_budget = duration / u32::try_from(FAMILY_COUNTS.len()).unwrap();
    for hot_families in FAMILY_COUNTS {
        run_stage(ctx, &progress, hot_families, stage_budget).expect("multi-family pressure stage");
    }
}

fn run_stage(
    ctx: &mut StressContext,
    progress: &ProgressHandle,
    hot_families: u32,
    budget: Duration,
) -> MidgeResult<()> {
    let directory = tempfile::tempdir()?;
    let options = OpenOptions::local(directory.path())
        .with_memtable_flush_threshold(512 * 1024)
        .build()?;
    let mut engine = Engine::open(options.clone())?;
    let families = (0..=hot_families)
        .map(|index| engine.create_column_family(&format!("family-{index}")))
        .collect::<MidgeResult<Vec<_>>>()?;
    let cold = families[0].id();
    for family in &families {
        write_pair(&engine, family.id(), 0)?;
        engine.flush_cf(family)?;
        progress.advance();
    }

    let start = Barrier::new(families.len() * 2 - 1);
    let engine_ref = &engine;
    let start_ref = &start;
    let started = Instant::now();
    let results = thread::scope(|scope| {
        let mut writers = Vec::new();
        let mut readers = Vec::new();
        for family in &families[1..] {
            let id = family.id();
            writers.push(scope.spawn(move || {
                start_ref.wait();
                write_until(engine_ref, id, started + budget, progress)
            }));
        }
        for family in &families {
            let id = family.id();
            readers.push(scope.spawn(move || {
                start_ref.wait();
                read_until(engine_ref, id, id == cold, started + budget, progress)
            }));
        }
        let commit_stats = writers
            .into_iter()
            .map(|writer| writer.join().expect("writer thread"))
            .collect::<MidgeResult<Vec<_>>>()?;
        let snapshot_counts = readers
            .into_iter()
            .map(|reader| reader.join().expect("reader thread"))
            .collect::<MidgeResult<Vec<_>>>()?;
        Ok::<_, MidgeError>((commit_stats, snapshot_counts))
    })?;
    let elapsed = started.elapsed();
    let (writes, reads) = results;
    assert!(writes.iter().all(|write| write.acknowledged > 0));
    assert!(reads.iter().all(|&count| count > 0));

    engine.shutdown(Duration::from_secs(45))?;
    drop(engine);
    let reopened = Engine::open(options)?;
    assert_eq!(read_pair(&reopened, cold)?, 0);
    for (family, write) in families[1..].iter().zip(&writes) {
        assert_eq!(read_pair(&reopened, family.id())?, write.acknowledged);
        progress.advance();
    }
    let acknowledged = writes.iter().map(|write| write.acknowledged).sum();
    ctx.record_external_outcome(
        format!("hot-cold/{hot_families}-hot-families"),
        elapsed,
        LogicalUnit::new("write transaction"),
        OperationOutcome {
            attempted: acknowledged,
            completed: acknowledged,
            ..OperationOutcome::default()
        },
    );
    ctx.parameter("hot_families", hot_families)
        .parameter("read_transactions", reads.iter().sum::<u64>())
        .parameter("cold_read_transactions", reads[0])
        .parameter(
            "write_stalls",
            writes.iter().map(|write| write.stalls).sum::<u64>(),
        )
        .parameter(
            "resource_limits",
            writes
                .iter()
                .map(|write| write.resource_limits)
                .sum::<u64>(),
        );
    Ok(())
}

fn write_until(
    engine: &Engine,
    family: u32,
    deadline: Instant,
    progress: &ProgressHandle,
) -> MidgeResult<Writes> {
    let mut writes = Writes::default();
    while Instant::now() < deadline {
        match write_pair(engine, family, writes.acknowledged + 1) {
            Ok(()) => {
                writes.acknowledged += 1;
                progress.advance();
                continue;
            }
            Err(MidgeError::WriteStall(_)) => writes.stalls += 1,
            Err(MidgeError::ResourceLimit(_)) => writes.resource_limits += 1,
            Err(error) => return Err(error),
        }
        thread::sleep(Duration::from_millis(1));
    }
    Ok(writes)
}

fn read_until(
    engine: &Engine,
    family: u32,
    cold: bool,
    deadline: Instant,
    progress: &ProgressHandle,
) -> MidgeResult<u64> {
    let mut reads = 0;
    let mut previous = 0;
    while Instant::now() < deadline {
        let observed = read_pair(engine, family)?;
        assert!(observed >= previous, "family {family} moved backwards");
        if cold {
            assert_eq!(observed, 0, "cold family changed during hot writes");
        }
        previous = observed;
        reads += 1;
        progress.advance();
    }
    Ok(reads)
}

fn write_pair(engine: &Engine, family: u32, generation: u64) -> MidgeResult<()> {
    let mut value = vec![0; VALUE_BYTES];
    value[..4].copy_from_slice(&family.to_be_bytes());
    value[4..12].copy_from_slice(&generation.to_be_bytes());
    let mut tx = engine.begin_tx(family, TransactionMode::ReadWrite)?;
    for key in KEYS {
        tx.put(key.to_vec(), value.clone(), None)?;
    }
    tx.commit(WriteOptions::sync())
}

fn read_pair(engine: &Engine, family: u32) -> MidgeResult<u64> {
    let tx = engine.begin_tx(family, TransactionMode::ReadOnly)?;
    let first = tx.get(KEYS[0])?.expect("first seeded row");
    let second = tx.get(KEYS[1])?.expect("second seeded row");
    assert_eq!(first, second, "family {family} exposed a partial write");
    assert_eq!(first.len(), VALUE_BYTES);
    assert_eq!(first[..4], family.to_be_bytes(), "cross-family value");
    let query = Query::new();
    let mut scan = tx.scan(&query)?;
    for key in KEYS {
        let (scan_key, scan_value) = scan.next().expect("seeded scan row")?;
        assert_eq!(scan_key.as_ref(), key);
        assert_eq!(scan_value, first, "scan disagrees with point read");
    }
    assert!(scan.next().transpose()?.is_none(), "unexpected scan row");
    Ok(u64::from_be_bytes(first[4..12].try_into().unwrap()))
}
