//! Tier 3 â€” Flush cost (single primitive operation)
//!
//! Measures: cost of a single flush call
//! Subsystem: flush orchestration (engine/runtime boundary).
//! Storage semantics: tests run for durable backends (local/cloud) explicitly.
//!
//! What is *not* tested here:
//! - Compaction throughput or cascading (Tier-4)
//! - Long-running compaction loops (Tier-4)
//! - Throughput under sustained load (Tier-4)
//! - Cache warmup effects, sampling, or tuning (Tier-4)
//! - Complex overlap patterns or many-file cases (Tier-4)
//!
//! All setup strictly outside measurement; measured body is a single flush call.

#[path = "./stress_config.rs"]
mod stress_config;

use cntryl_stress::{stress, stress_main, StressContext};
#[allow(unused_imports)]
use stress_config::{BenchConfig, MidgeStressContextExt as _};

use cntryl_midge::{testkit::MidgeOptions, MidgeEngine};

const KEY_SIZE: usize = cntryl_midge::testkit::stress::KEY_SIZE;
const DEFAULT_VALUE_SIZE: usize = 100;
const TARGET_BATCH: usize = 1_000;
const DEFAULT_COMPACTION_KEYS: usize = 1_000;
const LOCAL_FLUSH_REPEATS: usize = 65_536;
const LOCAL_FLUSH_REPEAT_OPS: u64 = 65_536;
const CLOUD_FLUSH_REPEATS: usize = 1024;
const CLOUD_FLUSH_REPEAT_OPS: u64 = 1024;

fn precompute_kv(num_keys: usize, value_size: usize) -> (Vec<[u8; KEY_SIZE]>, Vec<Vec<u8>>) {
    cntryl_midge::testkit::stress::precompute_kv16_u64_be(num_keys, value_size, u8::MAX)
}

fn setup_engine(opts: MidgeOptions) -> MidgeEngine {
    cntryl_midge::testkit::stress::open_engine_no_compaction(opts)
}

fn run_flush_case(
    ctx: &mut StressContext,
    scenario: &'static str,
    opts: MidgeOptions,
    num_keys: usize,
    value_size: usize,
    flush_repeats: usize,
    flush_repeat_ops: u64,
) {
    let (keys, values) = precompute_kv(num_keys, value_size);

    let engine = setup_engine(opts);
    let cf = engine.create_column_family("cf1").unwrap();

    // All setup outside measurement â€” write in TARGET_BATCH-sized transactions
    let cf_id = cf.id();
    let write_opts = cntryl_midge::WriteOptions::best_effort(); // Fast setup: skip WAL I/O
    let total = keys.len();
    for start in (0..total).step_by(TARGET_BATCH) {
        let end = (start + TARGET_BATCH).min(total);
        let mut tx = engine
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
            .expect("begin");
        for i in start..end {
            tx.put(keys[i].to_vec(), values[i].clone(), None)
                .expect("setup put");
        }
        tx.commit(write_opts).expect("commit");
    }

    // Ensure durability before measurement
    engine.flush_cf(&cf).expect("setup flush");

    ctx.parameter("flush_repeats", flush_repeats);

    // Measure ONLY flush calls. Keep the repeat count bounded; unbounded
    // simulated-cloud flush loops can hit durable-capacity write stalls.
    stress_config::measure_external(ctx, scenario, flush_repeat_ops, || {
        for _ in 0..flush_repeats {
            engine.flush_cf(&cf).expect("flush failed");
        }
    });

    drop(engine);
}

// TIER 4 ONLY: compact_all() with complex setup patterns
// Moved to tier4_system_compaction_throughput.rs

// Reason: compact_all() cost depends heavily on keyspace overlap, file count,
// and staged recovery. This is SYSTEM BEHAVIOR under varying conditions, not
// a constant-time primitive cost. Tier 4 will measure cascading compaction
// throughput, multi-level overlaps, and degradation curves.

// ---------------------------------------------------------------------------
// Stress tests (explicit, one datapoint per test)
// ---------------------------------------------------------------------------

#[stress(tier = 3)]
fn tier3_compaction_flush_local(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("local");
    run_flush_case(
        ctx,
        "tier3_compaction_flush_local",
        opts,
        DEFAULT_COMPACTION_KEYS,
        DEFAULT_VALUE_SIZE,
        LOCAL_FLUSH_REPEATS,
        LOCAL_FLUSH_REPEAT_OPS,
    );
}

#[stress(tier = 3)]
fn tier3_compaction_flush_cloud(ctx: &mut StressContext) {
    let opts = cntryl_midge::testkit::opts_for_mode("cloud");
    run_flush_case(
        ctx,
        "tier3_compaction_flush_cloud",
        opts,
        DEFAULT_COMPACTION_KEYS,
        DEFAULT_VALUE_SIZE,
        CLOUD_FLUSH_REPEATS,
        CLOUD_FLUSH_REPEAT_OPS,
    );
}

stress_main!();
