//! Real public-Engine capture/recovery; no fabricated durable objects.
use cntryl_midge::__internal::recovery::RecoveryProbeVariant;
use cntryl_midge::{
    BackupManifest, Engine, MemoryBudget, OpenOptions, Query, TransactionMode, WriteOptions,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::field::{Field, Visit};
use tracing_subscriber::{layer::Context, Layer};

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const FAMILIES: [&str; 2] = ["covered", "unflushed"];

#[derive(Clone, Default)]
pub struct Capture(pub Arc<Mutex<Vec<Value>>>);
impl Capture {
    pub fn drain(&self) -> Vec<Value> {
        std::mem::take(&mut *self.0.lock().expect("capture lock"))
    }
}
#[derive(Default)]
struct Fields(serde_json::Map<String, Value>);
impl Visit for Fields {
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.insert(field.name().into(), json!(value));
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.0.insert(field.name().into(), json!(value));
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().into(), json!(value));
    }
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        let text = format!("{value:?}");
        self.0.insert(
            field.name().into(),
            if field.name() == "attribution" {
                serde_json::from_str(&text).expect("native attribution JSON")
            } else {
                json!(text)
            },
        );
    }
}
impl<S: tracing::Subscriber> Layer<S> for Capture {
    fn on_event(&self, event: &tracing::Event<'_>, _: Context<'_, S>) {
        if event.metadata().target() == "midge::recovery" {
            let mut fields = Fields::default();
            event.record(&mut fields);
            self.0
                .lock()
                .expect("capture lock")
                .push(Value::Object(fields.0));
        }
    }
}

fn options(root: &Path, memtable: usize, variant: RecoveryProbeVariant) -> Result<OpenOptions> {
    Ok(
        OpenOptions::cloud_simulated(root, "fixture-bucket", "fixture-prefix")
            .memory_budget(MemoryBudget::Bytes(128 * 1024 * 1024))
            .local_storage_budget(1024 * 1024 * 1024)
            .with_memtable_size_limit(memtable)
            .with_memtable_flush_threshold(memtable)
            .background_compaction(false)
            .target_sst_size_for_testing(128 * 1024)
            .recovery_probe_variant_for_testing(variant)
            .open_timeout(Duration::from_secs(30))
            .build()?,
    )
}
fn key(row: usize) -> Vec<u8> {
    format!("part-{:02}/row-{row:08}", row % 16).into_bytes()
}
fn value(family: usize, row: usize) -> Vec<u8> {
    let mut state = u64::try_from(row)
        .expect("bounded row")
        .wrapping_add(1)
        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
        ^ u64::try_from(family + 1).expect("bounded family");
    let mut bytes = Vec::with_capacity(512);
    for _ in 0..64 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        bytes.extend_from_slice(&state.to_le_bytes());
    }
    bytes
}

pub fn create(root: &Path, rows: usize) -> Result<Value> {
    if rows == 0 || rows > 8192 || root.exists() {
        return Err("invalid or occupied fixture target".into());
    }
    std::fs::create_dir_all(root)?;
    let scenario = fail::FailScenario::setup();
    fail::cfg("midge::cloud::defer_wal_prune_admission", "return")?;
    let mut engine = Engine::open(options(
        &root.join("source"),
        16 * 1024 * 1024,
        RecoveryProbeVariant::Baseline,
    )?)?;
    let families = [
        engine.create_column_family(FAMILIES[0])?,
        engine.create_column_family(FAMILIES[1])?,
    ];
    for first in (0..rows).step_by(128) {
        for (family, cf) in families.iter().enumerate() {
            let mut tx = engine.begin_tx(cf.id(), TransactionMode::ReadWrite)?;
            for row in first..(first + 128).min(rows) {
                tx.put(key(row), value(family, row), None)?;
            }
            tx.commit(WriteOptions::cloud_strict())?;
        }
        if (first + 128) % 2048 == 0 {
            engine.flush_cf(&families[0])?;
        }
    }
    engine.flush_cf(&families[0])?;
    let committed_wal_frontier = engine
        .metrics()
        .get_runtime_metrics()?
        .wal_cloud_durable_seq;
    engine.compact_all()?;
    let metrics = engine.metrics().get_runtime_metrics()?;
    if rows == 8192 && metrics.sst_count < 8 {
        return Err("fixture did not produce multiple partitioned SSTs".into());
    }
    let capture_started = Instant::now();
    let mut capture_attempts = Vec::new();
    let manifest = loop {
        let remaining = Duration::from_secs(30).saturating_sub(capture_started.elapsed());
        if remaining.is_zero() {
            return Err("fixed backup capture bound exceeded".into());
        }
        match engine.backup_to(root.join("backup"), remaining) {
            Ok(manifest) => {
                capture_attempts.push(json!({"outcome":"success"}));
                break manifest;
            }
            Err(error) => {
                let retryable = matches!(&error, cntryl_midge::MidgeError::Busy(_))
                    || matches!(&error, cntryl_midge::MidgeError::Io(error) if error.kind()==std::io::ErrorKind::NotFound);
                capture_attempts.push(
                    json!({"outcome":"failure","error":error.to_string(),"retryable":retryable}),
                );
                std::fs::write(
                    root.join("capture-attempts.json"),
                    serde_json::to_vec_pretty(&capture_attempts)?,
                )?;
                if !retryable || capture_attempts.len() >= 3 {
                    return Err(format!("backup capture: {error}").into());
                }
                std::thread::sleep(Duration::from_millis(100).min(remaining));
            }
        }
    };
    engine.shutdown(Duration::from_secs(30))?;
    drop(engine);
    scenario.teardown();
    let facts = json!({"rows_per_family":rows,"value_bytes":512,"seed_sst_count":metrics.sst_count,"frontier":manifest.durability_frontier,"committed_wal_frontier":committed_wal_frontier,"capture_attempts":capture_attempts,"backup":manifest,"inventory":inventory(&root.join("backup"),None)?});
    std::fs::write(
        root.join("fixture.json"),
        serde_json::to_vec_pretty(&facts)?,
    )?;
    Ok(facts)
}

pub fn inventory(artifact: &Path, restored: Option<&Path>) -> Result<Value> {
    let bytes = std::fs::read(artifact.join("backup.json"))?;
    let manifest: BackupManifest = serde_json::from_slice(&bytes)?;
    let mut seal = Sha256::new();
    seal.update(&bytes);
    let mut rows = Vec::new();
    for object in manifest.objects {
        let path = restored.map_or_else(
            || artifact.join("objects").join(&object.path),
            |root| root.join(&object.path),
        );
        let mut file = std::fs::File::open(path)?;
        let mut hash = Sha256::new();
        let mut buffer = vec![0u8; 64 * 1024];
        let mut size = 0u64;
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hash.update(&buffer[..read]);
            size += u64::try_from(read)?;
        }
        if size != object.size_bytes {
            return Err("fixture object length changed".into());
        }
        let digest = hex::encode(hash.finalize());
        seal.update(object.path.as_bytes());
        seal.update(digest.as_bytes());
        rows.push(json!({"path":object.path,"bytes":size,"sha256":digest}));
    }
    Ok(json!({"sha256":hex::encode(seal.finalize()),"objects":rows}))
}

fn verify(engine: &Engine, rows: usize, points: bool, label: &str) -> Result<Vec<Value>> {
    let mut checks = Vec::new();
    for (family, name) in FAMILIES.iter().enumerate() {
        let cf = engine
            .get_column_family(name)
            .ok_or("missing recovered family")?;
        let tx = engine.begin_tx(cf.id(), TransactionMode::ReadOnly)?;
        if points {
            for row in 0..rows {
                if tx.get(&key(row))?.as_deref() != Some(value(family, row).as_slice()) {
                    return Err("recovered point mismatch".into());
                }
            }
            checks.push(json!({"name":format!("{label}-{name}-points"),"expected_rows":rows,"actual_rows":rows,"value_mismatches":0,"passed":true}));
        }
        let expected: BTreeMap<_, _> = (0..rows).map(|r| (key(r), value(family, r))).collect();
        let mut actual = BTreeMap::new();
        for row in tx.scan(&Query::new())? {
            let (key, value) = row?;
            if actual.insert(key.to_vec(), value.to_vec()).is_some() {
                return Err("duplicate recovered scan key".into());
            }
        }
        if actual != expected {
            return Err("recovered scan mismatch".into());
        }
        checks.push(json!({"name":format!("{label}-{name}-scan"),"expected_rows":rows,"actual_rows":actual.len(),"value_mismatches":0,"passed":true}));
    }
    Ok(checks)
}

fn boundary(action: &str) -> Result<u128> {
    if std::env::var("MIDGE_RECOVERY_CPU_CONTROL").ok().as_deref() != Some("stdio") {
        return Ok(0);
    }
    let started = Instant::now();
    println!("MIDGE_PROFILE_BOUNDARY {action}");
    std::io::stdout().flush()?;
    let mut reply = String::new();
    std::io::stdin().read_line(&mut reply)?;
    if reply.trim() != "CONTINUE" {
        return Err("sampler control did not acknowledge boundary".into());
    }
    Ok(started.elapsed().as_nanos())
}

pub fn trial(
    artifact_root: &Path,
    target: &Path,
    variant: RecoveryProbeVariant,
    memtable: usize,
    capture: &Capture,
) -> Result<Value> {
    let facts: Value = serde_json::from_slice(&std::fs::read(artifact_root.join("fixture.json"))?)?;
    let rows = usize::try_from(facts["rows_per_family"].as_u64().ok_or("fixture rows")?)?;
    let artifact = artifact_root.join("backup");
    let opts = options(target, memtable, variant)?;
    let manifest = Engine::restore_backup(&artifact, opts.clone())?;
    let restored = inventory(&artifact, Some(target))?;
    if restored != facts["inventory"] {
        return Err("restored immutable input differs".into());
    }
    capture.drain();
    let enabled_ns = boundary("enable")?;
    let started = Instant::now();
    let opened = Engine::open(opts.clone());
    let open_ns = started.elapsed().as_nanos();
    let disabled_ns = boundary("disable")?;
    let native = capture.0.lock().expect("capture lock").clone();
    let mut engine = opened?;
    let metrics = engine.metrics().get_runtime_metrics()?;
    let committed = facts["committed_wal_frontier"]
        .as_u64()
        .ok_or("committed frontier")?;
    if metrics.wal_cloud_durable_seq != committed
        || metrics.current_sequence < committed
        || committed > manifest.durability_frontier
        || metrics.salvage_mode_opens != 0
    {
        return Err(format!(
            "recovery frontier or policy mismatch: durable_wal={}, backup={}, salvage={}",
            metrics.wal_cloud_durable_seq, manifest.durability_frontier, metrics.salvage_mode_opens
        )
        .into());
    }
    let mut checks = verify(&engine, rows, true, "recovered")?;
    engine.shutdown(Duration::from_secs(30))?;
    drop(engine);
    let mut engine = Engine::open(opts)?;
    checks.extend(verify(&engine, rows, false, "reopened")?);
    engine.shutdown(Duration::from_secs(30))?;
    Ok(
        json!({"variant":variant,"fixture_sha256":restored["sha256"],"rows_per_family":rows,"open_ns":open_ns,"target_ns":5_000_000_000u64,
        "target_met":open_ns<=5_000_000_000,"memory_bytes":128*1024*1024,"local_storage_bytes":1024*1024*1024,"memtable_bytes":memtable,"open_deadline_seconds":30,
        "frontier":manifest.durability_frontier,"committed_wal_frontier":committed,"runtime":metrics,"native_open_events":native,"verification":checks,"shutdowns":2,
        "sampling_control_enable_ns":enabled_ns,"sampling_control_disable_ns":disabled_ns,"cpu_scope":"Engine::open_only","production_optimization_accepted":false}),
    )
}
