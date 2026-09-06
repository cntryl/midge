//! Bounded fixture construction. Source data is deterministic and independent
//! of Midge reads; no complete backlog or expected-value ledger is kept in RAM.

use cntryl_midge::{
    wal::{cloud_segment_object_key, encoding, frame, WalOpKind, WalRecord},
    CloudObjectLayout, CloudProviderConfig, CloudStorageLocation, Engine, MemoryBudget,
    OpenOptions, TransactionMode, WriteOptions,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Profile {
    pub wal_bytes: u64,
    pub local_bytes: u64,
    pub memory_bytes: usize,
    pub value_bytes: usize,
    pub segment_bytes: usize,
    pub timeout_seconds: u64,
    #[serde(default)]
    pub memtable_bytes: Option<usize>,
}

impl Profile {
    pub fn selected() -> Vec<Self> {
        let local = number("MIDGE_QUALIFICATION_LOCAL_BYTES", 2 * 1024 * 1024);
        let profile = Self {
            wal_bytes: number("MIDGE_QUALIFICATION_WAL_BYTES", local * 3),
            local_bytes: local,
            memory_bytes: usize::try_from(number(
                "MIDGE_QUALIFICATION_MEMORY_BYTES",
                32 * 1024 * 1024,
            ))
            .expect("memory size"),
            value_bytes: usize::try_from(number("MIDGE_QUALIFICATION_VALUE_BYTES", 8 * 1024))
                .expect("value size"),
            segment_bytes: usize::try_from(number(
                "MIDGE_QUALIFICATION_SEGMENT_BYTES",
                32 * 1024 * 1024,
            ))
            .expect("segment size"),
            timeout_seconds: number("MIDGE_QUALIFICATION_TIMEOUT_SECONDS", 300),
            memtable_bytes: std::env::var("MIDGE_QUALIFICATION_MEMTABLE_BYTES")
                .ok()
                .map(|value| value.parse().expect("memtable target")),
        };
        assert!(
            profile.wal_bytes > profile.local_bytes,
            "qualification requires cloud WAL larger than local disk"
        );
        assert!(profile.value_bytes > 0 && profile.segment_bytes > profile.value_bytes);
        if std::env::var_os("MIDGE_QUALIFICATION_LOCAL_BYTES").is_some() {
            vec![profile]
        } else {
            let mut larger = profile.clone();
            larger.local_bytes *= 2;
            larger.wal_bytes *= 2;
            vec![profile, larger]
        }
    }
}

fn number(name: &str, default: u64) -> u64 {
    std::env::var(name).map_or(default, |value| {
        value.parse().expect("positive qualification number")
    })
}

#[test]
fn should_apply_profile_timeout_to_long_running_engine_requests() {
    // Arrange
    let campaign = Campaign {
        profile: Profile {
            wal_bytes: 6 * 1024 * 1024,
            local_bytes: 2 * 1024 * 1024,
            memory_bytes: 32 * 1024 * 1024,
            value_bytes: 8 * 1024,
            segment_bytes: 32 * 1024 * 1024,
            timeout_seconds: 3600,
            memtable_bytes: None,
        },
        cache: PathBuf::from("unused-qualification-cache"),
        artifacts: PathBuf::new(),
        bucket: "unused-qualification-bucket".into(),
        prefix: "unused/".into(),
        records: 0,
        actual_wal_bytes: 0,
    };

    // Act
    let options = campaign.options();

    // Assert
    assert_eq!(
        options.runtime_response_timeout(),
        Duration::from_secs(3600)
    );
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) struct Campaign {
    pub profile: Profile,
    pub cache: PathBuf,
    pub artifacts: PathBuf,
    pub bucket: String,
    pub prefix: String,
    pub records: u64,
    pub actual_wal_bytes: u64,
}

impl Campaign {
    pub fn prepare(directory: &Path, profile: Profile) -> Self {
        let id = uuid::Uuid::new_v4().to_string();
        let artifacts = std::env::var_os("MIDGE_QUALIFICATION_ARTIFACT_DIR").map_or_else(
            || directory.join("evidence"),
            |path| PathBuf::from(path).join(&id),
        );
        std::fs::create_dir_all(&artifacts).expect("artifacts directory");
        let mut campaign = Self {
            profile,
            cache: std::env::var_os("MIDGE_QUALIFICATION_CACHE_DIR").map_or_else(
                || directory.join("cache"),
                |path| PathBuf::from(path).join(&id),
            ),
            artifacts,
            bucket: "midge-sqrzl-operational".to_string(),
            prefix: format!("operational/{id}/"),
            records: 0,
            actual_wal_bytes: 0,
        };
        super::super::ensure_sqrzl_s3_bucket(&campaign.bucket)
            .expect("prepare qualification bucket");
        let mut engine =
            Engine::open(campaign.options()).expect("initialize native cloud metadata");
        let cf = super::super::default_cf(&engine);
        let mut tx = engine
            .begin_tx(cf.id(), TransactionMode::ReadWrite)
            .expect("seed transaction");
        tx.put(
            b"qualification-seed".to_vec(),
            b"acknowledged".to_vec(),
            None,
        )
        .expect("seed value");
        tx.commit(WriteOptions::cloud_strict())
            .expect("seed cloud acknowledgment");
        engine.flush_cf(&cf).expect("publish seed SST");
        let next_sequence = engine
            .get_storage_layout()
            .expect("seed layout")
            .manifest_last_persisted_sequence
            + 1;
        let next_segment = engine
            .get_runtime_metrics()
            .expect("seed WAL position")
            .wal_current_segment_id
            + 1;
        engine
            .shutdown(Duration::from_secs(30))
            .expect("shutdown fixture owner");
        drop(engine);
        campaign.publish_backlog(next_sequence, next_segment);
        campaign
    }

    pub fn options(&self) -> OpenOptions {
        let target = (self.profile.local_bytes / 8).min(self.profile.memory_bytes as u64 / 16);
        let target = self
            .profile
            .memtable_bytes
            .map_or(target, |bytes| bytes as u64);
        let builder = OpenOptions::cloud(
            self.cache.clone(),
            CloudStorageLocation::new(
                CloudProviderConfig::sqrzl_s3(&self.bucket),
                self.prefix.clone(),
            ),
        )
        .ttl_clock(super::clock::source())
        .memory_budget(MemoryBudget::Bytes(self.profile.memory_bytes))
        .local_storage_budget(self.profile.local_bytes)
        .with_memtable_size_limit(usize::try_from(target).expect("memtable target"))
        .target_sst_size_for_testing(usize::try_from(target).expect("SST target"))
        .lease_clock_skew_tolerance(Duration::ZERO);
        let options = builder.clone().build().expect("campaign engine options");
        let timeout = Duration::from_secs(self.profile.timeout_seconds);
        if timeout <= options.runtime_response_timeout() {
            return options;
        }
        builder
            .runtime_response_timeout(timeout)
            .build()
            .expect("campaign request timeout")
    }

    fn request(&self, method: &str, key: &str, body: &[u8]) -> Vec<u8> {
        super::super::signed_s3_request(
            method,
            &format!("/{}/{}{key}", self.bucket, self.prefix),
            body,
        )
        .unwrap_or_else(|error| panic!("qualification {method} {key}: {error}"))
    }

    fn publish_backlog(&mut self, mut sequence: u64, next_segment: u64) {
        let mut catalog: Value = serde_json::from_slice(&self.request(
            "GET",
            CloudObjectLayout::WAL_CATALOG_OBJECT_KEY,
            &[],
        ))
        .expect("native catalog JSON");
        let epoch = catalog["fencing_epoch"].as_u64().expect("catalog epoch");
        let segments = catalog["segments"].as_object_mut().expect("segments");
        let mut segment_id = (segments
            .keys()
            .map(|key| key.parse::<u64>().expect("segment id"))
            .max()
            .unwrap_or(0)
            + 1)
        .max(next_segment);
        while self.actual_wal_bytes < self.profile.wal_bytes {
            let mut bytes = Vec::new();
            while bytes.len() < self.profile.segment_bytes
                && self.actual_wal_bytes + (bytes.len() as u64) < self.profile.wal_bytes
            {
                let record = WalRecord::new(
                    WalOpKind::Put,
                    key(self.records).into(),
                    Some(value(self.records, self.profile.value_bytes).into()),
                    sequence,
                    epoch,
                );
                frame::append_frame(
                    &mut bytes,
                    &encoding::encode(&record).expect("encode WAL fixture"),
                )
                .expect("frame fixture");
                self.records += 1;
                sequence += 1;
            }
            let object_key = cloud_segment_object_key(segment_id, epoch);
            self.request("PUT", &object_key, &bytes);
            segments.insert(segment_id.to_string(), json!({
                "segment_id": segment_id, "writer_epoch": epoch, "max_sequence": sequence - 1,
                "size_bytes": bytes.len(), "content_crc32c": crc32c::crc32c(&bytes), "object_key": object_key,
            }));
            self.actual_wal_bytes += bytes.len() as u64;
            segment_id += 1;
        }
        let bytes = serde_json::to_vec_pretty(&catalog).expect("catalog encoding");
        self.request("PUT", CloudObjectLayout::WAL_CATALOG_OBJECT_KEY, &bytes);
        self.request(
            "PUT",
            CloudObjectLayout::WAL_CATALOG_MIRROR_OBJECT_KEY,
            &bytes,
        );
        assert!(self.actual_wal_bytes > self.profile.local_bytes);
        eprintln!(
            "MIDGE_OPERATIONAL_FIXTURE {} WAL bytes, {} records",
            self.actual_wal_bytes, self.records
        );
    }
}

pub(super) fn key(index: u64) -> Vec<u8> {
    format!("data-{index:020}").into_bytes()
}

pub(super) fn value(index: u64, length: usize) -> Vec<u8> {
    let mut random = index.wrapping_add(1).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    (0..length)
        .map(|_| {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            random.to_le_bytes()[0]
        })
        .collect()
}
