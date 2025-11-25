//! Hybrid storage layer: Local cache + Cloud tier with automatic fallback.
//!
//! This module implements the core abstraction that enables Midge to operate with
//! minimal local disk while using cloud storage as the source of truth.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────┐
//! │         Application Layer               │
//! │    (SST readers, WAL writers, etc)      │
//! └─────────────────────────────────────────┘
//!                    ↓
//! ┌─────────────────────────────────────────┐
//! │    HybridStorageBackend (Adapter)       │
//! │  (StorageBackend trait implementation)  │
//! └─────────────────────────────────────────┘
//!                    ↓
//! ┌─────────────────────────────────────────┐
//! │          HybridStorage                  │
//! │  (This module - orchestrates tier)      │
//! └─────────────────────────────────────────┘
//!          ↙                    ↘
//! ┌──────────────────┐    ┌──────────────────┐
//! │  Local Cache     │    │  Cloud Backend   │
//! │  (Fast, small)   │    │  (Durable, big)  │
//! │  fs::File ops    │    │  S3/Azure/GCS    │
//! └──────────────────┘    └──────────────────┘
//! ```
//!
//! # Key Features
//!
//! - **Read path**: Local cache first, cloud fallback on miss
//! - **Write path**: Write local + async cloud upload
//! - **Eviction**: LRU-based when cache exceeds max_local_bytes
//! - **Crash recovery**: Cloud is source of truth, local is disposable
//!
//! # Example
//!
//! ```rust,no_run
//! use cntryl_midge::cloud::hybrid::{HybridStorage, HybridStorageBackend};
//! use cntryl_midge::cloud::MockCloudBackend;
//! use std::sync::Arc;
//!
//! let backend = Arc::new(MockCloudBackend::new());
//! let hybrid = HybridStorage::new(
//!     "./local_cache",
//!     backend,
//!     1024 * 1024 * 1024, // 1GB cache
//! ).unwrap();
//!
//! // Spawn background workers for async uploads and eviction
//! hybrid.spawn_background_workers();
//!
//! // Wrap in StorageBackend adapter for use with SST/WAL
//! let storage_backend = Arc::new(HybridStorageBackend::new(Arc::new(hybrid), true));
//! ```

use crate::cloud::StorageBackend;
use crate::error::MidgeResult;
use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tracing::{debug, warn};

/// Cloud-specific performance metrics.
///
/// Tracks cache efficiency and cloud operation performance to help
/// diagnose issues and optimize configuration.
struct CloudMetrics {
    /// Total cache hits (read from local)
    cache_hits: AtomicU64,

    /// Total cache misses (fetched from cloud)
    cache_misses: AtomicU64,

    /// Total cloud uploads completed
    uploads_completed: AtomicU64,

    /// Total cloud upload failures
    uploads_failed: AtomicU64,

    /// Upload latency tracking
    upload_latencies: RwLock<UploadLatencyTracker>,

    /// Files evicted from cache
    files_evicted: AtomicU64,
}

/// Tracks upload latency statistics
struct UploadLatencyTracker {
    /// Recent upload latencies (ring buffer, last 100)
    recent_latencies_ms: VecDeque<u64>,

    /// Total upload time (for average calculation)
    total_upload_time_ms: u64,

    /// Total uploads tracked
    total_uploads: u64,
}

impl Default for CloudMetrics {
    fn default() -> Self {
        Self {
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            uploads_completed: AtomicU64::new(0),
            uploads_failed: AtomicU64::new(0),
            upload_latencies: RwLock::new(UploadLatencyTracker {
                recent_latencies_ms: VecDeque::with_capacity(100),
                total_upload_time_ms: 0,
                total_uploads: 0,
            }),
            files_evicted: AtomicU64::new(0),
        }
    }
}

impl CloudMetrics {
    fn record_cache_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    fn record_cache_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    fn record_upload_success(&self, duration: Duration) {
        self.uploads_completed.fetch_add(1, Ordering::Relaxed);

        let duration_ms = duration.as_millis() as u64;
        let mut tracker = self.upload_latencies.write();

        // Keep last 100 latencies
        if tracker.recent_latencies_ms.len() >= 100 {
            tracker.recent_latencies_ms.pop_front();
        }
        tracker.recent_latencies_ms.push_back(duration_ms);

        tracker.total_upload_time_ms += duration_ms;
        tracker.total_uploads += 1;
    }

    fn record_upload_failure(&self) {
        self.uploads_failed.fetch_add(1, Ordering::Relaxed);
    }

    fn record_eviction(&self) {
        self.files_evicted.fetch_add(1, Ordering::Relaxed);
    }
}

/// Hybrid storage combining local cache with cloud backend.
///
/// This is the core abstraction that enables cloud-backed storage modes.
/// It manages:
/// - Local file cache for hot data
/// - Automatic cloud uploads for durability
/// - LRU eviction when cache is full
/// - Transparent fallback on cache misses
pub struct HybridStorage {
    /// Local cache directory
    local_path: PathBuf,

    /// Cloud storage backend
    cloud_backend: Arc<dyn StorageBackend>,

    /// Maximum local cache size in bytes
    max_local_bytes: u64,

    /// Cache state (sizes, LRU tracking)
    cache_state: Arc<RwLock<CacheState>>,

    /// Background upload queue (for async mode)
    upload_queue: Arc<RwLock<VecDeque<UploadTask>>>,

    /// Shutdown signal for background threads
    shutdown: Arc<AtomicBool>,

    /// Cloud-specific metrics
    metrics: Arc<CloudMetrics>,

    /// Worker thread handles (for clean shutdown)
    worker_handles: parking_lot::Mutex<Vec<thread::JoinHandle<()>>>,
}

/// Internal cache state tracking
struct CacheState {
    /// Map of file key → local file size
    file_sizes: HashMap<String, u64>,

    /// LRU queue (front = most recently used, back = least recently used)
    lru_queue: VecDeque<String>,

    /// Current total cache usage in bytes
    total_bytes: u64,
}

/// Upload task for async cloud uploads
struct UploadTask {
    key: String,
    local_path: PathBuf,
}

impl HybridStorage {
    /// Create a new hybrid storage instance.
    ///
    /// # Arguments
    ///
    /// * `local_path` - Directory for local cache
    /// * `cloud_backend` - Cloud storage backend (S3, Azure, GCS, or mock)
    /// * `max_local_bytes` - Maximum cache size (LRU eviction when exceeded)
    pub fn new(
        local_path: impl Into<PathBuf>,
        cloud_backend: Arc<dyn StorageBackend>,
        max_local_bytes: u64,
    ) -> MidgeResult<Self> {
        let local_path = local_path.into();

        // Create local cache directory if needed
        std::fs::create_dir_all(&local_path)?;

        Ok(Self {
            local_path,
            cloud_backend,
            max_local_bytes,
            cache_state: Arc::new(RwLock::new(CacheState {
                file_sizes: HashMap::new(),
                lru_queue: VecDeque::new(),
                total_bytes: 0,
            })),
            upload_queue: Arc::new(RwLock::new(VecDeque::new())),
            shutdown: Arc::new(AtomicBool::new(false)),
            metrics: Arc::new(CloudMetrics::default()),
            worker_handles: parking_lot::Mutex::new(Vec::new()),
        })
    }

    /// Write data to both local cache and cloud.
    ///
    /// # Synchronous mode (Strict durability)
    /// Blocks until cloud upload completes and is verified.
    ///
    /// # Asynchronous mode (Steady/CloudReplicated durability)
    /// Writes to local cache immediately, queues cloud upload for background.
    pub fn write(&self, key: &str, data: Bytes, sync: bool) -> MidgeResult<()> {
        debug!(
            "Hybrid write: key={}, size={}, sync={}",
            key,
            data.len(),
            sync
        );

        // 1. Write to local cache
        let local_file = self.local_file_path(key);
        if let Some(parent) = local_file.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&local_file, &data)?;

        // 2. Update cache state
        self.update_cache_state(key, data.len() as u64);

        // 3. Upload to cloud
        if sync {
            // Synchronous: block until upload completes
            let start = Instant::now();
            match self.cloud_backend.put_blob(key, data) {
                Ok(_) => {
                    let duration = start.elapsed();
                    self.metrics.record_upload_success(duration);
                    debug!("Hybrid write completed (sync): {} ({:?})", key, duration);
                }
                Err(e) => {
                    self.metrics.record_upload_failure();
                    return Err(e);
                }
            }
        } else {
            // Asynchronous: queue for background upload
            self.queue_upload(key, local_file);
            debug!("Hybrid write completed (async queued): {}", key);
        }

        Ok(())
    }

    /// Read data from local cache or cloud (with fallback).
    ///
    /// # Read path
    /// 1. Check local cache → return if present (cache hit)
    /// 2. Download from cloud → cache locally → return (cache miss)
    pub fn read(&self, key: &str) -> MidgeResult<Bytes> {
        debug!("Hybrid read: key={}", key);

        // Try local cache first
        let local_file = self.local_file_path(key);
        if local_file.exists() {
            debug!("Cache hit: {}", key);
            self.metrics.record_cache_hit();

            // Update LRU (mark as recently used)
            self.touch_cache_entry(key);

            let data = std::fs::read(&local_file)?;
            return Ok(Bytes::from(data));
        }

        // Cache miss - fetch from cloud
        debug!("Cache miss: {}", key);
        self.metrics.record_cache_miss();

        let data = self.cloud_backend.get_blob(key)?;

        // Store in local cache for future reads
        if let Some(parent) = local_file.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&local_file, &data)?;
        self.update_cache_state(key, data.len() as u64);

        Ok(data)
    }

    /// Check if a file exists (checks both local and cloud).
    pub fn exists(&self, key: &str) -> MidgeResult<bool> {
        // Check local first (fast)
        let local_file = self.local_file_path(key);
        if local_file.exists() {
            return Ok(true);
        }

        // Check cloud
        Ok(self.cloud_backend.head_blob(key)?.is_some())
    }

    /// Delete from both local cache and cloud.
    pub fn delete(&self, key: &str) -> MidgeResult<()> {
        debug!("Hybrid delete: key={}", key);

        // Remove from local cache
        let local_file = self.local_file_path(key);
        if local_file.exists() {
            std::fs::remove_file(&local_file)?;
            self.remove_from_cache_state(key);
        }

        // Delete from cloud
        self.cloud_backend.delete_blob(key)?;

        Ok(())
    }

    /// List all files (from cloud - cloud is source of truth).
    pub fn list(&self, prefix: &str) -> MidgeResult<Vec<String>> {
        self.cloud_backend.list_blobs(prefix)
    }

    /// Get current cache usage statistics.
    pub fn cache_stats(&self) -> CacheStats {
        let state = self.cache_state.read();
        CacheStats {
            total_bytes: state.total_bytes,
            max_bytes: self.max_local_bytes,
            file_count: state.file_sizes.len(),
            usage_percent: (state.total_bytes as f64 / self.max_local_bytes as f64 * 100.0) as u32,
        }
    }

    /// Get cloud operation metrics snapshot.
    ///
    /// Returns current metrics including cache hit ratio, upload latencies, and eviction counts.
    pub fn cloud_metrics(&self) -> CloudMetricsSnapshot {
        let cache_hits = self.metrics.cache_hits.load(Ordering::Relaxed);
        let cache_misses = self.metrics.cache_misses.load(Ordering::Relaxed);
        let total_reads = cache_hits + cache_misses;

        let cache_hit_ratio = if total_reads > 0 {
            cache_hits as f64 / total_reads as f64
        } else {
            0.0
        };

        let tracker = self.metrics.upload_latencies.read();
        let avg_upload_latency_ms = if tracker.total_uploads > 0 {
            tracker.total_upload_time_ms as f64 / tracker.total_uploads as f64
        } else {
            0.0
        };

        // Calculate percentiles from recent latencies
        let mut sorted_latencies: Vec<u64> = tracker.recent_latencies_ms.iter().copied().collect();
        sorted_latencies.sort_unstable();

        let p50 = if !sorted_latencies.is_empty() {
            sorted_latencies[sorted_latencies.len() / 2]
        } else {
            0
        };

        let p99 = if !sorted_latencies.is_empty() {
            let idx = (sorted_latencies.len() as f64 * 0.99) as usize;
            sorted_latencies[idx.min(sorted_latencies.len() - 1)]
        } else {
            0
        };

        CloudMetricsSnapshot {
            cache_hits,
            cache_misses,
            cache_hit_ratio,
            uploads_completed: self.metrics.uploads_completed.load(Ordering::Relaxed),
            uploads_failed: self.metrics.uploads_failed.load(Ordering::Relaxed),
            avg_upload_latency_ms,
            p50_upload_latency_ms: p50,
            p99_upload_latency_ms: p99,
            files_evicted: self.metrics.files_evicted.load(Ordering::Relaxed),
        }
    }

    /// Evict least recently used files until cache is under limit.
    ///
    /// Called automatically when cache exceeds max_local_bytes.
    pub fn evict_lru(&self) -> MidgeResult<usize> {
        let mut state = self.cache_state.write();
        let mut evicted = 0;

        while state.total_bytes > self.max_local_bytes && !state.lru_queue.is_empty() {
            // Remove least recently used (back of queue)
            if let Some(key) = state.lru_queue.pop_back() {
                if let Some(size) = state.file_sizes.remove(&key) {
                    // Delete local file
                    let local_file = self.local_file_path(&key);
                    if let Err(e) = std::fs::remove_file(&local_file) {
                        warn!("Failed to evict {}: {}", key, e);
                    } else {
                        state.total_bytes = state.total_bytes.saturating_sub(size);
                        evicted += 1;
                        self.metrics.record_eviction();
                        debug!("Evicted: {} ({} bytes)", key, size);
                    }
                }
            }
        }

        debug!(
            "Evicted {} files, cache now: {} bytes",
            evicted, state.total_bytes
        );
        Ok(evicted)
    }

    /// Process pending async uploads (call from background thread).
    pub fn process_uploads(&self, max_uploads: usize) -> MidgeResult<usize> {
        let mut queue = self.upload_queue.write();
        let mut processed = 0;

        while processed < max_uploads && !queue.is_empty() {
            if let Some(task) = queue.pop_front() {
                // Read from local file
                match std::fs::read(&task.local_path) {
                    Ok(data) => {
                        // Upload to cloud and track latency
                        let start = Instant::now();
                        match self.cloud_backend.put_blob(&task.key, Bytes::from(data)) {
                            Ok(_) => {
                                let duration = start.elapsed();
                                self.metrics.record_upload_success(duration);
                                debug!("Upload completed: {} ({:?})", task.key, duration);
                                processed += 1;
                            }
                            Err(e) => {
                                self.metrics.record_upload_failure();
                                warn!("Upload failed: {} - {}", task.key, e);
                                // Re-queue with backoff (optional: implement retry logic)
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Failed to read local file for upload: {} - {}", task.key, e);
                    }
                }
            }
        }

        Ok(processed)
    }

    // ===== Internal Helpers =====

    fn local_file_path(&self, key: &str) -> PathBuf {
        let sanitized = key.replace('/', std::path::MAIN_SEPARATOR_STR);
        self.local_path.join(sanitized)
    }

    fn update_cache_state(&self, key: &str, size: u64) {
        let mut state = self.cache_state.write();

        // Remove from old position in LRU queue if exists
        if let Some(pos) = state.lru_queue.iter().position(|k| k == key) {
            state.lru_queue.remove(pos);
            if let Some(old_size) = state.file_sizes.get(key) {
                state.total_bytes = state.total_bytes.saturating_sub(*old_size);
            }
        }

        // Add to front of LRU queue (most recently used)
        state.lru_queue.push_front(key.to_string());
        state.file_sizes.insert(key.to_string(), size);
        state.total_bytes += size;

        drop(state);

        // Check if we need to evict
        if self.cache_state.read().total_bytes > self.max_local_bytes {
            let _ = self.evict_lru();
        }
    }

    fn touch_cache_entry(&self, key: &str) {
        let mut state = self.cache_state.write();

        // Move to front of LRU queue (mark as recently used)
        if let Some(pos) = state.lru_queue.iter().position(|k| k == key) {
            state.lru_queue.remove(pos);
            state.lru_queue.push_front(key.to_string());
        }
    }

    fn remove_from_cache_state(&self, key: &str) {
        let mut state = self.cache_state.write();

        if let Some(size) = state.file_sizes.remove(key) {
            state.total_bytes = state.total_bytes.saturating_sub(size);
        }

        if let Some(pos) = state.lru_queue.iter().position(|k| k == key) {
            state.lru_queue.remove(pos);
        }
    }

    fn queue_upload(&self, key: &str, local_path: PathBuf) {
        let mut queue = self.upload_queue.write();
        queue.push_back(UploadTask {
            key: key.to_string(),
            local_path,
        });
    }

    /// Start background threads for async uploads and cache eviction.
    ///
    /// This spawns two threads:
    /// 1. **Upload worker**: Processes queued async uploads to cloud
    /// 2. **Eviction worker**: Periodically checks cache size and evicts LRU files
    ///
    /// Stores the handles internally for clean shutdown on drop.
    pub fn spawn_background_workers(&self) {
        let mut handles = self.worker_handles.lock();
        handles.push(self.spawn_upload_worker());
        handles.push(self.spawn_eviction_worker());
    }

    /// Spawn background thread to process async uploads.
    fn spawn_upload_worker(&self) -> thread::JoinHandle<()> {
        let upload_queue = Arc::clone(&self.upload_queue);
        let cloud_backend = Arc::clone(&self.cloud_backend);
        let shutdown = Arc::clone(&self.shutdown);
        let metrics = Arc::clone(&self.metrics);

        thread::spawn(move || {
            debug!("Upload worker started");

            while !shutdown.load(Ordering::Relaxed) {
                // Process up to 10 uploads per iteration
                let mut processed = 0;
                loop {
                    let task = {
                        let mut queue = upload_queue.write();
                        queue.pop_front()
                    };

                    let Some(task) = task else {
                        break;
                    };

                    // Read local file and upload to cloud
                    match std::fs::read(&task.local_path) {
                        Ok(data) => {
                            let data = Bytes::from(data);
                            let start = Instant::now();
                            match cloud_backend.put_blob(&task.key, data) {
                                Ok(_) => {
                                    let duration = start.elapsed();
                                    metrics.record_upload_success(duration);
                                    debug!("Background uploaded: {} ({:?})", task.key, duration);
                                }
                                Err(e) => {
                                    metrics.record_upload_failure();
                                    warn!("Background upload failed for {}: {}", task.key, e);
                                }
                            }
                        }
                        Err(e) => {
                            warn!(
                                "Failed to read {} for upload: {}",
                                task.local_path.display(),
                                e
                            );
                        }
                    }

                    processed += 1;
                    if processed >= 10 {
                        break;
                    }
                }

                // Sleep 100ms between batches
                thread::sleep(Duration::from_millis(100));
            }

            debug!("Upload worker stopped");
        })
    }

    /// Spawn background thread to perform periodic cache eviction.
    fn spawn_eviction_worker(&self) -> thread::JoinHandle<()> {
        let cache_state = Arc::clone(&self.cache_state);
        let local_path = self.local_path.clone();
        let max_local_bytes = self.max_local_bytes;
        let shutdown = Arc::clone(&self.shutdown);
        let metrics = Arc::clone(&self.metrics);

        thread::spawn(move || {
            debug!("Eviction worker started");

            while !shutdown.load(Ordering::Relaxed) {
                // Check if eviction needed
                let needs_eviction = {
                    let state = cache_state.read();
                    state.total_bytes > max_local_bytes
                };

                if needs_eviction {
                    // Evict LRU files until under 90% of limit
                    let target = (max_local_bytes as f64 * 0.9) as u64;
                    let mut evicted = 0;

                    loop {
                        let (key_to_evict, _current_usage) = {
                            let state = cache_state.read();
                            if state.total_bytes <= target {
                                break;
                            }
                            (state.lru_queue.back().cloned(), state.total_bytes)
                        };

                        let Some(key) = key_to_evict else {
                            break;
                        };

                        // Remove file
                        let file_path = local_path.join(&key);
                        if file_path.exists() {
                            if let Err(e) = std::fs::remove_file(&file_path) {
                                warn!("Failed to evict {}: {}", key, e);
                                break;
                            }
                        }

                        // Update state
                        let mut state = cache_state.write();
                        if let Some(size) = state.file_sizes.remove(&key) {
                            state.total_bytes = state.total_bytes.saturating_sub(size);
                            metrics.record_eviction();
                            evicted += 1;
                        }
                        if let Some(pos) = state.lru_queue.iter().position(|k| k == &key) {
                            state.lru_queue.remove(pos);
                        }
                    }

                    if evicted > 0 {
                        let state = cache_state.read();
                        debug!(
                            "Evicted {} files, cache now at {}/{} bytes",
                            evicted, state.total_bytes, max_local_bytes
                        );
                    }
                }

                // Check every 5 seconds
                thread::sleep(Duration::from_secs(5));
            }

            debug!("Eviction worker stopped");
        })
    }

    /// Shutdown background workers gracefully.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        
        // Join all worker threads
        let mut handles = self.worker_handles.lock();
        for handle in handles.drain(..) {
            if let Err(_) = handle.join() {
                debug!("Worker thread panicked during shutdown");
            }
        }
    }
}

impl Drop for HybridStorage {
    fn drop(&mut self) {
        eprintln!("[SHUTDOWN] HybridStorage::drop - signaling workers to stop");
        self.shutdown.store(true, Ordering::Relaxed);
        
        // Join all worker threads
        let mut handles = self.worker_handles.lock();
        for handle in handles.drain(..) {
            eprintln!("[SHUTDOWN] HybridStorage::drop - joining worker thread");
            match handle.join() {
                Ok(_) => eprintln!("[SHUTDOWN] HybridStorage worker joined successfully"),
                Err(e) => eprintln!("[SHUTDOWN] HybridStorage worker panicked: {:?}", e),
            }
        }
        eprintln!("[SHUTDOWN] HybridStorage::drop - complete");
    }
}

/// Cache usage statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub total_bytes: u64,
    pub max_bytes: u64,
    pub file_count: usize,
    pub usage_percent: u32,
}

/// Cloud operation metrics snapshot.
///
/// Provides visibility into cache efficiency and cloud performance.
#[derive(Debug, Clone)]
pub struct CloudMetricsSnapshot {
    /// Total reads served from local cache
    pub cache_hits: u64,

    /// Total reads that required cloud download
    pub cache_misses: u64,

    /// Cache hit ratio (0.0 to 1.0)
    pub cache_hit_ratio: f64,

    /// Total successful cloud uploads
    pub uploads_completed: u64,

    /// Total failed cloud uploads
    pub uploads_failed: u64,

    /// Average upload latency in milliseconds
    pub avg_upload_latency_ms: f64,

    /// P50 upload latency in milliseconds (median)
    pub p50_upload_latency_ms: u64,

    /// P99 upload latency in milliseconds
    pub p99_upload_latency_ms: u64,

    /// Total files evicted from cache
    pub files_evicted: u64,
}

/// Adapter implementing StorageBackend trait using HybridStorage.
///
/// This allows HybridStorage to be used wherever a StorageBackend is expected
/// (e.g., CloudSstFactory, CloudWalWriter). It enables caching for all cloud-backed
/// operations.
///
/// # Write Modes
///
/// - **Sync mode** (`sync_writes=true`): All puts block until cloud upload completes
/// - **Async mode** (`sync_writes=false`): Puts queue cloud uploads for background processing
pub struct HybridStorageBackend {
    hybrid: Arc<HybridStorage>,
    sync_writes: bool,
}

impl HybridStorageBackend {
    /// Create a new adapter wrapping HybridStorage.
    ///
    /// # Arguments
    ///
    /// * `hybrid` - The underlying HybridStorage instance
    /// * `sync_writes` - Whether to wait for cloud uploads (true = Strict durability)
    pub fn new(hybrid: Arc<HybridStorage>, sync_writes: bool) -> Self {
        Self {
            hybrid,
            sync_writes,
        }
    }

    /// Get the underlying HybridStorage instance
    pub fn hybrid(&self) -> &Arc<HybridStorage> {
        &self.hybrid
    }
}

impl StorageBackend for HybridStorageBackend {
    fn put_blob(&self, key: &str, data: Bytes) -> MidgeResult<()> {
        self.hybrid.write(key, data, self.sync_writes)
    }

    fn get_blob(&self, key: &str) -> MidgeResult<Bytes> {
        self.hybrid.read(key)
    }

    fn get_blob_range(&self, key: &str, start: u64, end: Option<u64>) -> MidgeResult<Bytes> {
        // For range reads, always read from cloud (caching full files only for now)
        // Future optimization: could cache and serve ranges from local files
        let cloud_backend = &self.hybrid.cloud_backend;
        cloud_backend.get_blob_range(key, start, end)
    }

    fn delete_blob(&self, key: &str) -> MidgeResult<()> {
        self.hybrid.delete(key)
    }

    fn list_blobs(&self, prefix: &str) -> MidgeResult<Vec<String>> {
        self.hybrid.list(prefix)
    }

    fn head_blob(&self, key: &str) -> MidgeResult<Option<crate::cloud::BlobMeta>> {
        // Delegate to cloud backend (source of truth for metadata)
        self.hybrid.cloud_backend.head_blob(key)
    }

    fn put_blob_if_not_exists(&self, key: &str, data: Bytes) -> MidgeResult<String> {
        // Write to cloud first (source of truth for existence checks)
        let etag = self
            .hybrid
            .cloud_backend
            .put_blob_if_not_exists(key, data.clone())?;

        // Then cache locally
        let local_file = self.hybrid.local_file_path(key);
        if let Some(parent) = local_file.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&local_file, &data)?;
        self.hybrid.update_cache_state(key, data.len() as u64);

        Ok(etag)
    }

    fn put_if_match(&self, key: &str, data: Bytes, expected_etag: &str) -> MidgeResult<String> {
        // Delegate to cloud backend for conditional puts
        let etag = self
            .hybrid
            .cloud_backend
            .put_if_match(key, data.clone(), expected_etag)?;

        // Update cache after successful cloud write
        let local_file = self.hybrid.local_file_path(key);
        if let Some(parent) = local_file.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&local_file, &data)?;
        self.hybrid.update_cache_state(key, data.len() as u64);

        Ok(etag)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cloud::MockCloudBackend;

    #[test]
    fn should_create_hybrid_storage() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let cache_dir = std::env::temp_dir().join("hybrid_test_create");

        // Act
        let hybrid = HybridStorage::new(cache_dir.clone(), backend, 1024 * 1024);

        // Assert
        assert!(hybrid.is_ok());
        assert!(cache_dir.exists());
    }

    #[test]
    fn should_write_to_hybrid_storage() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let cache_dir = std::env::temp_dir().join("hybrid_test_write");
        let hybrid = HybridStorage::new(cache_dir, backend.clone(), 1024 * 1024).unwrap();
        let data = Bytes::from("test data");

        // Act
        hybrid.write("test.dat", data, true).unwrap();

        // Assert
        assert_eq!(backend.upload_count(), 1);
    }

    #[test]
    fn should_read_from_hybrid_storage_with_cache_hit() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let cache_dir = std::env::temp_dir().join("hybrid_test_cache_hit");
        let hybrid = HybridStorage::new(cache_dir, backend.clone(), 1024 * 1024).unwrap();
        let data = Bytes::from("test data");
        hybrid.write("test.dat", data.clone(), true).unwrap(); // Pre-fill

        // Act
        let retrieved = hybrid.read("test.dat").unwrap();

        // Assert
        assert_eq!(retrieved, data);
        assert_eq!(backend.upload_count(), 1); // No additional upload
    }

    #[test]
    fn should_fallback_to_cloud_on_cache_miss() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let cache_dir = std::env::temp_dir().join("hybrid_test_cache_miss");
        let hybrid = HybridStorage::new(cache_dir.clone(), backend.clone(), 1024 * 1024).unwrap();
        let data = Bytes::from("cloud data");

        // Act - Write directly to cloud (bypassing hybrid)
        backend.put_blob("cloud_only.dat", data.clone()).unwrap();

        // Act
        // Read through hybrid (should fetch from cloud)
        let retrieved = hybrid.read("cloud_only.dat").unwrap();

        // Assert
        assert_eq!(retrieved, data);

        // Verify it's now cached locally
        let local_file = cache_dir.join("cloud_only.dat");
        assert!(local_file.exists());
    }

    #[test]
    fn should_evict_lru_when_cache_full() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let cache_dir = std::env::temp_dir().join("hybrid_test_eviction");
        let max_cache = 100; // 100 bytes max
        let hybrid = HybridStorage::new(cache_dir, backend, max_cache).unwrap();

        // Act
        hybrid
            .write("file1.dat", Bytes::from(vec![0u8; 50]), true)
            .unwrap();
        hybrid
            .write("file2.dat", Bytes::from(vec![1u8; 50]), true)
            .unwrap();
        hybrid
            .write("file3.dat", Bytes::from(vec![2u8; 50]), true)
            .unwrap(); // Triggers eviction

        // Assert
        let stats = hybrid.cache_stats();
        assert!(stats.total_bytes <= max_cache);

        // Oldest file (file1) should be evicted
        assert!(!hybrid.local_file_path("file1.dat").exists());
        assert!(hybrid.local_file_path("file3.dat").exists());
    }

    #[test]
    fn should_track_cache_statistics() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let cache_dir = std::env::temp_dir().join("hybrid_test_stats");
        let hybrid = HybridStorage::new(cache_dir, backend, 1000).unwrap();

        // Act
        hybrid
            .write("f1.dat", Bytes::from(vec![0u8; 100]), true)
            .unwrap();
        hybrid
            .write("f2.dat", Bytes::from(vec![0u8; 200]), true)
            .unwrap();
        let stats = hybrid.cache_stats();

        // Assert
        assert_eq!(stats.total_bytes, 300);
        assert_eq!(stats.file_count, 2);
        assert_eq!(stats.usage_percent, 30); // 300/1000 = 30%
    }

    #[test]
    fn should_delete_file_from_hybrid_storage() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let cache_dir = std::env::temp_dir().join("hybrid_test_delete");
        let hybrid = HybridStorage::new(cache_dir.clone(), backend.clone(), 1024).unwrap();

        // Act
        hybrid
            .write("delete_me.dat", Bytes::from("data"), true)
            .unwrap();
        assert!(hybrid.exists("delete_me.dat").unwrap());

        hybrid.delete("delete_me.dat").unwrap();

        // Assert
        assert!(!hybrid.exists("delete_me.dat").unwrap());
        assert!(!cache_dir.join("delete_me.dat").exists());
    }

    #[test]
    fn should_list_files_from_cloud() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let cache_dir = std::env::temp_dir().join("hybrid_test_list");
        let hybrid = HybridStorage::new(cache_dir, backend, 1024).unwrap();

        // Act
        hybrid
            .write("prefix/file1.dat", Bytes::from("1"), true)
            .unwrap();
        hybrid
            .write("prefix/file2.dat", Bytes::from("2"), true)
            .unwrap();
        hybrid
            .write("other/file3.dat", Bytes::from("3"), true)
            .unwrap();

        let files = hybrid.list("prefix/").unwrap();

        // Assert
        assert_eq!(files.len(), 2);
        assert!(files.contains(&"prefix/file1.dat".to_string()));
        assert!(files.contains(&"prefix/file2.dat".to_string()));
    }

    #[test]
    fn should_queue_async_uploads() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let cache_dir = std::env::temp_dir().join("hybrid_test_async");
        let hybrid = HybridStorage::new(cache_dir, backend.clone(), 1024).unwrap();

        // Act
        hybrid
            .write("async.dat", Bytes::from("data"), false)
            .unwrap();

        // Upload not yet processed
        assert_eq!(backend.upload_count(), 0);

        // Process uploads
        let processed = hybrid.process_uploads(10).unwrap();

        // Assert
        assert_eq!(processed, 1);
        assert_eq!(backend.upload_count(), 1);
    }

    #[test]
    fn should_process_uploads_in_background_thread() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let cache_dir = std::env::temp_dir().join("hybrid_test_bg_upload");
        let hybrid = HybridStorage::new(cache_dir, backend.clone(), 1024 * 1024).unwrap();

        // Act
        let _handles = hybrid.spawn_background_workers();

        // Write async (queues upload)
        hybrid
            .write("bg1.dat", Bytes::from("data1"), false)
            .unwrap();
        hybrid
            .write("bg2.dat", Bytes::from("data2"), false)
            .unwrap();

        // Give upload worker time to process
        std::thread::sleep(std::time::Duration::from_millis(300));

        // Assert
        assert_eq!(backend.upload_count(), 2);

        // Cleanup
        hybrid.shutdown();
    }

    #[test]
    fn should_evict_cache_in_background_thread() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let cache_dir = std::env::temp_dir().join("hybrid_test_bg_evict");
        let _ = std::fs::remove_dir_all(&cache_dir); // Clean start
        let max_bytes = 1000;
        let hybrid = HybridStorage::new(cache_dir.clone(), backend, max_bytes).unwrap();

        // Act
        hybrid.spawn_background_workers();

        // Write files up to limit (but not exceeding)
        hybrid
            .write("file1.dat", Bytes::from(vec![0u8; 300]), true)
            .unwrap();
        hybrid
            .write("file2.dat", Bytes::from(vec![1u8; 300]), true)
            .unwrap();
        hybrid
            .write("file3.dat", Bytes::from(vec![2u8; 300]), true)
            .unwrap();

        let stats_initial = hybrid.cache_stats();
        assert_eq!(
            stats_initial.total_bytes, 900,
            "Should have all three files (900 bytes)"
        );

        // Manually add a file to local cache to exceed limit (simulating a scenario
        // where files are added outside normal write path, or where synchronous
        // eviction was skipped for some reason)
        let manual_file = cache_dir.join("manual.dat");
        std::fs::write(&manual_file, vec![3u8; 300]).unwrap();

        // Manually update cache state to reflect the new file
        {
            let mut state = hybrid.cache_state.write();
            state.file_sizes.insert("manual.dat".to_string(), 300);
            state.lru_queue.push_back("manual.dat".to_string()); // Least recently used
            state.total_bytes += 300;
        }

        // Verify cache now exceeds limit
        let stats_before = hybrid.cache_stats();
        assert_eq!(
            stats_before.total_bytes, 1200,
            "Cache should now have 1200 bytes"
        );
        assert!(stats_before.total_bytes > max_bytes, "Cache exceeds limit");

        // Give eviction worker time to run (checks every 5s, so wait 6s to be safe)
        std::thread::sleep(std::time::Duration::from_secs(6));

        // Assert
        let stats_after = hybrid.cache_stats();
        assert!(
            stats_after.total_bytes <= max_bytes,
            "Expected <= {} bytes after background eviction, got {}",
            max_bytes,
            stats_after.total_bytes
        );

        // Verify eviction actually happened
        assert!(
            stats_after.total_bytes < stats_before.total_bytes,
            "Cache should have shrunk from {} to {}",
            stats_before.total_bytes,
            stats_after.total_bytes
        );

        // The manual file (LRU) should have been evicted
        assert!(!manual_file.exists(), "LRU file should have been evicted");

        // Cleanup
        hybrid.shutdown();
    }

    // HybridStorageBackend adapter tests
    #[test]
    fn should_use_adapter_for_sync_writes() {
        // Arrange
        let cloud = Arc::new(MockCloudBackend::new());
        let cache_dir = std::env::temp_dir().join("hybrid_adapter_sync");
        let hybrid = Arc::new(HybridStorage::new(cache_dir, cloud.clone(), 1024).unwrap());
        let backend = HybridStorageBackend::new(hybrid, true);

        // Act
        backend.put_blob("test.dat", Bytes::from("data")).unwrap();

        // Assert
        assert_eq!(cloud.upload_count(), 1);

        // And readable through adapter
        let data = backend.get_blob("test.dat").unwrap();
        assert_eq!(data, Bytes::from("data"));
    }

    #[test]
    fn should_use_adapter_for_async_writes() {
        // Arrange
        let cloud = Arc::new(MockCloudBackend::new());
        let cache_dir = std::env::temp_dir().join("hybrid_adapter_async");
        let hybrid = Arc::new(HybridStorage::new(cache_dir, cloud.clone(), 1024).unwrap());
        let backend = HybridStorageBackend::new(hybrid.clone(), false);

        // Act
        backend.put_blob("test.dat", Bytes::from("data")).unwrap();

        // Assert
        assert_eq!(cloud.upload_count(), 0);

        // Process uploads
        hybrid.process_uploads(10).unwrap();

        // Now in cloud
        assert_eq!(cloud.upload_count(), 1);
    }

    #[test]
    fn should_use_adapter_for_cache_hits() {
        // Arrange
        let cloud = Arc::new(MockCloudBackend::new());
        let cache_dir = std::env::temp_dir().join("hybrid_adapter_cache");
        let hybrid = Arc::new(HybridStorage::new(cache_dir, cloud.clone(), 1024).unwrap());
        let backend = HybridStorageBackend::new(hybrid, true);

        // Act
        backend
            .put_blob("cached.dat", Bytes::from("cached_data"))
            .unwrap();
        let data = backend.get_blob("cached.dat").unwrap();

        // Assert
        assert_eq!(data, Bytes::from("cached_data"));
        assert_eq!(cloud.sst_download_count(), 0); // Cache hit, no cloud read
    }

    // ===== Metrics Tests =====

    #[test]
    fn should_track_cache_hit_miss_metrics_when_reading() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let cache_dir = std::env::temp_dir().join("hybrid_test_cache_metrics");
        let _ = std::fs::remove_dir_all(&cache_dir);
        let hybrid = HybridStorage::new(cache_dir.clone(), backend.clone(), 1024).unwrap();

        // Act
        hybrid
            .write("file1.dat", Bytes::from("data1"), true)
            .unwrap();
        hybrid.read("file1.dat").unwrap();

        // Write to cloud directly and read (cache miss)
        backend.put_blob("file2.dat", Bytes::from("data2")).unwrap();
        hybrid.read("file2.dat").unwrap();

        // Read cached file again (cache hit)
        hybrid.read("file1.dat").unwrap();

        // Assert
        let metrics = hybrid.cloud_metrics();
        assert_eq!(metrics.cache_hits, 2, "Should have 2 cache hits");
        assert_eq!(metrics.cache_misses, 1, "Should have 1 cache miss");
        assert!(
            (metrics.cache_hit_ratio - 0.666).abs() < 0.01,
            "Cache hit ratio should be ~66.6%, got {}",
            metrics.cache_hit_ratio
        );
    }

    #[test]
    fn should_track_upload_metrics() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let cache_dir = std::env::temp_dir().join("hybrid_test_upload_metrics");
        let hybrid = HybridStorage::new(cache_dir, backend, 1024).unwrap();

        // Act
        hybrid.write("f1.dat", Bytes::from("data1"), true).unwrap();
        hybrid.write("f2.dat", Bytes::from("data2"), true).unwrap();
        hybrid.write("f3.dat", Bytes::from("data3"), true).unwrap();

        // Assert
        let metrics = hybrid.cloud_metrics();
        assert_eq!(
            metrics.uploads_completed, 3,
            "Should have 3 completed uploads"
        );
        assert_eq!(metrics.uploads_failed, 0, "Should have 0 failed uploads");
        assert!(
            metrics.avg_upload_latency_ms >= 0.0,
            "Average latency should be non-negative"
        );
    }

    #[test]
    fn should_track_upload_latency_percentiles() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let cache_dir = std::env::temp_dir().join("hybrid_test_latency_percentiles");
        let hybrid = HybridStorage::new(cache_dir, backend, 10 * 1024).unwrap();

        // Act
        for i in 0..20 {
            let data = Bytes::from(format!("data_{}", i));
            hybrid.write(&format!("file{}.dat", i), data, true).unwrap();
        }

        // Assert
        let metrics = hybrid.cloud_metrics();
        assert_eq!(metrics.uploads_completed, 20);
        // P50 latency is always non-negative (u64), no need to check
        assert!(
            metrics.p99_upload_latency_ms >= metrics.p50_upload_latency_ms,
            "P99 should be >= P50"
        );
    }

    #[test]
    fn should_track_eviction_metrics() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let cache_dir = std::env::temp_dir().join("hybrid_test_eviction_metrics");
        let _ = std::fs::remove_dir_all(&cache_dir);
        let max_cache = 200;
        let hybrid = HybridStorage::new(cache_dir, backend, max_cache).unwrap();

        // Act
        hybrid
            .write("f1.dat", Bytes::from(vec![0u8; 100]), true)
            .unwrap();
        hybrid
            .write("f2.dat", Bytes::from(vec![1u8; 100]), true)
            .unwrap();
        hybrid
            .write("f3.dat", Bytes::from(vec![2u8; 100]), true)
            .unwrap(); // Triggers eviction

        // Assert
        let metrics = hybrid.cloud_metrics();
        assert!(
            metrics.files_evicted >= 1,
            "Should have evicted at least 1 file, got {}",
            metrics.files_evicted
        );
    }

    #[test]
    fn should_track_async_upload_metrics() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let cache_dir = std::env::temp_dir().join("hybrid_test_async_metrics");
        let hybrid = HybridStorage::new(cache_dir, backend.clone(), 1024).unwrap();

        // Act
        hybrid
            .write("async1.dat", Bytes::from("data1"), false)
            .unwrap();
        hybrid
            .write("async2.dat", Bytes::from("data2"), false)
            .unwrap();

        // Process uploads
        hybrid.process_uploads(10).unwrap();

        // Assert
        let metrics = hybrid.cloud_metrics();
        assert_eq!(
            metrics.uploads_completed, 2,
            "Should have 2 completed async uploads"
        );
        assert!(
            metrics.avg_upload_latency_ms >= 0.0,
            "Should have tracked upload time (non-negative)"
        );
    }

    #[test]
    fn should_show_zero_cache_hit_ratio_with_no_reads() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let cache_dir = std::env::temp_dir().join("hybrid_test_no_reads");
        let hybrid = HybridStorage::new(cache_dir, backend, 1024).unwrap();

        // Act
        let metrics = hybrid.cloud_metrics();

        // Assert
        assert_eq!(metrics.cache_hits, 0);
        assert_eq!(metrics.cache_misses, 0);
        assert_eq!(metrics.cache_hit_ratio, 0.0);
    }

    #[test]
    fn should_track_background_worker_metrics() {
        // Arrange
        let backend = Arc::new(MockCloudBackend::new());
        let cache_dir = std::env::temp_dir().join("hybrid_test_bg_metrics");
        let hybrid = HybridStorage::new(cache_dir, backend.clone(), 1024).unwrap();

        // Act
        hybrid.spawn_background_workers();

        // Async writes
        hybrid
            .write("bg1.dat", Bytes::from("data1"), false)
            .unwrap();
        hybrid
            .write("bg2.dat", Bytes::from("data2"), false)
            .unwrap();

        // Wait for background upload
        std::thread::sleep(std::time::Duration::from_millis(300));

        // Assert
        let metrics = hybrid.cloud_metrics();
        assert_eq!(
            metrics.uploads_completed, 2,
            "Background worker should have uploaded 2 files"
        );
        assert!(
            metrics.avg_upload_latency_ms >= 0.0,
            "Should have tracked latency (non-negative)"
        );

        // Cleanup
        hybrid.shutdown();
    }
}
