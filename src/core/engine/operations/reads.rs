//! Read operations for the Midge engine.
//!
//! This module contains all read-related operations including:
//! - Point reads (get)
//! - Range scans (scan, scan_streaming)
//! - Snapshot reads (get_at, scan_at)

use bytes::Bytes;

use crate::{
    api::{column_family::ColumnFamilyHandle, query::Query, snapshot::Snapshot},
    common::timestamp,
    core::manifest::Manifest,
    error::MidgeResult,
};

use super::super::MidgeEngine;

impl MidgeEngine {
    /// Get a value by key from a specific column family.
    ///
    /// Returns `None` if the key doesn't exist or has been deleted.
    /// Checks memtables first (active + immutable), then SST files from newest to oldest.
    pub fn get(&self, cf: &ColumnFamilyHandle, key: &[u8]) -> MidgeResult<Option<Bytes>> {
        self.metrics.record_get();

        let cf_id = cf.id();
        let column_family = self.cf_set.get_cf(cf_id).ok_or_else(|| {
            crate::error::MidgeError::invalid_config(format!(
                "Column family '{}' does not exist",
                cf.name()
            ))
        })?;

        // Check active memtable first
        {
            let mt = column_family.memtable.read();
            if let Some(v) = mt.get(key) {
                return Ok(Some(v));
            }
        }

        // Check immutable memtables (newest to oldest)
        {
            let immutables = column_family.immutable_memtables.lock();
            // Iterate in reverse order (newest to oldest)
            for immutable_mt in immutables.iter().rev() {
                if let Some(v) = immutable_mt.get(key) {
                    return Ok(Some(v));
                }
            }
        }

        let manifest = self.get_manifest();
        let cf_files: Vec<_> = manifest
            .files
            .iter()
            .filter(|f| f.cf_id == cf_id.as_u32())
            .collect();

        for file in cf_files.iter().rev() {
            let p = self.sst_dir.join(&file.name);
            // CloudSstReaderFactory will download from cloud if not in local cache
            if let Ok(sst) = self.sst_reader_factory.open(&p) {
                match sst.get_state(key) {
                    Ok(crate::sst::KeyState::Value(v, _, expiration)) => {
                        // Check if key is expired
                        if let Some(exp_ts) = expiration {
                            let now_millis = timestamp::now_millis();
                            if exp_ts <= now_millis {
                                // Key is expired, treat as deleted
                                return Ok(None);
                            }
                        }
                        return Ok(Some(v));
                    }
                    Ok(crate::sst::KeyState::Tombstone(_)) => return Ok(None),
                    Ok(crate::sst::KeyState::Absent) => continue,
                    Err(_) => continue,
                }
            }
        }
        Ok(None)
    }

    /// Scan a range of keys in a column family.
    ///
    /// Returns key-value pairs matching the query criteria.
    /// Merges data from memtables and SST files, handling tombstones appropriately.
    pub fn scan(&self, cf: &ColumnFamilyHandle, query: Query) -> MidgeResult<Vec<(Bytes, Bytes)>> {
        let cf_id = cf.id();
        let start = query.start.as_ref().map(|b| b.as_ref());
        let end_ref = query.end.as_ref().map(|b| b.as_ref());

        let column_family = self.cf_set.get_cf(cf_id).ok_or_else(|| {
            crate::error::MidgeError::invalid_config(format!(
                "Column family '{}' does not exist",
                cf.name()
            ))
        })?;

        // Scan active memtable
        let (mem_items, mem_tombs) = {
            let mt = column_family.memtable.read();
            let items = mt
                .scan_range(start, end_ref)
                .into_iter()
                .map(|(k, v)| (k, Some(v), 0u64))
                .collect();
            let tombs = mt
                .tombstones_range(start, end_ref)
                .into_iter()
                .map(|k| (k, None, 0u64))
                .collect();
            (items, tombs)
        };

        // Build sources for merging iterator
        let mut sources: Vec<Box<dyn crate::core::merge_iterator::IteratorSource>> = vec![];
        sources.push(Box::new(crate::core::merge_iterator::VecSource::new(
            mem_items,
        )));
        sources.push(Box::new(crate::core::merge_iterator::VecSource::new(
            mem_tombs,
        )));

        // Scan immutable memtables (newest to oldest)
        {
            let immutables = column_family.immutable_memtables.lock();
            for immutable_mt in immutables.iter().rev() {
                let immut_items: Vec<(Bytes, Option<Bytes>, u64)> = immutable_mt
                    .scan_range(start, end_ref)
                    .into_iter()
                    .map(|(k, v)| (k, Some(v), 0u64))
                    .collect();
                let immut_tombs: Vec<(Bytes, Option<Bytes>, u64)> = immutable_mt
                    .tombstones_range(start, end_ref)
                    .into_iter()
                    .map(|k| (k, None, 0u64))
                    .collect();

                if !immut_items.is_empty() {
                    sources.push(Box::new(crate::core::merge_iterator::VecSource::new(
                        immut_items,
                    )));
                }
                if !immut_tombs.is_empty() {
                    sources.push(Box::new(crate::core::merge_iterator::VecSource::new(
                        immut_tombs,
                    )));
                }
            }
        }

        // Add SST sources for this CF
        let manifest = self.get_manifest();
        let cf_files: Vec<_> = manifest
            .files
            .iter()
            .filter(|f| f.cf_id == cf_id.as_u32())
            .collect();

        for file in &cf_files {
            let p = self.sst_dir.join(&file.name);
            // CloudSstReaderFactory will download from cloud if not in local cache
            if let Ok(sst) = self.sst_reader_factory.open(&p) {
                if let Ok(rows) = sst.scan_range_state(start, end_ref) {
                    let now_millis = timestamp::now_millis();

                    let items: Vec<(Bytes, Option<Bytes>, u64)> = rows
                        .into_iter()
                        .map(|(k, st)| {
                            use crate::sst::KeyState;
                            match st {
                                KeyState::Value(v, _, expiration) => {
                                    // Check if key is expired
                                    if let Some(exp_ts) = expiration {
                                        if exp_ts <= now_millis {
                                            // Key is expired, treat as tombstone
                                            return (k, None, 0);
                                        }
                                    }
                                    (k, Some(v), 0)
                                }
                                KeyState::Tombstone(_) => (k, None, 0),
                                KeyState::Absent => (k, None, 0),
                            }
                        })
                        .collect();
                    if !items.is_empty() {
                        sources.push(Box::new(crate::core::merge_iterator::VecSource::new(items)));
                    }
                }
            }
        }

        // Merge and collect
        let iter = crate::core::merge_iterator::MergingIterator::new(sources, query.limit);
        let results: Vec<(Bytes, Bytes)> = iter.collect();

        if query.reverse {
            Ok(results.into_iter().rev().collect())
        } else {
            Ok(results)
        }
    }

    /// Streaming scan implementation (legacy, uses default CF).
    ///
    /// Note: This implementation is being deprecated in favor of the CF-aware scan().
    pub fn scan_streaming(&self, q: Query) -> MidgeResult<Vec<(Bytes, Bytes)>> {
        use crate::core::merge_iterator::{IteratorSource, MergingIterator, VecSource};

        // Compute effective range based on forward/reverse direction
        let start = q.effective_start();
        let end = q.effective_end();
        let end_ref = end.as_deref();

        let mut sources: Vec<Box<dyn IteratorSource>> = Vec::new();

        let mem_items = self
            .with_default_memtable(|mt| mt.scan_range(start, end_ref))
            .into_iter()
            .map(|(k, v)| (k, Some(v), 0u64))
            .collect();
        if q.reverse {
            sources.push(Box::new(VecSource::new_reverse(mem_items)));
        } else {
            sources.push(Box::new(VecSource::new(mem_items)));
        }

        // Add MemTable tombstones
        let mem_tombs = self
            .with_default_memtable(|mt| mt.tombstones_range(start, end_ref))
            .into_iter()
            .map(|k| (k, None, 0u64))
            .collect();
        if q.reverse {
            sources.push(Box::new(VecSource::new_reverse(mem_tombs)));
        } else {
            sources.push(Box::new(VecSource::new(mem_tombs)));
        }

        let manifest = Manifest::load(&self.db_path).unwrap_or_default();
        for name in manifest.ssts.iter().rev() {
            let p = self.sst_dir.join(name);
            // CloudSstReaderFactory will download from cloud if not in local cache
            if let Ok(sst) = self.sst_reader_factory.open(&p) {
                if let Ok(rows) = sst.scan_range_state(start, end_ref) {
                    let now_millis = timestamp::now_millis();

                    let items: Vec<(Bytes, Option<Bytes>, u64)> = rows
                        .into_iter()
                        .map(|(k, st)| {
                            let user_key = crate::internal_key::decode_internal_key(k.as_ref())
                                .map(|(u, s, _t)| (Bytes::from(u), s))
                                .unwrap_or_else(|| (k, 0));
                            match st {
                                crate::sst::KeyState::Value(v, seq, expiration) => {
                                    // Check if key is expired
                                    if let Some(exp_ts) = expiration {
                                        if exp_ts <= now_millis {
                                            // Key is expired, treat as tombstone
                                            return (user_key.0, None, seq);
                                        }
                                    }
                                    (user_key.0, Some(v), seq)
                                }
                                crate::sst::KeyState::Tombstone(seq) => (user_key.0, None, seq),
                                crate::sst::KeyState::Absent => (user_key.0, None, 0),
                            }
                        })
                        .filter(|(_, val, _)| val.is_some() || val.is_none()) // Keep all (values and tombstones)
                        .collect();
                    if q.reverse {
                        sources.push(Box::new(VecSource::new_reverse(items)));
                    } else {
                        sources.push(Box::new(VecSource::new(items)));
                    }
                }
            }
        }

        // Create merging iterator and collect results
        let iter = MergingIterator::with_reverse(sources, q.limit, q.reverse);
        Ok(iter.collect())
    }

    /// Get at a specific snapshot sequence.
    ///
    /// Returns the value visible at the given snapshot, respecting MVCC semantics.
    pub fn get_at(&self, key: &[u8], snap: &Snapshot) -> MidgeResult<Option<Bytes>> {
        // 1) Check MemTable visible value at snapshot
        if let Some(v) = self.with_default_memtable(|mt| mt.get_at(key, snap.seq)) {
            return Ok(Some(v));
        }
        // 2) If MemTable has a visible tombstone at snapshot, it's deleted
        let end_key = {
            let mut v = key.to_vec();
            v.push(0);
            v
        };
        let tombs = self.with_default_memtable(|mt| {
            mt.tombstones_range_at(Some(key), Some(end_key.as_slice()), snap.seq)
        });
        if !tombs.is_empty() {
            return Ok(None);
        }
        // 3) Probe SSTs newest->oldest using snapshot-aware state
        let manifest = Manifest::load(&self.db_path).unwrap_or_default();
        let now_millis = timestamp::now_millis();

        for name in manifest.ssts.iter().rev() {
            let p = self.sst_dir.join(name);
            // CloudSstReaderFactory will download from cloud if not in local cache
            if let Ok(sst) = self.sst_reader_factory.open(&p) {
                match sst.get_state_at(key, snap.seq) {
                    Ok(crate::sst::KeyState::Value(v, _seq, exp)) => {
                        // Check if value is expired
                        if let Some(exp_millis) = exp {
                            if now_millis >= exp_millis {
                                // Expired, return None (enforcing no-resurrection)
                                return Ok(None);
                            }
                        }
                        return Ok(Some(v));
                    }
                    Ok(crate::sst::KeyState::Tombstone(_seq)) => return Ok(None),
                    Ok(state) => {
                        tracing::debug!(
                            key = ?key,
                            seq = snap.seq,
                            state = ?state,
                            "unexpected key state in get_at"
                        );
                    }
                    Err(e) => {
                        tracing::debug!(error = %e, "get_state_at error in get_at");
                    }
                }
            }
        }
        Ok(None)
    }

    /// Scan at a specific snapshot.
    ///
    /// Returns key-value pairs visible at the given snapshot, respecting MVCC semantics.
    pub fn scan_at(&self, q: Query, snap: &Snapshot) -> MidgeResult<Vec<(Bytes, Bytes)>> {
        let start = q
            .start
            .as_ref()
            .map(|b| b.as_ref())
            .or_else(|| q.prefix.as_ref().map(|p| p.as_ref()));
        let end_from_prefix: Option<Vec<u8>> = q.prefix.as_ref().map(|p| {
            let mut v = p.to_vec();
            v.push(0xFF);
            v
        });
        let end = match (
            q.end.as_ref().map(|b| b.as_ref()),
            end_from_prefix.as_deref(),
        ) {
            (Some(e), _) => Some(e),
            (None, Some(ep)) => Some(ep),
            (None, None) => None,
        };
        // Pre-compute MemTable tombstones visible at snapshot
        let mem_tombs: std::collections::BTreeSet<Vec<u8>> = self
            .with_default_memtable(|mt| mt.tombstones_range_at(start, end, snap.seq))
            .into_iter()
            .map(|b| b.to_vec())
            .collect();

        // Scan MemTable
        let mem_rows = self.with_default_memtable(|mt| mt.scan_range_at(start, end, snap.seq));

        // Scan SSTs newest-to-oldest
        let manifest = Manifest::load(&self.db_path).unwrap_or_default();
        let now_millis = timestamp::now_millis();

        let mut collected: std::collections::BTreeMap<Vec<u8>, Option<Bytes>> =
            std::collections::BTreeMap::new();
        for (k, v) in &mem_rows {
            collected.insert(k.to_vec(), Some(v.clone()));
        }
        for k in &mem_tombs {
            collected.insert(k.clone(), None);
        }

        for name in manifest.ssts.iter().rev() {
            let p = self.sst_dir.join(name);
            if let Ok(sst) = self.sst_reader_factory.open(&p) {
                if let Ok(rows) = sst.scan_range_state_at(start, end, snap.seq) {
                    for (k, st) in rows {
                        let key_vec = k.to_vec();
                        if !collected.contains_key(&key_vec) {
                            match st {
                                crate::sst::KeyState::Value(v, _, exp) => {
                                    // Check if value is expired
                                    if let Some(exp_millis) = exp {
                                        if now_millis >= exp_millis {
                                            // Expired
                                            collected.insert(key_vec, None);
                                            continue;
                                        }
                                    }
                                    collected.insert(key_vec, Some(v));
                                }
                                crate::sst::KeyState::Tombstone(_) => {
                                    collected.insert(key_vec, None);
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
        }

        // Filter out tombstones and apply limit+reverse
        let mut result: Vec<(Bytes, Bytes)> = collected
            .into_iter()
            .filter_map(|(k, v_opt)| v_opt.map(|v| (Bytes::from(k), v)))
            .collect();

        if q.reverse {
            result.reverse();
        }
        if let Some(limit) = q.limit {
            result.truncate(limit);
        }
        Ok(result)
    }
}
