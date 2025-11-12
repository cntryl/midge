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
    /// Resolves merge operations if a merge operator is registered for the CF.
    pub fn get(&self, cf: &ColumnFamilyHandle, key: &[u8]) -> MidgeResult<Option<Bytes>> {
        self.metrics.record_get();

        let cf_id = cf.id();
        let column_family = self.cf_set.get_cf(cf_id).ok_or_else(|| {
            crate::error::MidgeError::invalid_config(format!(
                "Column family '{}' does not exist",
                cf.name()
            ))
        })?;

        // Check active memtable first - check for merge operations
        {
            let mt = column_family.memtable.read();
            let versions = mt.get_versions_for_merge(key, u64::MAX);

            if !versions.is_empty() {
                // Check if there are any merge operations
                let has_merges = versions
                    .iter()
                    .any(|(_, _, op)| *op == crate::core::data_structures::skiplist::OpType::Merge);

                if has_merges {
                    // Resolve merges using the engine's merge operator registry
                    let ops = self.merge_operators.read();
                    if let Some(merge_op) = ops.get(&cf_id.as_u32()) {
                        // Collect merge operands and base value (oldest to newest)
                        let mut operands: Vec<Bytes> = Vec::new();
                        let mut base_value: Option<Bytes> = None;

                        for (value_opt, _exp, op_type) in versions.iter().rev() {
                            match op_type {
                                crate::core::data_structures::skiplist::OpType::Put => {
                                    base_value = value_opt.clone();
                                    operands.clear();
                                }
                                crate::core::data_structures::skiplist::OpType::Merge => {
                                    if let Some(val) = value_opt {
                                        operands.push(val.clone());
                                    }
                                }
                                crate::core::data_structures::skiplist::OpType::Delete => {
                                    base_value = None;
                                    operands.clear();
                                }
                            }
                        }

                        if !operands.is_empty() {
                            let operand_refs: Vec<&[u8]> =
                                operands.iter().map(|b| b.as_ref()).collect();
                            if let Ok(resolved) =
                                merge_op.merge_many(key, base_value.as_deref(), &operand_refs)
                            {
                                return Ok(Some(Bytes::from(resolved)));
                            }
                        } else if let Some(base) = base_value {
                            return Ok(Some(base));
                        }
                    }
                } else {
                    // No merges, just return the latest value
                    if let Some(v) = mt.get(key) {
                        return Ok(Some(v));
                    }
                }
            } else if let Some(v) = mt.get(key) {
                return Ok(Some(v));
            }
        }

        // Check immutable memtables (newest to oldest)
        {
            let immutables = column_family.immutable_memtables.lock();
            // Iterate in reverse order (newest to oldest)
            for immutable_mt in immutables.iter().rev() {
                let versions = immutable_mt.get_versions_for_merge(key, u64::MAX);

                if !versions.is_empty() {
                    let has_merges = versions
                        .iter()
                        .any(|(_, _, op)| *op == crate::core::data_structures::skiplist::OpType::Merge);

                    if has_merges {
                        let ops = self.merge_operators.read();
                        if let Some(merge_op) = ops.get(&cf_id.as_u32()) {
                            let mut operands: Vec<Bytes> = Vec::new();
                            let mut base_value: Option<Bytes> = None;

                            for (value_opt, _exp, op_type) in versions.iter().rev() {
                                match op_type {
                                    crate::core::data_structures::skiplist::OpType::Put => {
                                        base_value = value_opt.clone();
                                        operands.clear();
                                    }
                                    crate::core::data_structures::skiplist::OpType::Merge => {
                                        if let Some(val) = value_opt {
                                            operands.push(val.clone());
                                        }
                                    }
                                    crate::core::data_structures::skiplist::OpType::Delete => {
                                        base_value = None;
                                        operands.clear();
                                    }
                                }
                            }

                            if !operands.is_empty() {
                                let operand_refs: Vec<&[u8]> =
                                    operands.iter().map(|b| b.as_ref()).collect();
                                if let Ok(resolved) =
                                    merge_op.merge_many(key, base_value.as_deref(), &operand_refs)
                                {
                                    return Ok(Some(Bytes::from(resolved)));
                                }
                            } else if let Some(base) = base_value {
                                return Ok(Some(base));
                            }
                        }
                    } else if let Some(v) = immutable_mt.get(key) {
                        return Ok(Some(v));
                    }
                } else if let Some(v) = immutable_mt.get(key) {
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
        let start = query.effective_start();
        let end_owned = query.effective_end();
        let end_ref = end_owned.as_deref();

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
        if query.reverse {
            sources.push(Box::new(
                crate::core::merge_iterator::VecSource::new_reverse(mem_items),
            ));
            sources.push(Box::new(
                crate::core::merge_iterator::VecSource::new_reverse(mem_tombs),
            ));
        } else {
            sources.push(Box::new(crate::core::merge_iterator::VecSource::new(
                mem_items,
            )));
            sources.push(Box::new(crate::core::merge_iterator::VecSource::new(
                mem_tombs,
            )));
        }

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
                    if query.reverse {
                        sources.push(Box::new(
                            crate::core::merge_iterator::VecSource::new_reverse(immut_items),
                        ));
                    } else {
                        sources.push(Box::new(crate::core::merge_iterator::VecSource::new(
                            immut_items,
                        )));
                    }
                }
                if !immut_tombs.is_empty() {
                    if query.reverse {
                        sources.push(Box::new(
                            crate::core::merge_iterator::VecSource::new_reverse(immut_tombs),
                        ));
                    } else {
                        sources.push(Box::new(crate::core::merge_iterator::VecSource::new(
                            immut_tombs,
                        )));
                    }
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
                        if query.reverse {
                            sources.push(Box::new(
                                crate::core::merge_iterator::VecSource::new_reverse(items),
                            ));
                        } else {
                            sources
                                .push(Box::new(crate::core::merge_iterator::VecSource::new(items)));
                        }
                    }
                }
            }
        }

        // Merge and collect (with proper reverse handling)
        let iter = crate::core::merge_iterator::MergingIterator::with_reverse(
            sources,
            query.limit,
            query.reverse,
        );
        let results: Vec<(Bytes, Bytes)> = iter.collect();
        Ok(results)
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
                            let user_key =
                                crate::common::internal_key::decode_internal_key(k.as_ref())
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

    /// Get at a specific snapshot sequence from a specific column family.
    ///
    /// Returns the value visible at the given snapshot, respecting MVCC semantics.
    pub fn get_at(
        &self,
        cf: &ColumnFamilyHandle,
        key: &[u8],
        snap: &Snapshot,
    ) -> MidgeResult<Option<Bytes>> {
        let cf_id = cf.id();
        let column_family = self.cf_set.get_cf(cf_id).ok_or_else(|| {
            crate::error::MidgeError::invalid_config(format!(
                "Column family '{}' does not exist",
                cf.name()
            ))
        })?;

        // 1) Check active memtable visible value at snapshot
        {
            let mt = column_family.memtable.read();
            if let Some(v) = mt.get_at(key, snap.seq) {
                return Ok(Some(v));
            }
        }

        // 2) Check immutable memtables (newest to oldest)
        {
            let immutables = column_family.immutable_memtables.lock();
            for immutable_mt in immutables.iter().rev() {
                if let Some(v) = immutable_mt.get_at(key, snap.seq) {
                    return Ok(Some(v));
                }
            }
        }

        // 3) Check if MemTable has a visible tombstone at snapshot
        let end_key = {
            let mut v = key.to_vec();
            v.push(0);
            v
        };
        let tombs = {
            let mt = column_family.memtable.read();
            mt.tombstones_range_at(Some(key), Some(end_key.as_slice()), snap.seq)
        };
        if !tombs.is_empty() {
            return Ok(None);
        }

        // 4) Probe SSTs newest->oldest using snapshot-aware state, filtered by CF
        let manifest = self.get_manifest();
        let now_millis = timestamp::now_millis();

        let cf_files: Vec<_> = manifest
            .files
            .iter()
            .filter(|f| f.cf_id == cf_id.as_u32())
            .collect();

        for file in cf_files.iter().rev() {
            let p = self.sst_dir.join(&file.name);
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

    /// Scan at a specific snapshot from a specific column family.
    ///
    /// Returns key-value pairs visible at the given snapshot, respecting MVCC semantics.
    pub fn scan_at(
        &self,
        cf: &ColumnFamilyHandle,
        q: Query,
        snap: &Snapshot,
    ) -> MidgeResult<Vec<(Bytes, Bytes)>> {
        let cf_id = cf.id();
        let column_family = self.cf_set.get_cf(cf_id).ok_or_else(|| {
            crate::error::MidgeError::invalid_config(format!(
                "Column family '{}' does not exist",
                cf.name()
            ))
        })?;

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

        // Pre-compute MemTable tombstones visible at snapshot (active + immutable)
        let mut mem_tombs: std::collections::BTreeSet<Vec<u8>> = std::collections::BTreeSet::new();
        {
            let mt = column_family.memtable.read();
            let tombs = mt.tombstones_range_at(start, end, snap.seq);
            mem_tombs.extend(tombs.into_iter().map(|b| b.to_vec()));
        }
        {
            let immutables = column_family.immutable_memtables.lock();
            for immutable_mt in immutables.iter() {
                let tombs = immutable_mt.tombstones_range_at(start, end, snap.seq);
                mem_tombs.extend(tombs.into_iter().map(|b| b.to_vec()));
            }
        }

        // Scan MemTable (active + immutable)
        let mut mem_rows = Vec::new();
        {
            let mt = column_family.memtable.read();
            mem_rows.extend(mt.scan_range_at(start, end, snap.seq));
        }
        {
            let immutables = column_family.immutable_memtables.lock();
            for immutable_mt in immutables.iter() {
                mem_rows.extend(immutable_mt.scan_range_at(start, end, snap.seq));
            }
        }

        // Scan SSTs newest-to-oldest, filtered by CF
        let manifest = self.get_manifest();
        let now_millis = timestamp::now_millis();

        let cf_files: Vec<_> = manifest
            .files
            .iter()
            .filter(|f| f.cf_id == cf_id.as_u32())
            .collect();

        let mut collected: std::collections::BTreeMap<Vec<u8>, Option<Bytes>> =
            std::collections::BTreeMap::new();
        for (k, v) in &mem_rows {
            collected.insert(k.to_vec(), Some(v.clone()));
        }
        for k in &mem_tombs {
            collected.insert(k.clone(), None);
        }

        for file in cf_files.iter().rev() {
            let p = self.sst_dir.join(&file.name);
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
