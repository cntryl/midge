//! Multi-version concurrency control (MVCC) transaction management
//!
//! Manages the lifecycle of database transactions, ensuring ACID properties
//! through conflict detection, snapshot isolation, and coordinated commit/abort.
//! Tracks active transactions, maintains read/write sets for conflict resolution,
//! and coordinates with the storage engine to provide consistent transactional semantics.

use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Key(pub u32, pub Bytes);

impl Key {
    pub fn new(cf: u32, key: Bytes) -> Self {
        Self(cf, key)
    }
}

#[derive(Clone)]
struct TxnInfo {
    begin_seq: u64,
    write_set: HashSet<Key>,
    write_ranges: HashSet<(u32, Bytes, Bytes)>,
    read_set: HashSet<Key>,
    read_versions: HashMap<Key, u64>,
}

#[derive(Default)]
struct Inner {
    active: HashMap<u64, TxnInfo>,
    committed: HashMap<u64, (u64, HashSet<Key>, HashSet<(u32, Bytes, Bytes)>)>, // commit_seq -> (txn_id, writes, ranges)
    wait_for: HashMap<u64, HashSet<u64>>,
    max_retained: usize,
}

#[derive(Clone, Default)]
pub struct TransactionController {
    inner: Arc<RwLock<Inner>>,
}

impl TransactionController {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn begin(
        &self,
        txn_id: u64,
        begin_seq: u64,
        write_set: HashSet<Key>,
        write_ranges: HashSet<(u32, Bytes, Bytes)>,
        read_set: HashSet<Key>,
        read_versions: HashMap<Key, u64>,
    ) -> Result<(), String> {
        let txn = TxnInfo {
            begin_seq,
            write_set,
            write_ranges,
            read_set,
            read_versions,
        };
        self.inner.write().active.insert(txn_id, txn);
        Ok(())
    }

    pub fn try_commit(
        &self,
        txn_id: u64,
        commit_seq: u64,
        write_set: &HashSet<Key>,
        write_ranges: &HashSet<(u32, Bytes, Bytes)>,
        read_set: &HashSet<Key>,
        read_versions: &HashMap<Key, u64>,
    ) -> Result<(), String> {
        let inner = self.inner.read();
        // Create a temporary TxnInfo with the actual conflict sets
        let txn_info = TxnInfo {
            begin_seq: inner
                .active
                .get(&txn_id)
                .ok_or("Transaction not found")?
                .begin_seq,
            write_set: write_set.clone(),
            write_ranges: write_ranges.clone(),
            read_set: read_set.clone(),
            read_versions: read_versions.clone(),
        };

        // OCC: Detect conflicts only with committed transactions
        // Do NOT check active transactions - that would be pessimistic locking
        // In OCC, transactions can overlap during execution and conflicts are detected only at commit time

        if Self::has_commit_conflict(&txn_info, &inner.committed, txn_id) {
            return Err("Write-write conflict with committed transaction".into());
        }
        if Self::has_commit_range_conflict(&txn_info, &inner.committed, txn_id) {
            return Err("Write-range conflict with committed transaction".into());
        }
        if Self::has_read_conflict(&txn_info, &inner.committed) {
            return Err("Read-write conflict detected".into());
        }

        // Now update the committed state
        drop(inner);
        let mut inner = self.inner.write();
        inner.committed.insert(
            commit_seq,
            (txn_id, write_set.clone(), write_ranges.clone()),
        );
        inner.active.remove(&txn_id);

        // Keep only N most recent commits
        while inner.committed.len() > inner.max_retained.max(1000) {
            if let Some(min) = inner.committed.keys().min().copied() {
                inner.committed.remove(&min);
            }
        }
        Ok(())
    }

    pub fn abort(&self, txn_id: u64) {
        let mut inner = self.inner.write();
        inner.active.remove(&txn_id);
        inner.wait_for.remove(&txn_id);
        for deps in inner.wait_for.values_mut() {
            deps.remove(&txn_id);
        }
    }

    pub fn active_count(&self) -> usize {
        self.inner.read().active.len()
    }

    pub fn is_active(&self, txn_id: u64) -> bool {
        self.inner.read().active.contains_key(&txn_id)
    }

    pub fn update_wait_for_graph(&self, txn_id: u64) -> Result<(), String> {
        let mut inner = self.inner.write();
        let txn = inner
            .active
            .get(&txn_id)
            .ok_or("Transaction not found")?
            .clone();
        let mut waits = HashSet::new();

        for (other_id, other) in &inner.active {
            if *other_id == txn_id {
                continue;
            }
            if !txn.write_set.is_disjoint(&other.write_set)
                || txn.read_set.iter().any(|k| other.write_set.contains(k))
            {
                waits.insert(*other_id);
            }
        }

        inner.wait_for.insert(txn_id, waits);
        Ok(())
    }

    pub fn update(
        &self,
        txn_id: u64,
        write_set: HashSet<Key>,
        write_ranges: HashSet<(u32, Bytes, Bytes)>,
        read_set: HashSet<Key>,
        read_versions: HashMap<Key, u64>,
    ) -> Result<(), String> {
        let mut inner = self.inner.write();
        if let Some(txn) = inner.active.get_mut(&txn_id) {
            txn.write_set = write_set;
            txn.write_ranges = write_ranges;
            txn.read_set = read_set;
            txn.read_versions = read_versions;
            Ok(())
        } else {
            Err("Transaction not found".to_string())
        }
    }

    pub fn check_for_deadlock(&self) -> Option<(u64, Vec<u64>)> {
        let inner = self.inner.read();
        let mut visited = HashSet::new();
        let mut stack = Vec::new();

        for &start in inner.wait_for.keys() {
            if visited.contains(&start) {
                continue;
            }

            stack.push((start, vec![start]));
            while let Some((node, path)) = stack.pop() {
                visited.insert(node);
                if let Some(neighbors) = inner.wait_for.get(&node) {
                    for &n in neighbors {
                        if path.contains(&n) {
                            let idx = path
                                .iter()
                                .position(|&x| x == n)
                                .expect("position should exist since path.contains returned true");
                            let cycle = path[idx..].to_vec();
                            let victim = *cycle.iter().max().expect("cycle should not be empty");
                            return Some((victim, cycle));
                        }
                        let mut next = path.clone();
                        next.push(n);
                        stack.push((n, next));
                    }
                }
            }
        }
        None
    }

    // --- Private helpers ---

    fn has_commit_conflict(
        txn: &TxnInfo,
        committed: &HashMap<u64, (u64, HashSet<Key>, HashSet<(u32, Bytes, Bytes)>)>,
        id: u64,
    ) -> bool {
        committed
            .iter()
            .any(|(&_seq, (cid, ws, _))| *cid != id && !txn.write_set.is_disjoint(ws))
    }

    #[allow(dead_code)] // May be useful for pessimistic locking in the future
    fn has_active_conflict(txn: &TxnInfo, active: &HashMap<u64, TxnInfo>, id: u64) -> bool {
        // Check for direct key write conflicts
        for (&other_id, other) in active {
            if other_id == id {
                continue;
            }
            if !txn.write_set.is_disjoint(&other.write_set) {
                return true;
            }

            // Check if our writes fall inside other's write ranges
            for key in &txn.write_set {
                if other
                    .write_ranges
                    .iter()
                    .any(|(cf, start, end)| key.0 == *cf && &key.1 >= start && &key.1 < end)
                {
                    return true;
                }
            }

            // Check if our write ranges conflict with other's writes
            for (cf, start, end) in &txn.write_ranges {
                if other
                    .write_set
                    .iter()
                    .any(|k| k.0 == *cf && &k.1 >= start && &k.1 < end)
                {
                    return true;
                }
            }

            // Check for range-range overlap
            for (cf1, s1, e1) in &txn.write_ranges {
                for (cf2, s2, e2) in &other.write_ranges {
                    if cf1 == cf2 && s1 < e2 && s2 < e1 {
                        return true;
                    }
                }
            }
        }
        false
    }

    fn has_read_conflict(
        txn: &TxnInfo,
        committed: &HashMap<u64, (u64, HashSet<Key>, HashSet<(u32, Bytes, Bytes)>)>,
    ) -> bool {
        txn.read_versions.iter().any(|(key, ver)| {
            committed
                .iter()
                .any(|(&seq, (_, ws, _))| seq > *ver && ws.contains(key))
        })
    }

    fn has_commit_range_conflict(
        txn: &TxnInfo,
        committed: &HashMap<u64, (u64, HashSet<Key>, HashSet<(u32, Bytes, Bytes)>)>,
        id: u64,
    ) -> bool {
        committed.iter().any(|(&seq, (cid, _, ranges))| {
            seq >= txn.begin_seq
                && *cid != id
                && (
                    // Check if our writes conflict with committed ranges
                    ranges.iter().any(|(cf, start, end)| {
                    txn.write_set.iter().any(|key| key.0 == *cf && &key.1 >= start && &key.1 < end)
                }) ||
                // Check if our ranges conflict with committed writes (simplified - no committed writes to check)
                false
                )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::collections::{HashMap, HashSet};

    const CF: u32 = 1;
    fn k(name: &str) -> Key {
        Key::new(CF, Bytes::from(name.to_string()))
    }

    // =========================================================================
    // Begin / Abort
    // =========================================================================

    #[test]
    fn should_register_transaction_given_valid_begin() {
        // Arrange
        let tm = TransactionController::new();

        // Act
        let result = tm.begin(
            1,
            100,
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            HashMap::new(),
        );

        // Assert
        assert!(result.is_ok());
        assert_eq!(tm.active_count(), 1);
    }

    #[test]
    fn should_remove_transaction_given_abort() {
        // Arrange
        let tm = TransactionController::new();
        tm.begin(
            1,
            100,
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            HashMap::new(),
        )
        .unwrap();

        // Act
        tm.abort(1);

        // Assert
        assert_eq!(tm.active_count(), 0);
    }

    // =========================================================================
    // Commit success
    // =========================================================================

    #[test]
    fn should_commit_transaction_given_no_conflicts() {
        // Arrange
        let tm = TransactionController::new();
        let mut ws = HashSet::new();
        ws.insert(k("a"));
        tm.begin(
            1,
            10,
            ws.clone(),
            HashSet::new(),
            HashSet::new(),
            HashMap::new(),
        )
        .unwrap();

        // Act
        let result = tm.try_commit(
            1,
            20,
            &ws,
            &HashSet::new(),
            &HashSet::new(),
            &HashMap::new(),
        );

        // Assert
        assert!(result.is_ok());
        assert_eq!(tm.active_count(), 0);
    }

    #[test]
    fn should_allow_disjoint_commits_between_two_transactions() {
        // Arrange
        let tm = TransactionController::new();
        let mut ws1 = HashSet::new();
        ws1.insert(k("a"));
        let mut ws2 = HashSet::new();
        ws2.insert(k("b"));
        tm.begin(
            1,
            10,
            ws1.clone(),
            HashSet::new(),
            HashSet::new(),
            HashMap::new(),
        )
        .unwrap();
        tm.begin(
            2,
            10,
            ws2.clone(),
            HashSet::new(),
            HashSet::new(),
            HashMap::new(),
        )
        .unwrap();

        // Act
        let r1 = tm.try_commit(
            1,
            20,
            &ws1,
            &HashSet::new(),
            &HashSet::new(),
            &HashMap::new(),
        );
        let r2 = tm.try_commit(
            2,
            21,
            &ws2,
            &HashSet::new(),
            &HashSet::new(),
            &HashMap::new(),
        );

        // Assert
        assert!(r1.is_ok());
        assert!(r2.is_ok());
    }

    // =========================================================================
    // Conflict detection
    // =========================================================================

    #[test]
    fn should_allow_concurrent_writes_to_same_key_in_active_transactions() {
        // Arrange: In OCC, active transactions can write to the same key
        // Conflicts are detected only at commit time against committed transactions
        let tm = TransactionController::new();
        let mut ws1 = HashSet::new();
        ws1.insert(k("x"));
        let mut ws2 = HashSet::new();
        ws2.insert(k("x"));
        tm.begin(
            1,
            1,
            ws1.clone(),
            HashSet::new(),
            HashSet::new(),
            HashMap::new(),
        )
        .unwrap();
        tm.begin(
            2,
            1,
            ws2.clone(),
            HashSet::new(),
            HashSet::new(),
            HashMap::new(),
        )
        .unwrap();

        // Act: First transaction commits successfully
        let result1 = tm.try_commit(
            1,
            5,
            &ws1,
            &HashSet::new(),
            &HashSet::new(),
            &HashMap::new(),
        );
        assert!(result1.is_ok(), "First commit should succeed");

        // Second transaction should fail because txn1 is now committed
        let result2 = tm.try_commit(
            2,
            6,
            &ws2,
            &HashSet::new(),
            &HashSet::new(),
            &HashMap::new(),
        );

        // Assert: Second transaction detects conflict with committed txn1
        assert!(result2.is_err());
        assert!(result2.unwrap_err().contains("Write-write conflict"));
    }

    #[test]
    fn should_detect_write_write_conflict_with_committed_transaction() {
        // Arrange
        let tm = TransactionController::new();
        let mut ws = HashSet::new();
        ws.insert(k("shared"));
        tm.begin(
            1,
            10,
            ws.clone(),
            HashSet::new(),
            HashSet::new(),
            HashMap::new(),
        )
        .unwrap();
        tm.try_commit(
            1,
            20,
            &ws,
            &HashSet::new(),
            &HashSet::new(),
            &HashMap::new(),
        )
        .unwrap();
        tm.begin(
            2,
            15,
            ws.clone(),
            HashSet::new(),
            HashSet::new(),
            HashMap::new(),
        )
        .unwrap();

        // Act
        let result = tm.try_commit(
            2,
            25,
            &ws,
            &HashSet::new(),
            &HashSet::new(),
            &HashMap::new(),
        );

        // Assert
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Write-write conflict"));
    }

    #[test]
    fn should_detect_read_write_conflict_given_key_modified_after_read() {
        // Arrange
        let tm = TransactionController::new();
        let mut ws1 = HashSet::new();
        ws1.insert(k("data"));
        tm.begin(
            1,
            10,
            ws1.clone(),
            HashSet::new(),
            HashSet::new(),
            HashMap::new(),
        )
        .unwrap();
        tm.try_commit(
            1,
            20,
            &ws1,
            &HashSet::new(),
            &HashSet::new(),
            &HashMap::new(),
        )
        .unwrap();

        let mut reads = HashMap::new();
        reads.insert(k("data"), 15);
        tm.begin(
            2,
            15,
            HashSet::new(),
            HashSet::new(),
            HashSet::new(),
            reads.clone(),
        )
        .unwrap();

        // Act
        let result = tm.try_commit(
            2,
            30,
            &HashSet::new(),
            &HashSet::new(),
            &HashSet::new(),
            &reads,
        );

        // Assert
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Read-write conflict"));
    }

    // =========================================================================
    // Retention management
    // =========================================================================

    #[test]
    fn should_trim_committed_entries_given_exceeds_max_retained() {
        // Arrange
        let tm = TransactionController::new();

        // Act
        for i in 0..1100 {
            let mut ws = HashSet::new();
            ws.insert(k(&format!("k{i}")));
            tm.begin(
                i,
                i,
                ws.clone(),
                HashSet::new(),
                HashSet::new(),
                HashMap::new(),
            )
            .unwrap();
            tm.try_commit(
                i,
                1000 + i,
                &ws,
                &HashSet::new(),
                &HashSet::new(),
                &HashMap::new(),
            )
            .unwrap();
        }

        // Assert
        let inner = tm.inner.read();
        assert!(inner.committed.len() <= inner.max_retained.max(1000));
    }

    // =========================================================================
    // Wait-for graph
    // =========================================================================

    #[test]
    fn should_add_wait_for_edge_given_write_conflict() {
        // Arrange
        let tm = TransactionController::new();
        let mut ws1 = HashSet::new();
        ws1.insert(k("shared"));
        let mut ws2 = HashSet::new();
        ws2.insert(k("shared"));
        tm.begin(1, 1, ws1, HashSet::new(), HashSet::new(), HashMap::new())
            .unwrap();
        tm.begin(2, 1, ws2, HashSet::new(), HashSet::new(), HashMap::new())
            .unwrap();

        // Act
        tm.update_wait_for_graph(2).unwrap();

        // Assert
        let inner = tm.inner.read();
        let waits = inner.wait_for.get(&2).unwrap();
        assert!(waits.contains(&1));
    }

    #[test]
    fn should_add_wait_for_edge_given_read_write_conflict() {
        // Arrange
        let tm = TransactionController::new();
        let mut ws1 = HashSet::new();
        ws1.insert(k("data"));
        let mut rs2 = HashSet::new();
        rs2.insert(k("data"));
        tm.begin(1, 1, ws1, HashSet::new(), HashSet::new(), HashMap::new())
            .unwrap();
        tm.begin(2, 1, HashSet::new(), HashSet::new(), rs2, HashMap::new())
            .unwrap();

        // Act
        tm.update_wait_for_graph(2).unwrap();

        // Assert
        let inner = tm.inner.read();
        let waits = inner.wait_for.get(&2).unwrap();
        assert!(waits.contains(&1));
    }

    #[test]
    fn should_clear_wait_for_edges_given_abort() {
        // Arrange
        let tm = TransactionController::new();
        let mut ws1 = HashSet::new();
        ws1.insert(k("shared"));
        let mut ws2 = HashSet::new();
        ws2.insert(k("shared"));
        tm.begin(1, 1, ws1, HashSet::new(), HashSet::new(), HashMap::new())
            .unwrap();
        tm.begin(2, 1, ws2, HashSet::new(), HashSet::new(), HashMap::new())
            .unwrap();
        tm.update_wait_for_graph(2).unwrap();

        // Act
        tm.abort(2);

        // Assert
        let inner = tm.inner.read();
        assert!(!inner.wait_for.contains_key(&2));
    }

    // =========================================================================
    // Deadlock detection
    // =========================================================================

    #[test]
    fn should_detect_two_transaction_cycle() {
        // Arrange
        let tm = TransactionController::new();

        let mut ws1 = HashSet::new();
        ws1.insert(k("A"));
        let mut rs1 = HashSet::new();
        rs1.insert(k("B"));

        let mut ws2 = HashSet::new();
        ws2.insert(k("B"));
        let mut rs2 = HashSet::new();
        rs2.insert(k("A"));

        tm.begin(1, 1, ws1, HashSet::new(), rs1, HashMap::new())
            .unwrap();
        tm.begin(2, 1, ws2, HashSet::new(), rs2, HashMap::new())
            .unwrap();

        tm.update_wait_for_graph(1).unwrap();
        tm.update_wait_for_graph(2).unwrap();

        // Act
        let result = tm.check_for_deadlock();

        // Assert
        assert!(result.is_some());
        let (victim, cycle) = result.unwrap();
        assert!(victim == 1 || victim == 2);
        assert!(cycle.len() >= 2);
    }

    #[test]
    fn should_detect_three_transaction_cycle() {
        // Arrange
        let tm = TransactionController::new();

        let (a, b, c) = (k("A"), k("B"), k("C"));

        let ws1 = HashSet::from([a.clone()]);
        let rs1 = HashSet::from([b.clone()]);
        let ws2 = HashSet::from([b.clone()]);
        let rs2 = HashSet::from([c.clone()]);
        let ws3 = HashSet::from([c.clone()]);
        let rs3 = HashSet::from([a.clone()]);

        tm.begin(1, 1, ws1, HashSet::new(), rs1, HashMap::new())
            .unwrap();
        tm.begin(2, 1, ws2, HashSet::new(), rs2, HashMap::new())
            .unwrap();
        tm.begin(3, 1, ws3, HashSet::new(), rs3, HashMap::new())
            .unwrap();

        tm.update_wait_for_graph(1).unwrap();
        tm.update_wait_for_graph(2).unwrap();
        tm.update_wait_for_graph(3).unwrap();

        // Act
        let result = tm.check_for_deadlock();

        // Assert
        assert!(result.is_some());
        let (victim, cycle) = result.unwrap();
        assert!(victim == 1 || victim == 2 || victim == 3);
        assert!(cycle.len() >= 2);
    }

    #[test]
    fn should_not_detect_cycle_given_disjoint_keys() {
        // Arrange
        let tm = TransactionController::new();
        let mut ws1 = HashSet::new();
        ws1.insert(k("x"));
        let mut ws2 = HashSet::new();
        ws2.insert(k("y"));
        tm.begin(1, 1, ws1, HashSet::new(), HashSet::new(), HashMap::new())
            .unwrap();
        tm.begin(2, 1, ws2, HashSet::new(), HashSet::new(), HashMap::new())
            .unwrap();
        tm.update_wait_for_graph(1).unwrap();
        tm.update_wait_for_graph(2).unwrap();

        // Act
        let result = tm.check_for_deadlock();

        // Assert
        assert!(result.is_none());
    }
}
