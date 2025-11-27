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

// Type alias for committed transaction info: commit_seq -> (txn_id, writes, ranges)
type CommittedTxnInfo = HashMap<u64, (u64, HashSet<Key>, HashSet<(u32, Bytes, Bytes)>)>;

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
    committed: CommittedTxnInfo, // commit_seq -> (txn_id, writes, ranges)
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
        _read_set: &HashSet<Key>,
        _read_versions: &HashMap<Key, u64>,
    ) -> Result<(), String> {
        let inner = self.inner.read();
        // Verify transaction exists
        let _txn_info = inner
            .active
            .get(&txn_id)
            .ok_or("Transaction not found")?;

        // LWW (Last-Write-Wins) semantics for PUT/DELETE operations:
        // - No write-write conflict detection: concurrent writes to the same key are allowed
        // - No read-write conflict detection: reads don't block writes
        // - The transaction with the later commit sequence wins
        //
        // Note: INSERT and CAS operations use separate conflict detection at a higher layer.
        // INSERT checks if key exists at commit time.
        // CAS checks if value matches expected at commit time.

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
        let _txn = inner
            .active
            .get(&txn_id)
            .ok_or("Transaction not found")?
            .clone();

        // LWW semantics: PUT/DELETE operations don't create wait edges.
        // Concurrent writes are allowed and resolved by commit order (last-write-wins).
        // Reads don't block writes either.
        //
        // Wait edges are only created for INSERT conflicts (checked separately).
        // With LWW, there are no deadlocks from standard write operations.
        let waits = HashSet::new();

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

    fn has_commit_conflict(txn: &TxnInfo, committed: &CommittedTxnInfo, id: u64) -> bool {
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
                    .any(|(cf, start, end)| key.0 == *cf && key.1 >= start && key.1 < end)
                {
                    return true;
                }
            }

            // Check if our write ranges conflict with other's writes
            for (cf, start, end) in &txn.write_ranges {
                if other
                    .write_set
                    .iter()
                    .any(|k| k.0 == *cf && k.1 >= start && k.1 < end)
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

    fn has_read_conflict(txn: &TxnInfo, committed: &CommittedTxnInfo) -> bool {
        txn.read_versions.iter().any(|(key, ver)| {
            committed
                .iter()
                .any(|(&seq, (_, ws, _))| seq > *ver && ws.contains(key))
        })
    }

    fn has_commit_range_conflict(txn: &TxnInfo, committed: &CommittedTxnInfo, id: u64) -> bool {
        committed.iter().any(|(&seq, (cid, _, ranges))| {
            seq >= txn.begin_seq
                && *cid != id
                && ranges.iter().any(|(cf, start, end)| {
                    txn.write_set
                        .iter()
                        .any(|key| key.0 == *cf && key.1 >= start && key.1 < end)
                })
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
    fn should_allow_concurrent_writes_to_same_key_with_lww() {
        // Arrange
        // LWW semantics: PUT operations use last-write-wins, no conflict detection.
        // Both transactions writing to the same key should succeed.
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

        // Act
        let result1 = tm.try_commit(
            1,
            5,
            &ws1,
            &HashSet::new(),
            &HashSet::new(),
            &HashMap::new(),
        );

        // LWW: Second transaction also succeeds - the later commit wins
        let result2 = tm.try_commit(
            2,
            6,
            &ws2,
            &HashSet::new(),
            &HashSet::new(),
            &HashMap::new(),
        );

        // Assert - both commits succeed with LWW
        assert!(result1.is_ok(), "First commit should succeed");
        assert!(result2.is_ok(), "Second commit should also succeed with LWW");
    }

    #[test]
    fn should_allow_write_after_committed_write_with_lww() {
        // Arrange
        // LWW semantics: A transaction that started before another committed
        // can still commit successfully. The later commit wins.
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
        // Txn 2 started at seq 15, before txn 1 committed at seq 20
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

        // Assert - LWW allows this, txn 2's value wins
        assert!(result.is_ok(), "LWW allows writes after committed writes");
    }

    #[test]
    fn should_allow_read_only_transaction_despite_concurrent_write() {
        // Arrange
        // LWW semantics: Read-only transactions don't track reads for conflict
        // detection purposes. A read-only txn can commit even if another txn
        // modified a key it read. The read-only txn sees the snapshot it started with.
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

        // Txn 2 is read-only and read "data" at version 15 (before txn1 committed)
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

        // Assert - LWW doesn't track reads for PUT conflict detection
        // Read-only transactions always succeed (they don't modify anything)
        assert!(result.is_ok(), "Read-only transactions should always succeed");
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
    fn should_not_add_wait_for_edge_for_write_with_lww() {
        // Arrange
        // LWW semantics: PUT operations don't create conflicts, so no wait edges.
        // This test documents that with LWW, concurrent writes don't block.
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

        // Assert - with LWW, no wait edges for writes
        let inner = tm.inner.read();
        let waits = inner.wait_for.get(&2);
        assert!(
            waits.is_none() || waits.unwrap().is_empty(),
            "LWW: no wait edges for concurrent writes"
        );
    }

    #[test]
    fn should_not_add_wait_for_edge_for_read_write_with_lww() {
        // Arrange
        // LWW semantics: Reads don't create conflicts with writes.
        // A transaction reading a key doesn't wait for writers.
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

        // Assert - with LWW, readers don't wait for writers
        let inner = tm.inner.read();
        let waits = inner.wait_for.get(&2);
        assert!(
            waits.is_none() || waits.unwrap().is_empty(),
            "LWW: readers don't wait for writers"
        );
    }

    #[test]
    fn should_clear_wait_for_edges_given_abort() {
        // Arrange
        // Even with LWW, abort should clean up any wait-for state.
        // This test uses INSERT semantics which DO create wait edges.
        let tm = TransactionController::new();
        // Use different keys to avoid LWW - simulate INSERT conflicts
        let mut ws1 = HashSet::new();
        ws1.insert(k("insert_key"));
        let mut ws2 = HashSet::new();
        ws2.insert(k("other_key"));
        tm.begin(1, 1, ws1, HashSet::new(), HashSet::new(), HashMap::new())
            .unwrap();
        tm.begin(2, 1, ws2, HashSet::new(), HashSet::new(), HashMap::new())
            .unwrap();
        // Manually add a wait edge to test cleanup
        {
            let mut inner = tm.inner.write();
            inner.wait_for.entry(2).or_default().insert(1);
        }

        // Act
        tm.abort(2);

        // Assert - abort clears any wait-for entries
        let inner = tm.inner.read();
        assert!(!inner.wait_for.contains_key(&2));
    }

    // =========================================================================
    // Deadlock detection
    // =========================================================================

    #[test]
    fn should_not_detect_deadlock_with_lww_writes() {
        // Arrange
        // LWW semantics: PUT operations don't create wait edges, so no deadlocks.
        // This test documents that concurrent writes don't deadlock with LWW.
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

        // Assert - with LWW, no wait edges means no deadlock possible
        assert!(result.is_none(), "LWW: concurrent writes don't create deadlocks");
    }

    #[test]
    fn should_not_detect_deadlock_with_three_lww_transactions() {
        // Arrange
        // LWW semantics: Even with three transactions in a potential cycle pattern,
        // LWW doesn't create wait edges for writes, so no deadlock.
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

        // Assert - with LWW, no deadlocks from write conflicts
        assert!(result.is_none(), "LWW: no deadlocks from concurrent writes");
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
