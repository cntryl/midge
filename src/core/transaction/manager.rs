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
    read_set: HashSet<Key>,
    read_versions: HashMap<Key, u64>,
}

#[derive(Default)]
struct Inner {
    active: HashMap<u64, TxnInfo>,
    committed: HashMap<u64, (u64, HashSet<Key>)>, // commit_seq -> (txn_id, writes)
    wait_for: HashMap<u64, HashSet<u64>>,
    max_retained: usize,
}

#[derive(Clone, Default)]
pub struct TransactionManager {
    inner: Arc<RwLock<Inner>>,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn begin(
        &self,
        txn_id: u64,
        begin_seq: u64,
        write_set: HashSet<Key>,
        read_set: HashSet<Key>,
        read_versions: HashMap<Key, u64>,
    ) -> Result<(), String> {
        let txn = TxnInfo {
            begin_seq,
            write_set,
            read_set,
            read_versions,
        };
        self.inner.write().active.insert(txn_id, txn);
        Ok(())
    }

    pub fn try_commit(&self, txn_id: u64, commit_seq: u64) -> Result<(), String> {
        let mut inner = self.inner.write();
        let txn = inner
            .active
            .get(&txn_id)
            .ok_or_else(|| "Transaction not found".to_string())?
            .clone();

        if Self::has_write_conflict(&txn, &inner.active, txn_id) {
            return Err("Write-write conflict with active transaction".into());
        }
        if Self::has_commit_conflict(&txn, &inner.committed, txn_id) {
            return Err("Write-write conflict with committed transaction".into());
        }
        if Self::has_read_conflict(&txn, &inner.committed) {
            return Err("Read-write conflict detected".into());
        }

        inner.committed.insert(commit_seq, (txn_id, txn.write_set));
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
                            let idx = path.iter().position(|&x| x == n)
                                .expect("position should exist since path.contains returned true");
                            let cycle = path[idx..].to_vec();
                            let victim = *cycle.iter().max()
                                .expect("cycle should not be empty");
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

    fn has_write_conflict(txn: &TxnInfo, active: &HashMap<u64, TxnInfo>, id: u64) -> bool {
        active
            .iter()
            .any(|(other_id, o)| *other_id != id && !txn.write_set.is_disjoint(&o.write_set))
    }

    fn has_commit_conflict(
        txn: &TxnInfo,
        committed: &HashMap<u64, (u64, HashSet<Key>)>,
        id: u64,
    ) -> bool {
        committed.iter().any(|(&seq, (cid, ws))| {
            seq >= txn.begin_seq && *cid != id && !txn.write_set.is_disjoint(ws)
        })
    }

    fn has_read_conflict(txn: &TxnInfo, committed: &HashMap<u64, (u64, HashSet<Key>)>) -> bool {
        txn.read_versions.iter().any(|(key, ver)| {
            committed
                .iter()
                .any(|(&seq, (_, ws))| seq > *ver && ws.contains(key))
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
        let tm = TransactionManager::new();

        // Act
        let result = tm.begin(1, 100, HashSet::new(), HashSet::new(), HashMap::new());

        // Assert
        assert!(result.is_ok());
        assert_eq!(tm.active_count(), 1);
    }

    #[test]
    fn should_remove_transaction_given_abort() {
        // Arrange
        let tm = TransactionManager::new();
        tm.begin(1, 100, HashSet::new(), HashSet::new(), HashMap::new())
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
        let tm = TransactionManager::new();
        let mut ws = HashSet::new();
        ws.insert(k("a"));
        tm.begin(1, 10, ws, HashSet::new(), HashMap::new()).unwrap();

        // Act
        let result = tm.try_commit(1, 20);

        // Assert
        assert!(result.is_ok());
        assert_eq!(tm.active_count(), 0);
    }

    #[test]
    fn should_allow_disjoint_commits_between_two_transactions() {
        // Arrange
        let tm = TransactionManager::new();
        let mut ws1 = HashSet::new();
        ws1.insert(k("a"));
        let mut ws2 = HashSet::new();
        ws2.insert(k("b"));
        tm.begin(1, 10, ws1, HashSet::new(), HashMap::new())
            .unwrap();
        tm.begin(2, 10, ws2, HashSet::new(), HashMap::new())
            .unwrap();

        // Act
        let r1 = tm.try_commit(1, 20);
        let r2 = tm.try_commit(2, 21);

        // Assert
        assert!(r1.is_ok());
        assert!(r2.is_ok());
    }

    // =========================================================================
    // Conflict detection
    // =========================================================================

    #[test]
    fn should_detect_write_write_conflict_between_active_transactions() {
        // Arrange
        let tm = TransactionManager::new();
        let mut ws1 = HashSet::new();
        ws1.insert(k("x"));
        let mut ws2 = HashSet::new();
        ws2.insert(k("x"));
        tm.begin(1, 1, ws1, HashSet::new(), HashMap::new()).unwrap();
        tm.begin(2, 1, ws2, HashSet::new(), HashMap::new()).unwrap();

        // Act
        let result = tm.try_commit(2, 5);

        // Assert
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Write-write conflict"));
    }

    #[test]
    fn should_detect_write_write_conflict_with_committed_transaction() {
        // Arrange
        let tm = TransactionManager::new();
        let mut ws = HashSet::new();
        ws.insert(k("shared"));
        tm.begin(1, 10, ws.clone(), HashSet::new(), HashMap::new())
            .unwrap();
        tm.try_commit(1, 20).unwrap();
        tm.begin(2, 15, ws, HashSet::new(), HashMap::new()).unwrap();

        // Act
        let result = tm.try_commit(2, 25);

        // Assert
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Write-write conflict"));
    }

    #[test]
    fn should_detect_read_write_conflict_given_key_modified_after_read() {
        // Arrange
        let tm = TransactionManager::new();
        let mut ws1 = HashSet::new();
        ws1.insert(k("data"));
        tm.begin(1, 10, ws1, HashSet::new(), HashMap::new())
            .unwrap();
        tm.try_commit(1, 20).unwrap();

        let mut reads = HashMap::new();
        reads.insert(k("data"), 15);
        tm.begin(2, 15, HashSet::new(), HashSet::new(), reads)
            .unwrap();

        // Act
        let result = tm.try_commit(2, 30);

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
        let tm = TransactionManager::new();

        // Act
        for i in 0..1100 {
            let mut ws = HashSet::new();
            ws.insert(k(&format!("k{i}")));
            tm.begin(i, i as u64, ws, HashSet::new(), HashMap::new())
                .unwrap();
            tm.try_commit(i, 1000 + i as u64).unwrap();
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
        let tm = TransactionManager::new();
        let mut ws1 = HashSet::new();
        ws1.insert(k("shared"));
        let mut ws2 = HashSet::new();
        ws2.insert(k("shared"));
        tm.begin(1, 1, ws1, HashSet::new(), HashMap::new()).unwrap();
        tm.begin(2, 1, ws2, HashSet::new(), HashMap::new()).unwrap();

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
        let tm = TransactionManager::new();
        let mut ws1 = HashSet::new();
        ws1.insert(k("data"));
        let mut rs2 = HashSet::new();
        rs2.insert(k("data"));
        tm.begin(1, 1, ws1, HashSet::new(), HashMap::new()).unwrap();
        tm.begin(2, 1, HashSet::new(), rs2, HashMap::new()).unwrap();

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
        let tm = TransactionManager::new();
        let mut ws1 = HashSet::new();
        ws1.insert(k("shared"));
        let mut ws2 = HashSet::new();
        ws2.insert(k("shared"));
        tm.begin(1, 1, ws1, HashSet::new(), HashMap::new()).unwrap();
        tm.begin(2, 1, ws2, HashSet::new(), HashMap::new()).unwrap();
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
        let tm = TransactionManager::new();

        let mut ws1 = HashSet::new();
        ws1.insert(k("A"));
        let mut rs1 = HashSet::new();
        rs1.insert(k("B"));

        let mut ws2 = HashSet::new();
        ws2.insert(k("B"));
        let mut rs2 = HashSet::new();
        rs2.insert(k("A"));

        tm.begin(1, 1, ws1, rs1, HashMap::new()).unwrap();
        tm.begin(2, 1, ws2, rs2, HashMap::new()).unwrap();

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
        let tm = TransactionManager::new();

        let (a, b, c) = (k("A"), k("B"), k("C"));

        let ws1 = HashSet::from([a.clone()]);
        let rs1 = HashSet::from([b.clone()]);
        let ws2 = HashSet::from([b.clone()]);
        let rs2 = HashSet::from([c.clone()]);
        let ws3 = HashSet::from([c.clone()]);
        let rs3 = HashSet::from([a.clone()]);

        tm.begin(1, 1, ws1, rs1, HashMap::new()).unwrap();
        tm.begin(2, 1, ws2, rs2, HashMap::new()).unwrap();
        tm.begin(3, 1, ws3, rs3, HashMap::new()).unwrap();

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
        let tm = TransactionManager::new();
        let mut ws1 = HashSet::new();
        ws1.insert(k("x"));
        let mut ws2 = HashSet::new();
        ws2.insert(k("y"));
        tm.begin(1, 1, ws1, HashSet::new(), HashMap::new()).unwrap();
        tm.begin(2, 1, ws2, HashSet::new(), HashMap::new()).unwrap();
        tm.update_wait_for_graph(1).unwrap();
        tm.update_wait_for_graph(2).unwrap();

        // Act
        let result = tm.check_for_deadlock();

        // Assert
        assert!(result.is_none());
    }
}
