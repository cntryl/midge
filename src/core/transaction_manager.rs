use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Tracks active transactions and detects conflicts
pub struct TransactionManager {
    inner: Arc<Mutex<TransactionManagerInner>>,
}

struct TransactionManagerInner {
    active_txns: HashMap<u64, TransactionInfo>,
    committed_writes: HashMap<u64, (u64, HashSet<(u32, Bytes)>)>, // seq -> (txn_id, write_set)
    max_retained_commits: usize,
    wait_for_graph: HashMap<u64, HashSet<u64>>, // txn_id -> set of txns it waits for
}

#[derive(Clone)]
struct TransactionInfo {
    #[allow(dead_code)]
    txn_id: u64,
    begin_seq: u64,
    write_set: HashSet<(u32, Bytes)>,
    read_set: HashSet<(u32, Bytes)>,
    read_versions: HashMap<(u32, Bytes), u64>,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(TransactionManagerInner {
                active_txns: HashMap::new(),
                committed_writes: HashMap::new(),
                max_retained_commits: 1000,
                wait_for_graph: HashMap::new(),
            })),
        }
    }

    /// Register a new transaction
    pub fn begin(
        &self,
        txn_id: u64,
        begin_seq: u64,
        write_set: HashSet<(u32, Bytes)>,
        read_set: HashSet<(u32, Bytes)>,
        read_versions: HashMap<(u32, Bytes), u64>,
    ) -> Result<(), String> {
        let mut inner = self.inner.lock();

        let info = TransactionInfo {
            txn_id,
            begin_seq,
            write_set,
            read_set,
            read_versions,
        };

        inner.active_txns.insert(txn_id, info);
        Ok(())
    }

    /// Check for conflicts and commit if no conflicts found
    pub fn try_commit(&self, txn_id: u64, commit_seq: u64) -> Result<(), String> {
        let mut inner = self.inner.lock();

        let txn_info = inner
            .active_txns
            .get(&txn_id)
            .ok_or_else(|| "Transaction not found".to_string())?
            .clone();

        // Check for write-write conflicts with other active transactions
        for (other_id, other_info) in &inner.active_txns {
            if *other_id != txn_id && !txn_info.write_set.is_disjoint(&other_info.write_set) {
                return Err(format!(
                    "Write-write conflict detected with transaction {}",
                    other_id
                ));
            }
        }

        // Check for write-write conflicts with committed transactions after our begin
        for (seq, (committed_txn_id, writes)) in &inner.committed_writes {
            // Conflict if: committed after our begin AND it's not our own transaction AND writes overlap
            if *seq >= txn_info.begin_seq
                && *committed_txn_id != txn_id
                && !txn_info.write_set.is_disjoint(writes)
            {
                return Err(format!(
                    "Write-write conflict: key was modified by transaction {} at sequence {}",
                    committed_txn_id, seq
                ));
            }
        }

        // Check for read-write conflicts (optimistic locking)
        // If a key was read by this txn, verify no writes occurred after read version
        for (key, read_ver) in &txn_info.read_versions {
            // Check if any committed transaction wrote to this key after our read
            for (seq, (_committed_txn_id, writes)) in &inner.committed_writes {
                if *seq > *read_ver && writes.contains(key) {
                    return Err(format!(
                        "Read-write conflict: key modified after read at version {}",
                        read_ver
                    ));
                }
            }
        }

        // Commit: record write set for future conflict detection
        inner
            .committed_writes
            .insert(commit_seq, (txn_id, txn_info.write_set.clone()));
        inner.active_txns.remove(&txn_id);

        // Cleanup old commits to prevent unbounded growth
        if inner.committed_writes.len() > inner.max_retained_commits {
            let oldest_keys: Vec<u64> = inner
                .committed_writes
                .keys()
                .copied()
                .take(inner.committed_writes.len() - inner.max_retained_commits)
                .collect();
            for key in oldest_keys {
                inner.committed_writes.remove(&key);
            }
        }

        Ok(())
    }

    /// Abort a transaction
    pub fn abort(&self, txn_id: u64) {
        let mut inner = self.inner.lock();
        inner.active_txns.remove(&txn_id);
        inner.wait_for_graph.remove(&txn_id);
        // Remove all edges pointing to this transaction
        for edges in inner.wait_for_graph.values_mut() {
            edges.remove(&txn_id);
        }
    }

    /// Get number of active transactions
    pub fn active_count(&self) -> usize {
        let inner = self.inner.lock();
        inner.active_txns.len()
    }

    /// Detect cycles in wait-for graph using DFS
    /// Returns Some(cycle_path) if deadlock detected, None otherwise
    fn detect_cycle(
        graph: &HashMap<u64, HashSet<u64>>,
        start: u64,
        visited: &mut HashSet<u64>,
        rec_stack: &mut HashSet<u64>,
        path: &mut Vec<u64>,
    ) -> Option<Vec<u64>> {
        visited.insert(start);
        rec_stack.insert(start);
        path.push(start);

        if let Some(neighbors) = graph.get(&start) {
            for &neighbor in neighbors {
                if !visited.contains(&neighbor) {
                    if let Some(cycle) =
                        Self::detect_cycle(graph, neighbor, visited, rec_stack, path)
                    {
                        return Some(cycle);
                    }
                } else if rec_stack.contains(&neighbor) {
                    // Found a cycle - extract the cycle from path
                    let cycle_start_idx = path
                        .iter()
                        .position(|&x| x == neighbor)
                        .expect("Neighbor must exist in path when cycle detected");
                    return Some(path[cycle_start_idx..].to_vec());
                }
            }
        }

        path.pop();
        rec_stack.remove(&start);
        None
    }

    /// Check for deadlocks in the wait-for graph
    /// Returns Some((victim_id, cycle_path)) if deadlock detected
    pub fn check_for_deadlock(&self) -> Option<(u64, Vec<u64>)> {
        let inner = self.inner.lock();

        if inner.wait_for_graph.is_empty() {
            return None;
        }

        let mut visited = HashSet::new();

        for &txn_id in inner.wait_for_graph.keys() {
            if !visited.contains(&txn_id) {
                let mut rec_stack = HashSet::new();
                let mut path = Vec::new();

                if let Some(cycle) = Self::detect_cycle(
                    &inner.wait_for_graph,
                    txn_id,
                    &mut visited,
                    &mut rec_stack,
                    &mut path,
                ) {
                    // Choose victim: youngest transaction (highest txn_id) in the cycle
                    let victim = *cycle
                        .iter()
                        .max()
                        .expect("Cycle must contain at least one transaction");
                    return Some((victim, cycle));
                }
            }
        }

        None
    }

    /// Build wait-for edges based on potential conflicts
    /// Called during try_commit to detect circular dependencies
    pub fn update_wait_for_graph(&self, txn_id: u64) -> Result<(), String> {
        let mut inner = self.inner.lock();

        let txn_info = inner
            .active_txns
            .get(&txn_id)
            .ok_or_else(|| "Transaction not found".to_string())?
            .clone();

        // Clear previous wait-for edges for this transaction
        let mut wait_for_set = HashSet::new();

        // Build wait-for edges: this txn waits for any active txn with conflicting writes
        for (other_id, other_info) in &inner.active_txns {
            if *other_id != txn_id {
                // If write sets overlap, this txn "waits for" the other
                if !txn_info.write_set.is_disjoint(&other_info.write_set) {
                    wait_for_set.insert(*other_id);
                }

                // If this txn reads what the other writes, it waits for the other
                if txn_info
                    .read_set
                    .iter()
                    .any(|k| other_info.write_set.contains(k))
                {
                    wait_for_set.insert(*other_id);
                }
            }
        }

        inner.wait_for_graph.insert(txn_id, wait_for_set);
        Ok(())
    }
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_register_transaction_given_begin() {
        // Arrange
        let tm = TransactionManager::new();

        // Act
        let result = tm.begin(1, 100, HashSet::new(), HashSet::new(), HashMap::new());

        // Assert
        assert!(result.is_ok());
        assert_eq!(tm.active_count(), 1);
    }

    #[test]
    fn should_commit_transaction_given_no_conflicts() {
        // Arrange
        let tm = TransactionManager::new();
        tm.begin(1, 100, HashSet::new(), HashSet::new(), HashMap::new())
            .unwrap();

        // Act
        let result = tm.try_commit(1, 101);

        // Assert
        assert!(result.is_ok());
        assert_eq!(tm.active_count(), 0);
    }

    #[test]
    fn should_detect_write_write_conflict_given_overlapping_write_sets() {
        // Arrange
        let tm = TransactionManager::new();
        let mut ws1 = HashSet::new();
        ws1.insert((crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("key1")));
        ws1.insert((crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("key2")));

        let mut ws2 = HashSet::new();
        ws2.insert((crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("key2")));
        ws2.insert((crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("key3")));

        tm.begin(1, 100, ws1, HashSet::new(), HashMap::new())
            .unwrap();
        tm.begin(2, 100, ws2, HashSet::new(), HashMap::new())
            .unwrap();

        // Act
        let result = tm.try_commit(2, 101);

        // Assert
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Write-write conflict"));
    }

    #[test]
    fn should_allow_commit_given_disjoint_write_sets() {
        // Arrange
        let tm = TransactionManager::new();
        let mut ws1 = HashSet::new();
        ws1.insert((crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("key1")));

        let mut ws2 = HashSet::new();
        ws2.insert((crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("key2")));

        tm.begin(1, 100, ws1, HashSet::new(), HashMap::new())
            .unwrap();
        tm.begin(2, 100, ws2, HashSet::new(), HashMap::new())
            .unwrap();

        // Act
        let r1 = tm.try_commit(1, 101);
        let r2 = tm.try_commit(2, 102);

        // Assert
        assert!(r1.is_ok());
        assert!(r2.is_ok());
    }

    #[test]
    fn should_detect_read_write_conflict_given_key_modified_after_read() {
        // Arrange
        let tm = TransactionManager::new();

        let mut ws1 = HashSet::new();
        ws1.insert(Bytes::from("key"));
        tm.begin(1, 100, ws1, HashSet::new(), HashMap::new())
            .unwrap();
        tm.try_commit(1, 110).unwrap();

        let mut read_versions = HashMap::new();
        read_versions.insert(
            (crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("key")),
            105,
        );
        tm.begin(2, 105, HashSet::new(), HashSet::new(), read_versions)
            .unwrap();

        // Act
        let result = tm.try_commit(2, 115);

        // Assert
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Read-write conflict"));
    }

    #[test]
    fn should_abort_transaction_given_abort_called() {
        // Arrange
        let tm = TransactionManager::new();
        tm.begin(1, 100, HashSet::new(), HashSet::new(), HashMap::new())
            .unwrap();

        // Act
        tm.abort(1);

        // Assert
        assert_eq!(tm.active_count(), 0);
    }

    #[test]
    fn should_track_multiple_active_transactions() {
        // Arrange
        let tm = TransactionManager::new();

        // Act
        tm.begin(1, 100, HashSet::new(), HashSet::new(), HashMap::new())
            .unwrap();
        tm.begin(2, 101, HashSet::new(), HashSet::new(), HashMap::new())
            .unwrap();
        tm.begin(3, 102, HashSet::new(), HashSet::new(), HashMap::new())
            .unwrap();

        // Assert
        assert_eq!(tm.active_count(), 3);
    }

    #[test]
    fn should_cleanup_old_commits_given_exceeds_retention_limit() {
        // Arrange
        let tm = TransactionManager::new();

        // Act
        for i in 0..1100 {
            let mut ws = HashSet::new();
            ws.insert((
                crate::api::DEFAULT_CF_ID.as_u32(),
                Bytes::from(format!("key{}", i)),
            ));
            tm.begin(i, 100 + i, ws, HashSet::new(), HashMap::new())
                .unwrap();
            tm.try_commit(i, 100 + i).unwrap();
        }

        // Assert
        let inner = tm.inner.lock();
        assert!(inner.committed_writes.len() <= 1000);
    }

    #[test]
    fn should_update_wait_for_graph_given_conflicting_write_sets() {
        // Arrange
        let tm = TransactionManager::new();

        let mut ws1 = HashSet::new();
        ws1.insert((crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("key1")));

        let mut ws2 = HashSet::new();
        ws2.insert((crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("key1"))); // Conflicts with txn1
        ws2.insert((crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("key2")));

        tm.begin(1, 100, ws1, HashSet::new(), HashMap::new())
            .unwrap();
        tm.begin(2, 100, ws2, HashSet::new(), HashMap::new())
            .unwrap();

        // Act
        tm.update_wait_for_graph(2).unwrap();

        // Assert
        let inner = tm.inner.lock();
        let wait_for = inner.wait_for_graph.get(&2).unwrap();
        assert!(wait_for.contains(&1), "Txn 2 should wait for txn 1");
    }

    #[test]
    fn should_update_wait_for_graph_given_read_write_conflict() {
        // Arrange
        let tm = TransactionManager::new();

        let mut ws1 = HashSet::new();
        ws1.insert((crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("data")));

        let mut rs2 = HashSet::new();
        rs2.insert((crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("data"))); // Reads what txn1 writes

        tm.begin(1, 100, ws1, HashSet::new(), HashMap::new())
            .unwrap();
        tm.begin(2, 100, HashSet::new(), rs2, HashMap::new())
            .unwrap();

        // Act
        tm.update_wait_for_graph(2).unwrap();

        // Assert
        let inner = tm.inner.lock();
        let wait_for = inner.wait_for_graph.get(&2).unwrap();
        assert!(
            wait_for.contains(&1),
            "Txn 2 should wait for txn 1 (read-write)"
        );
    }

    #[test]
    fn should_detect_two_transaction_cycle() {
        // Arrange
        let tm = TransactionManager::new();

        // Txn 1 writes A, wants B
        let mut ws1 = HashSet::new();
        ws1.insert((crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("A")));
        let mut rs1 = HashSet::new();
        rs1.insert((crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("B")));

        // Txn 2 writes B, wants A
        let mut ws2 = HashSet::new();
        ws2.insert((crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("B")));
        let mut rs2 = HashSet::new();
        rs2.insert((crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("A")));

        tm.begin(1, 100, ws1.clone(), rs1.clone(), HashMap::new())
            .unwrap();
        tm.begin(2, 100, ws2.clone(), rs2.clone(), HashMap::new())
            .unwrap();

        tm.update_wait_for_graph(1).unwrap();
        tm.update_wait_for_graph(2).unwrap();

        // Act
        let result = tm.check_for_deadlock();

        // Assert
        assert!(result.is_some(), "Should detect deadlock");
        let (victim, cycle) = result.unwrap();
        assert!(
            victim == 1 || victim == 2,
            "Victim should be one of the transactions"
        );
        assert!(
            cycle.len() >= 2,
            "Cycle should contain at least 2 transactions"
        );
    }

    #[test]
    fn should_detect_three_transaction_cycle() {
        // Arrange
        let tm = TransactionManager::new();

        // Txn 1 writes A, reads B
        let mut ws1 = HashSet::new();
        ws1.insert((crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("A")));
        let mut rs1 = HashSet::new();
        rs1.insert((crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("B")));

        // Txn 2 writes B, reads C
        let mut ws2 = HashSet::new();
        ws2.insert((crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("B")));
        let mut rs2 = HashSet::new();
        rs2.insert((crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("C")));

        // Txn 3 writes C, reads A
        let mut ws3 = HashSet::new();
        ws3.insert((crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("C")));
        let mut rs3 = HashSet::new();
        rs3.insert((crate::api::DEFAULT_CF_ID.as_u32(), Bytes::from("A")));

        tm.begin(1, 100, ws1, rs1, HashMap::new()).unwrap();
        tm.begin(2, 100, ws2, rs2, HashMap::new()).unwrap();
        tm.begin(3, 100, ws3, rs3, HashMap::new()).unwrap();

        tm.update_wait_for_graph(1).unwrap();
        tm.update_wait_for_graph(2).unwrap();
        tm.update_wait_for_graph(3).unwrap();

        // Act
        let result = tm.check_for_deadlock();

        // Assert
        assert!(result.is_some(), "Should detect 3-way deadlock");
        let (victim, cycle) = result.unwrap();
        assert!(
            victim == 1 || victim == 2 || victim == 3,
            "Victim should be highest txn_id in cycle"
        );
        assert!(cycle.len() >= 2, "Cycle should contain transactions");
    }

    #[test]
    fn should_not_detect_cycle_given_no_circular_dependency() {
        // Arrange
        let tm = TransactionManager::new();

        let mut ws1 = HashSet::new();
        ws1.insert(Bytes::from("key1"));

        let mut ws2 = HashSet::new();
        ws2.insert(Bytes::from("key2"));

        tm.begin(1, 100, ws1, HashSet::new(), HashMap::new())
            .unwrap();
        tm.begin(2, 100, ws2, HashSet::new(), HashMap::new())
            .unwrap();

        tm.update_wait_for_graph(1).unwrap();
        tm.update_wait_for_graph(2).unwrap();

        // Act
        let result = tm.check_for_deadlock();

        // Assert
        assert!(
            result.is_none(),
            "Should not detect deadlock with disjoint keys"
        );
    }

    #[test]
    fn should_clear_wait_for_edges_given_transaction_aborted() {
        // Arrange
        let tm = TransactionManager::new();

        let mut ws1 = HashSet::new();
        ws1.insert(Bytes::from("key"));
        let mut ws2 = HashSet::new();
        ws2.insert(Bytes::from("key"));

        tm.begin(1, 100, ws1, HashSet::new(), HashMap::new())
            .unwrap();
        tm.begin(2, 100, ws2, HashSet::new(), HashMap::new())
            .unwrap();
        tm.update_wait_for_graph(2).unwrap();

        // Act
        tm.abort(2);

        // Assert
        let inner = tm.inner.lock();
        assert!(
            !inner.wait_for_graph.contains_key(&2),
            "Wait-for edges should be cleared"
        );
    }
}
