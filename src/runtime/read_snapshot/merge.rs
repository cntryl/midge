impl ReadSnapshot {
    fn state_sequence(state: &KeyState) -> Option<u64> {
        match state {
            KeyState::Absent => None,
            KeyState::Tombstone(seq) | KeyState::Value(_, seq, _, _) => Some(*seq),
        }
    }

    fn candidate_wins(existing: &KeyState, candidate: &KeyState) -> MidgeResult<bool> {
        let existing_seq = Self::state_sequence(existing).unwrap_or(0);
        let candidate_seq = Self::state_sequence(candidate).unwrap_or(0);
        if !matches!(existing, KeyState::Absent)
            && !matches!(candidate, KeyState::Absent)
            && candidate_seq == existing_seq
        {
            crate::types::resolve_same_sequence(
                crate::types::VersionContent::from_state(existing).expect("present state"),
                crate::types::VersionContent::from_state(candidate).expect("present state"),
            )
            .map_err(|()| {
                MidgeError::Corruption(format!(
                    "conflicting read versions at sequence {candidate_seq}"
                ))
            })?;
        }
        Ok(candidate_seq > existing_seq || matches!(existing, KeyState::Absent))
    }

    #[cfg(test)]
    fn merge_state(
        states: &mut BTreeMap<Vec<u8>, KeyState>,
        key: Vec<u8>,
        state: KeyState,
    ) -> MidgeResult<()> {
        if matches!(state, KeyState::Absent) {
            return Ok(());
        }

        match states.get(&key) {
            Some(existing) if !Self::candidate_wins(existing, &state)? => {}
            _ => {
                states.insert(key, state);
            }
        }
        Ok(())
    }

    fn merge_best_state(best_state: &mut Option<KeyState>, state: KeyState) -> MidgeResult<()> {
        if matches!(state, KeyState::Absent) {
            return Ok(());
        }

        if best_state
            .as_ref()
            .map_or(Ok(true), |existing| Self::candidate_wins(existing, &state))?
        {
            *best_state = Some(state);
        }
        Ok(())
    }

    #[cfg(test)]
    fn is_visible_state(state: &KeyState, snapshot_seq: u64) -> bool {
        snapshot_seq == u64::MAX
            || Self::state_sequence(state).is_some_and(|state_seq| state_seq <= snapshot_seq)
    }

    fn range_tombstone_overlaps_query(
        tombstone: &RangeTombstone,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> bool {
        if let Some(scan_end) = end {
            if tombstone.start.as_slice() >= scan_end {
                return false;
            }
        }
        if let Some(scan_start) = start {
            if tombstone.end.as_slice() <= scan_start {
                return false;
            }
        }
        true
    }

    #[cfg(test)]
    fn range_tombstone_covers_state(
        tombstones: &[RangeTombstone],
        key: &[u8],
        state: &KeyState,
    ) -> bool {
        let state_seq = Self::state_sequence(state).unwrap_or(0);
        tombstones
            .iter()
            .any(|tombstone| tombstone.covers(key) && tombstone.seq >= state_seq)
    }


}
