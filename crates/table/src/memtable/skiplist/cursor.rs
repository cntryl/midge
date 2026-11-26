//! Lazy range cursor over the skiplist (#920).
//!
//! A scan used to copy every visible version in its range before yielding the
//! first row. The cursor seeks once and then yields on demand, so a scan that
//! stops after ten rows touches about ten nodes. Forward steps follow the
//! level-zero link. Reverse steps re-run `find` on the current key to reach its
//! predecessor, `O(log n)` each, because the list has no back links.
//!
//! Concurrent inserts publish nodes whose versions are newer than any snapshot
//! a reader holds, so they are filtered like every other invisible version;
//! the snapshot semantics are those of the eager range functions this replaces.

use super::{Node, SkipList, SkipListEntryWithExp, AO, MAX_LEVEL};
use std::ptr;
use std::sync::Arc;

/// Position of the cursor between steps.
enum Position {
    /// Not yet seeked.
    Start,
    /// At a node still to be checked against the bounds and yielded.
    At(*mut Node),
    Done,
}

pub struct VisibleCursor {
    list: Arc<SkipList>,
    start: Option<Vec<u8>>,
    end: Option<Vec<u8>>,
    snapshot_seq: u64,
    reverse: bool,
    position: Position,
}

// SAFETY: the cursor holds the list alive through its `Arc`; published nodes
// are never freed while the list is shared, and every node pointer it stores
// was loaded with `Acquire` from a published link, exactly as the list's own
// `Send`/`Sync` impls rely on. Only `Send` is needed: a cursor is consumed by
// one thread.
unsafe impl Send for VisibleCursor {}

impl VisibleCursor {
    pub(super) fn new(
        list: Arc<SkipList>,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        snapshot_seq: u64,
        reverse: bool,
    ) -> Self {
        Self {
            list,
            start: start.map(<[u8]>::to_vec),
            end: end.map(<[u8]>::to_vec),
            snapshot_seq,
            reverse,
            position: Position::Start,
        }
    }

    /// First node of the walk: the first key at or after `start` going
    /// forward, the last key before `end` going backward.
    fn seek(&self) -> *mut Node {
        if self.reverse {
            match &self.end {
                Some(end) => self.list.predecessor(end),
                None => self.list.last_node(),
            }
        } else {
            match &self.start {
                Some(start) => self.list.successor(start),
                None => self.list.head.next.load(AO::Acquire),
            }
        }
    }

    fn within_bounds(&self, key: &[u8]) -> bool {
        if self.reverse {
            self.start.as_deref().is_none_or(|start| key >= start)
        } else {
            self.end.as_deref().is_none_or(|end| key < end)
        }
    }

    fn step(&self, node: &Node) -> *mut Node {
        if self.reverse {
            self.list.predecessor(&node.key)
        } else {
            node.next.load(AO::Acquire)
        }
    }
}

impl Iterator for VisibleCursor {
    type Item = SkipListEntryWithExp;

    fn next(&mut self) -> Option<Self::Item> {
        let mut curr = match self.position {
            Position::Start => self.seek(),
            Position::At(node) => node,
            Position::Done => return None,
        };
        // SAFETY: `curr` came from `seek`/`step`, which load published links
        // with Acquire; nodes are never freed while `self.list` is shared.
        while let Some(node) = unsafe { curr.as_ref() } {
            #[cfg(test)]
            self.list.note_node_visited();
            if !self.within_bounds(&node.key) {
                break;
            }
            let following = self.step(node);
            if let Some(vn) = SkipList::visible_version(&node.versions_head, self.snapshot_seq) {
                self.position = Position::At(following);
                return Some((
                    node.key.clone(),
                    vn.val.clone(),
                    vn.seq,
                    vn.val.is_none(),
                    vn.exp,
                    vn.op,
                ));
            }
            curr = following;
        }
        self.position = Position::Done;
        None
    }
}

impl SkipList {
    /// Lazily yield the versions visible at `snapshot_seq` in `[start, end)`,
    /// in key order or reverse key order.
    #[must_use]
    pub(crate) fn visible_cursor(
        self: &Arc<Self>,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        snapshot_seq: u64,
        reverse: bool,
    ) -> VisibleCursor {
        VisibleCursor::new(Arc::clone(self), start, end, snapshot_seq, reverse)
    }

    /// First node whose key is at or after `key`, or null.
    fn successor(&self, key: &[u8]) -> *mut Node {
        let mut preds: [*mut Node; MAX_LEVEL] = [ptr::null_mut(); MAX_LEVEL];
        let mut succs: [*mut Node; MAX_LEVEL] = [ptr::null_mut(); MAX_LEVEL];
        self.find(key, &mut preds, &mut succs);
        succs[0]
    }

    /// Last node whose key is before `key`, or null when none precedes it.
    fn predecessor(&self, key: &[u8]) -> *mut Node {
        let mut preds: [*mut Node; MAX_LEVEL] = [ptr::null_mut(); MAX_LEVEL];
        let mut succs: [*mut Node; MAX_LEVEL] = [ptr::null_mut(); MAX_LEVEL];
        self.find(key, &mut preds, &mut succs);
        let pred = preds[0];
        if ptr::eq(pred, ptr::from_ref(self.head.as_ref())) {
            ptr::null_mut()
        } else {
            pred
        }
    }

    /// The node with the greatest key, or null when the list is empty:
    /// descend from the top level, running to the end of each level.
    fn last_node(&self) -> *mut Node {
        let mut pred = ptr::from_ref(self.head.as_ref()).cast_mut();
        let mut level = self.top_level.load(AO::Relaxed);
        while level > 0 {
            let l = level - 1;
            // SAFETY: published nodes live until exclusive list drop; `pred`
            // is the head or a node reached through Acquire loads.
            let mut curr = unsafe { &*pred }.link(l).load(AO::Acquire);
            while let Some(curr_ref) = unsafe { curr.as_ref() } {
                pred = curr;
                curr = curr_ref.link(l).load(AO::Acquire);
            }
            level -= 1;
        }
        if ptr::eq(pred, ptr::from_ref(self.head.as_ref())) {
            ptr::null_mut()
        } else {
            pred
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::SkipList;
    use std::sync::Arc;

    fn list_with_keys(keys: &[&str]) -> Arc<SkipList> {
        let list = Arc::new(SkipList::new());
        for (index, key) in keys.iter().enumerate() {
            let seq = u64::try_from(index).expect("fixture index") + 1;
            list.upsert(
                bytes::Bytes::copy_from_slice(key.as_bytes()),
                Some(bytes::Bytes::from_static(b"v")),
                seq,
            );
        }
        list
    }

    fn keys_of(cursor: super::VisibleCursor) -> Vec<String> {
        cursor
            .map(|(key, ..)| String::from_utf8(key.to_vec()).expect("utf8 key"))
            .collect()
    }

    #[test]
    fn should_yield_range_in_both_directions_given_bounds_when_iterated() {
        // Arrange
        let list = list_with_keys(&["a", "c", "e", "g", "i"]);

        // Act
        let forward = keys_of(list.visible_cursor(Some(b"b"), Some(b"h"), u64::MAX, false));
        let reverse = keys_of(list.visible_cursor(Some(b"b"), Some(b"h"), u64::MAX, true));
        let open = keys_of(list.visible_cursor(None, None, u64::MAX, true));

        // Assert: `[start, end)` in both directions; an open reverse scan
        // starts at the last key.
        assert_eq!(forward, ["c", "e", "g"]);
        assert_eq!(reverse, ["g", "e", "c"]);
        assert_eq!(open, ["i", "g", "e", "c", "a"]);
    }

    #[test]
    fn should_skip_invisible_versions_given_snapshot_when_iterated() {
        // Arrange: "c" and "e" were written after the snapshot.
        let list = list_with_keys(&["a", "c", "e", "g"]);

        // Act
        let visible = keys_of(list.visible_cursor(None, None, 1, false));
        let reverse = keys_of(list.visible_cursor(None, None, 4, true));

        // Assert
        assert_eq!(visible, ["a"]);
        assert_eq!(reverse, ["g", "e", "c", "a"]);
    }

    #[test]
    fn should_yield_nothing_given_empty_list_when_iterated() {
        // Arrange
        let list = Arc::new(SkipList::new());

        // Act
        let forward = keys_of(list.visible_cursor(None, None, u64::MAX, false));
        let reverse = keys_of(list.visible_cursor(None, None, u64::MAX, true));

        // Assert
        assert_eq!(forward, Vec::<String>::new());
        assert_eq!(reverse, Vec::<String>::new());
    }
}
