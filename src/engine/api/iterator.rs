//! Iterator API - lazy, fallible range scans.

use crate::common::{MidgeError, MidgeResult};
use bytes::Bytes;

/// Iteration direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Forward iteration (ascending keys).
    Forward,
    /// Reverse iteration (descending keys).
    Reverse,
}

/// Observable terminal state of a lazy range iterator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IteratorState {
    /// The iterator may still yield rows.
    Active,
    /// The iterator reached the end of its range normally.
    Exhausted,
    /// A storage or decoding error terminated the scan.
    Failed,
}

/// A lazy range iterator over key-value pairs.
///
/// Storage is consulted as the iterator advances. Consequently, an error in a
/// later SST block is returned from the item that reaches that block instead
/// of being hidden during iterator construction. A terminal error is sticky:
/// callers that advance again receive the same error variant and message, and
/// must propagate the error or stop iterating.
pub struct Iterator<'a> {
    inner: Box<dyn std::iter::Iterator<Item = MidgeResult<(Bytes, Bytes)>> + 'a>,
    direction: Direction,
    state: IteratorState,
    failure: Option<MidgeError>,
}

impl<'a> Iterator<'a> {
    pub(crate) fn from_iter<I>(iterator: I, direction: Direction) -> Self
    where
        I: std::iter::Iterator<Item = MidgeResult<(Bytes, Bytes)>> + 'a,
    {
        Self {
            inner: Box::new(iterator),
            direction,
            state: IteratorState::Active,
            failure: None,
        }
    }

    /// Return whether iteration is complete.
    #[must_use]
    pub fn exhausted(&self) -> bool {
        self.state == IteratorState::Exhausted
    }

    /// Return whether iteration terminated because of a read error.
    #[must_use]
    pub fn failed(&self) -> bool {
        self.state == IteratorState::Failed
    }

    /// Return the explicit scan state.
    #[must_use]
    pub fn state(&self) -> IteratorState {
        self.state
    }

    /// Return the iteration direction.
    #[must_use]
    pub fn direction(&self) -> Direction {
        self.direction
    }

    /// Collect every remaining item, stopping at the first read error.
    ///
    /// # Errors
    ///
    /// Returns a storage or corruption error encountered while advancing the
    /// lazy scan.
    pub fn try_collect(mut self) -> MidgeResult<Vec<(Bytes, Bytes)>> {
        let mut rows = Vec::new();
        for row in self.by_ref() {
            rows.push(row?);
        }
        Ok(rows)
    }
}

impl std::iter::Iterator for Iterator<'_> {
    type Item = MidgeResult<(Bytes, Bytes)>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(error) = &self.failure {
            return Some(Err(error.replay()));
        }
        if self.state == IteratorState::Exhausted {
            return None;
        }
        match self.inner.next() {
            Some(Ok(row)) => Some(Ok(row)),
            Some(Err(error)) => {
                self.state = IteratorState::Failed;
                self.failure = Some(error);
                self.failure.as_ref().map(|error| Err(error.replay()))
            }
            None => {
                self.state = IteratorState::Exhausted;
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::MidgeError;

    #[test]
    fn should_surface_late_error_when_collecting_lazy_scan() {
        // Arrange
        let rows = vec![
            Ok((Bytes::from_static(b"a"), Bytes::from_static(b"1"))),
            Err(MidgeError::Corruption("late block".to_string())),
        ];
        let iterator = Iterator::from_iter(rows.into_iter(), Direction::Forward);

        // Act
        let error = iterator.try_collect().expect_err("late error must surface");

        // Assert
        assert!(matches!(error, MidgeError::Corruption(_)));
    }

    #[test]
    fn should_mark_scan_exhausted_after_final_item() {
        // Arrange
        let rows = vec![Ok((Bytes::from_static(b"a"), Bytes::from_static(b"1")))];
        let mut iterator = Iterator::from_iter(rows.into_iter(), Direction::Reverse);

        // Act
        let first = iterator.next();
        let end = iterator.next();

        // Assert
        assert!(first.is_some());
        assert!(end.is_none());
        assert!(iterator.exhausted());
        assert!(!iterator.failed());
        assert_eq!(iterator.state(), IteratorState::Exhausted);
        assert_eq!(iterator.direction(), Direction::Reverse);
    }

    #[test]
    fn should_replay_terminal_error_without_marking_iterator_exhausted() {
        // Arrange
        let rows = vec![Err(MidgeError::Corruption("late block".to_string()))];
        let mut iterator = Iterator::from_iter(rows.into_iter(), Direction::Forward);

        // Act
        let first = iterator.next().expect("first error item");
        let replayed = iterator.next().expect("sticky replayed error item");

        // Assert
        assert!(
            matches!(first, Err(MidgeError::Corruption(ref message)) if message == "late block")
        );
        assert!(
            matches!(replayed, Err(MidgeError::Corruption(ref message)) if message == "late block")
        );
        assert!(iterator.failed());
        assert!(!iterator.exhausted());
        assert_eq!(iterator.state(), IteratorState::Failed);
    }
}
