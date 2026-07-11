//! Iterator API - lazy, fallible range scans.

use crate::common::MidgeResult;
use bytes::Bytes;

/// Iteration direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Forward iteration (ascending keys).
    Forward,
    /// Reverse iteration (descending keys).
    Reverse,
}

/// A lazy range iterator over key-value pairs.
///
/// Storage is consulted as the iterator advances. Consequently, an error in a
/// later SST block is returned from the item that reaches that block instead
/// of being hidden during iterator construction.
pub struct Iterator<'a> {
    inner: Box<dyn std::iter::Iterator<Item = MidgeResult<(Bytes, Bytes)>> + 'a>,
    direction: Direction,
    exhausted: bool,
}

impl<'a> Iterator<'a> {
    pub(crate) fn from_iter<I>(iterator: I, direction: Direction) -> Self
    where
        I: std::iter::Iterator<Item = MidgeResult<(Bytes, Bytes)>> + 'a,
    {
        Self {
            inner: Box::new(iterator),
            direction,
            exhausted: false,
        }
    }

    /// Return whether iteration is complete.
    #[must_use]
    pub fn exhausted(&self) -> bool {
        self.exhausted
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
        if self.exhausted {
            return None;
        }
        let next = self.inner.next();
        if next.is_none() {
            self.exhausted = true;
        }
        next
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
        assert_eq!(iterator.direction(), Direction::Reverse);
    }
}
