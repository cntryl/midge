//! Trie accelerator for structured SST key spaces.

pub mod builder;
pub mod encoding;
pub mod node;
pub mod reader;
pub mod writer;

pub use builder::TrieBuilder;
pub use reader::TrieReader;

/// Calculate longest common prefix length
#[must_use]
pub fn lcp(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

#[cfg(test)]
mod lcp_tests {
    use super::*;

    #[test]
    fn should_compute_lcp_correctly() {
        // Arrange
        let cases = [
            (b"abc".as_slice(), b"abd".as_slice(), 2),
            (b"test".as_slice(), b"test".as_slice(), 4),
            (b"hello".as_slice(), b"world".as_slice(), 0),
            (b"prefix_a".as_slice(), b"prefix_b".as_slice(), 7),
            (b"".as_slice(), b"anything".as_slice(), 0),
        ];

        // Act
        let results: Vec<usize> = cases.iter().map(|(a, b, _)| lcp(a, b)).collect();

        // Assert
        for ((_, _, expected), actual) in cases.iter().zip(results) {
            assert_eq!(actual, *expected);
        }
    }
}
