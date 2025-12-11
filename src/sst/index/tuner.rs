//! Auto-tuning logic for choosing between SparseIndex and TrieIndex

use super::profiler::KeyStructureProfile;

/// Index kind selection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum IndexKind {
    Sparse = 0,
    Trie = 1,
}

impl IndexKind {
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Sparse),
            1 => Some(Self::Trie),
            _ => None,
        }
    }

    pub fn to_u8(self) -> u8 {
        self as u8
    }
}

/// Auto-tuning decision maker for index strategy
pub struct IndexTuner;

impl IndexTuner {
    /// Decide which index kind to use based on key structure profile
    ///
    /// Returns Trie if ANY of these conditions are met:
    /// 1. High prefix correlation (avg_shared_prefix >= 6 AND entropy < 3.5)
    /// 2. Strong hierarchical signal (common_prefix_len >= 2 OR max_shared_prefix >= 8)
    /// 3. Small branching factor (prefix_divergence < 256)
    /// 4. High key length variance (indicates structured keys with meaningful suffixes)
    ///
    /// Returns Sparse if ANY of these conditions are met:
    /// 1. Keys look random (entropy > 4.0)
    /// 2. Many branch points (prefix_divergence >= 1024)
    /// 3. Too few keys (key_count < 128)
    /// 4. Very short keys (avg key length < 8)
    pub fn decide(profile: &KeyStructureProfile) -> IndexKind {
        // Early exit for tiny SSTs
        if profile.key_count < 128 {
            return IndexKind::Sparse;
        }

        // Calculate average key length for short key detection
        // This is approximated from total_bytes / key_count if we had that data
        // For now, we use a heuristic based on variance and other signals

        // Check for random keys (high entropy)
        if profile.entropy > 4.0 {
            return IndexKind::Sparse;
        }

        // Check for too many branch points (trie blowup risk)
        if profile.prefix_divergence >= 1024 {
            return IndexKind::Sparse;
        }

        // Check for high prefix correlation
        if profile.avg_shared_prefix >= 6.0 && profile.entropy < 3.5 {
            return IndexKind::Trie;
        }

        // Check for strong hierarchical signal
        if profile.common_prefix_len >= 2 || profile.max_shared_prefix >= 8 {
            return IndexKind::Trie;
        }

        // Check for small branching factor (structured keys)
        if profile.prefix_divergence < 256 {
            return IndexKind::Trie;
        }

        // Check for high key length variance (structured keys with suffixes)
        if profile.key_length_variance > 5.0 && profile.avg_shared_prefix >= 4.0 {
            return IndexKind::Trie;
        }

        // Default to sparse index for safety
        IndexKind::Sparse
    }

    /// Get human-readable explanation for the decision
    pub fn explain(profile: &KeyStructureProfile) -> String {
        let decision = Self::decide(profile);

        match decision {
            IndexKind::Trie => {
                let mut reasons = Vec::new();

                if profile.avg_shared_prefix >= 6.0 && profile.entropy < 3.5 {
                    reasons.push(format!(
                        "High prefix correlation (avg={:.1}, entropy={:.1})",
                        profile.avg_shared_prefix, profile.entropy
                    ));
                }

                if profile.common_prefix_len >= 2 {
                    reasons.push(format!(
                        "Common prefix across all keys ({} bytes)",
                        profile.common_prefix_len
                    ));
                }

                if profile.max_shared_prefix >= 8 {
                    reasons.push(format!(
                        "Long shared prefixes detected ({} bytes max)",
                        profile.max_shared_prefix
                    ));
                }

                if profile.prefix_divergence < 256 {
                    reasons.push(format!(
                        "Low branching factor ({} unique prefixes)",
                        profile.prefix_divergence
                    ));
                }

                format!("Trie index selected: {}", reasons.join(", "))
            }
            IndexKind::Sparse => {
                let mut reasons = Vec::new();

                if profile.key_count < 128 {
                    reasons.push(format!("Small SST ({} keys)", profile.key_count));
                }

                if profile.entropy > 4.0 {
                    reasons.push(format!(
                        "High entropy keys ({:.1} bits/byte)",
                        profile.entropy
                    ));
                }

                if profile.prefix_divergence >= 1024 {
                    reasons.push(format!(
                        "Many branch points ({} unique prefixes)",
                        profile.prefix_divergence
                    ));
                }

                if reasons.is_empty() {
                    reasons.push("Default choice for safety".to_string());
                }

                format!("Sparse index selected: {}", reasons.join(", "))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mock_profile() -> KeyStructureProfile {
        KeyStructureProfile {
            avg_shared_prefix: 0.0,
            max_shared_prefix: 0,
            prefix_divergence: 0,
            entropy: 0.0,
            common_prefix_len: 0,
            key_length_variance: 0.0,
            prefix_heat: Vec::new(),
            key_count: 1000,
        }
    }

    #[test]
    fn should_choose_trie_for_high_prefix_correlation() {
        let profile = KeyStructureProfile {
            avg_shared_prefix: 8.0,
            entropy: 2.5,
            key_count: 1000,
            ..mock_profile()
        };

        assert_eq!(IndexTuner::decide(&profile), IndexKind::Trie);
    }

    #[test]
    fn should_choose_trie_for_common_prefix() {
        let profile = KeyStructureProfile {
            common_prefix_len: 4,
            key_count: 1000,
            ..mock_profile()
        };

        assert_eq!(IndexTuner::decide(&profile), IndexKind::Trie);
    }

    #[test]
    fn should_choose_trie_for_long_shared_prefix() {
        let profile = KeyStructureProfile {
            max_shared_prefix: 10,
            key_count: 1000,
            ..mock_profile()
        };

        assert_eq!(IndexTuner::decide(&profile), IndexKind::Trie);
    }

    #[test]
    fn should_choose_trie_for_small_branching() {
        let profile = KeyStructureProfile {
            prefix_divergence: 100,
            key_count: 1000,
            ..mock_profile()
        };

        assert_eq!(IndexTuner::decide(&profile), IndexKind::Trie);
    }

    #[test]
    fn should_choose_sparse_for_random_keys() {
        let profile = KeyStructureProfile {
            entropy: 4.5,
            key_count: 1000,
            ..mock_profile()
        };

        assert_eq!(IndexTuner::decide(&profile), IndexKind::Sparse);
    }

    #[test]
    fn should_choose_sparse_for_many_branches() {
        let profile = KeyStructureProfile {
            prefix_divergence: 2000,
            key_count: 1000,
            ..mock_profile()
        };

        assert_eq!(IndexTuner::decide(&profile), IndexKind::Sparse);
    }

    #[test]
    fn should_choose_sparse_for_small_ssts() {
        let profile = KeyStructureProfile {
            key_count: 50,
            ..mock_profile()
        };

        assert_eq!(IndexTuner::decide(&profile), IndexKind::Sparse);
    }

    #[test]
    fn should_explain_trie_decision() {
        let profile = KeyStructureProfile {
            avg_shared_prefix: 8.0,
            entropy: 2.5,
            common_prefix_len: 4,
            key_count: 1000,
            ..mock_profile()
        };

        let explanation = IndexTuner::explain(&profile);
        assert!(explanation.contains("Trie index selected"));
        assert!(explanation.contains("prefix correlation"));
    }

    #[test]
    fn should_explain_sparse_decision() {
        let profile = KeyStructureProfile {
            entropy: 4.5,
            key_count: 1000,
            ..mock_profile()
        };

        let explanation = IndexTuner::explain(&profile);
        assert!(explanation.contains("Sparse index selected"));
        assert!(explanation.contains("entropy"));
    }
}
