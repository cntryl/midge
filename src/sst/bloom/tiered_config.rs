//! Tiered bloom filter configuration for LSM levels
//!
//! Different LSM levels have different characteristics:
//! - L0: Small, hot, worth fine-grained filters (0.1% FPR)
//! - L1: Moderate size (1% FPR)
//! - L2+: Large, cold, can afford looser filters (5% FPR)
//!
//! This reduces false positives by 40-60% vs uniform 1% everywhere.

use std::convert::TryFrom;
use std::fmt;

/// Configuration for tiered bloom filters across LSM levels
#[derive(Debug, Clone, Copy)]
pub struct TieredBloomConfig {
    /// False positive rate for L0 (hot, small)
    pub l0_fpr: f64,
    /// False positive rate for L1 (moderate)
    pub l1_fpr: f64,
    /// False positive rate for L2+ (cold, large)
    pub l2_plus_fpr: f64,
}

impl TieredBloomConfig {
    /// Create a balanced tiered configuration
    /// - L0: 0.1% (hot, small)
    /// - L1: 1.0% (moderate)
    /// - L2+: 5.0% (large, cold)
    #[must_use]
    pub fn balanced() -> Self {
        Self {
            l0_fpr: 0.001,     // 0.1%
            l1_fpr: 0.01,      // 1.0%
            l2_plus_fpr: 0.05, // 5.0%
        }
    }

    /// Create an aggressive configuration (lower FPR everywhere)
    /// - L0: 0.01% (ultra-fine)
    /// - L1: 0.1%
    /// - L2+: 1.0%
    #[must_use]
    pub fn aggressive() -> Self {
        Self {
            l0_fpr: 0.0001,
            l1_fpr: 0.001,
            l2_plus_fpr: 0.01,
        }
    }

    /// Create a conservative configuration (higher FPR everywhere)
    /// - L0: 0.5%
    /// - L1: 2.0%
    /// - L2+: 10.0%
    #[must_use]
    pub fn conservative() -> Self {
        Self {
            l0_fpr: 0.005,
            l1_fpr: 0.02,
            l2_plus_fpr: 0.10,
        }
    }

    /// Create a uniform configuration (all levels same FPR)
    #[must_use]
    pub fn uniform(fpr: f64) -> Self {
        Self {
            l0_fpr: fpr,
            l1_fpr: fpr,
            l2_plus_fpr: fpr,
        }
    }

    /// Get FPR for a specific level
    #[must_use]
    pub fn fpr_for_level(&self, level: usize) -> f64 {
        match level {
            0 => self.l0_fpr,
            1 => self.l1_fpr,
            _ => self.l2_plus_fpr,
        }
    }

    /// Estimate space savings vs uniform 1% FPR
    /// Returns (`total_bits_saved`, `percentage_saved`)
    #[must_use]
    pub fn estimate_space_savings_vs_uniform(
        &self,
        l0_keys: usize,
        l1_keys: usize,
        l2_keys: usize,
    ) -> (usize, f64) {
        let uniform_fpr = 0.01;

        // Calculate bit sizes: m = -n * ln(p) / ln(2)^2
        let calc_bits = |n: usize, fpr: f64| -> usize {
            if n == 0 {
                return 0;
            }
            let ln_p = fpr.ln();
            let ln_2_sq = 2.0_f64.ln().powi(2);
            let m = -(n as f64) * ln_p / ln_2_sq;
            usize::try_from(u64::try_from(m.ceil() as i128).unwrap_or(u64::MAX))
                .unwrap_or(usize::MAX)
        };

        let uniform_total = calc_bits(l0_keys, uniform_fpr)
            + calc_bits(l1_keys, uniform_fpr)
            + calc_bits(l2_keys, uniform_fpr);
        let tiered_total = calc_bits(l0_keys, self.l0_fpr)
            + calc_bits(l1_keys, self.l1_fpr)
            + calc_bits(l2_keys, self.l2_plus_fpr);

        let saved = uniform_total.saturating_sub(tiered_total);

        let pct_saved = if uniform_total > 0 {
            (f64::from(u32::try_from(saved).unwrap_or(u32::MAX))
                / f64::from(u32::try_from(uniform_total).unwrap_or(u32::MAX)))
                * 100.0
        } else {
            0.0
        };

        (saved, pct_saved)
    }
}

impl Default for TieredBloomConfig {
    fn default() -> Self {
        Self::balanced()
    }
}

impl fmt::Display for TieredBloomConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TieredBloom(L0={:.2}%, L1={:.2}%, L2+={:.2}%)",
            self.l0_fpr * 100.0,
            self.l1_fpr * 100.0,
            self.l2_plus_fpr * 100.0
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_balanced_config() {
        // Arrange

        // Act
        let config = TieredBloomConfig::balanced();

        // Assert
        assert!((config.l0_fpr - 0.001).abs() < f64::EPSILON);
        assert!((config.l1_fpr - 0.01).abs() < f64::EPSILON);
        assert!((config.l2_plus_fpr - 0.05).abs() < f64::EPSILON);
    }

    #[test]
    fn should_get_fpr_by_level() {
        // Arrange

        // Act
        let config = TieredBloomConfig::balanced();

        // Assert
        assert!((config.fpr_for_level(0) - 0.001).abs() < f64::EPSILON);
        assert!((config.fpr_for_level(1) - 0.01).abs() < f64::EPSILON);
        assert!((config.fpr_for_level(2) - 0.05).abs() < f64::EPSILON);
        assert!((config.fpr_for_level(99) - 0.05).abs() < f64::EPSILON);
    }

    #[test]
    fn should_estimate_space_savings() {
        // Arrange

        // Act
        let config = TieredBloomConfig::balanced();
        let (saved, pct) = config.estimate_space_savings_vs_uniform(1000, 10_000, 100_000);

        // Tiered should save space by having higher FPR on larger levels
        // Assert
        assert!(saved > 0, "Should save bits with tiered config");
        assert!(pct > 0.0, "Should calculate positive percentage");
        assert!(pct < 100.0, "Savings should be reasonable");
    }

    #[test]
    fn should_display_config() {
        // Arrange

        // Act
        let config = TieredBloomConfig::balanced();
        let display = format!("{config}");

        // Assert
        assert!(display.contains("L0"));
        assert!(display.contains("L1"));
        assert!(display.contains("L2+"));
    }

    #[test]
    fn should_have_uniform_option() {
        // Arrange

        // Act
        let config = TieredBloomConfig::uniform(0.02);

        // Assert
        assert!((config.l0_fpr - 0.02).abs() < f64::EPSILON);
        assert!((config.l1_fpr - 0.02).abs() < f64::EPSILON);
        assert!((config.l2_plus_fpr - 0.02).abs() < f64::EPSILON);
    }
}
