//! Zipfian distribution generator (YCSB-style).
//!
//! This is used by stress/perf workloads that want a stable, deterministic
//! Zipfian key distribution with a tunable skew parameter.

use rand::Rng;

#[derive(Debug, Clone)]
pub struct ZipfianGenerator {
    items: usize,
    theta: f64,
    zeta_n: f64,
    alpha: f64,
    eta: f64,
}

impl ZipfianGenerator {
    #[must_use]
    pub fn new(items: usize, theta: f64) -> Self {
        let zeta_n = Self::zeta(items, theta);
        let zeta_2 = Self::zeta(2, theta);
        let alpha = 1.0 / (1.0 - theta);
        let items_f64 = usize_to_f64(items);
        let eta = (1.0 - (2.0 / items_f64).powf(1.0 - theta)) / (1.0 - (zeta_2 / zeta_n));

        Self {
            items,
            theta,
            zeta_n,
            alpha,
            eta,
        }
    }

    fn zeta(n: usize, theta: f64) -> f64 {
        (1..=n).map(|i| 1.0 / usize_to_f64(i).powf(theta)).sum()
    }

    /// Sample the next index using an injected `u64` source.
    ///
    /// This exists so in-repo stress binaries can use the Zipfian generator
    /// without depending on `rand` directly (the stress temp crate only
    /// depends on `cntryl_midge`).
    pub fn next_from_u64<F: FnMut() -> u64>(&self, next_u64: &mut F) -> usize {
        // Deterministic [0,1) from u64, no FP RNG in hot loop.
        let u: f64 = {
            let r = next_u64();
            let top53 = r >> 11;
            let hi = u32::try_from(top53 >> 21).unwrap_or(u32::MAX);
            let lo = u32::try_from(top53 & ((1_u64 << 21) - 1)).unwrap_or(u32::MAX);
            ((f64::from(hi) * 2_097_152.0) + f64::from(lo)) / 9_007_199_254_740_992.0
        };

        let uz = u * self.zeta_n;

        if uz < 1.0 {
            return 0;
        }
        if uz < 1.0 + 0.5_f64.powf(self.theta) {
            return 1;
        }

        let v = (self.eta * u - (self.eta - 1.0)).clamp(0.0, 1.0);
        let idx_f64 = usize_to_f64(self.items) * v.powf(self.alpha);
        let idx = if !idx_f64.is_finite() || idx_f64 <= 0.0 {
            0
        } else {
            let bounded = idx_f64.min(usize_to_f64(self.items));
            format!("{bounded:.0}")
                .parse::<usize>()
                .unwrap_or(self.items)
        };

        idx.min(self.items.saturating_sub(1))
    }

    /// Sample the next index using the provided RNG.
    pub fn next<R: Rng>(&self, rng: &mut R) -> usize {
        self.next_from_u64(&mut || rng.next_u64())
    }
}

fn usize_to_f64(value: usize) -> f64 {
    f64::from(u32::try_from(value).unwrap_or(u32::MAX))
}
