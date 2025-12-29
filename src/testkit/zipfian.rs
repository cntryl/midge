//! Zipfian distribution generator (YCSB-style).
//!
//! This is used by stress/perf workloads that want a stable, deterministic
//! Zipfian key distribution with a tunable skew parameter.

use rand::RngCore;

#[derive(Debug, Clone)]
pub struct ZipfianGenerator {
    items: usize,
    theta: f64,
    zeta_n: f64,
    alpha: f64,
    eta: f64,
}

impl ZipfianGenerator {
    pub fn new(items: usize, theta: f64) -> Self {
        let zeta_n = Self::zeta(items, theta);
        let zeta_2 = Self::zeta(2, theta);
        let alpha = 1.0 / (1.0 - theta);
        let eta = (1.0 - (2.0 / items as f64).powf(1.0 - theta)) / (1.0 - (zeta_2 / zeta_n));

        Self {
            items,
            theta,
            zeta_n,
            alpha,
            eta,
        }
    }

    fn zeta(n: usize, theta: f64) -> f64 {
        (1..=n).map(|i| 1.0 / (i as f64).powf(theta)).sum()
    }

    pub fn next<R: RngCore>(&self, rng: &mut R) -> usize {
        // Deterministic [0,1) from u64, no FP RNG in hot loop.
        let u: f64 = {
            let r = rng.next_u64();
            (r as f64) / 18446744073709551616.0 // 2^64
        };

        let uz = u * self.zeta_n;

        if uz < 1.0 {
            return 0;
        }
        if uz < 1.0 + 0.5_f64.powf(self.theta) {
            return 1;
        }

        let v = self.eta * u - (self.eta - 1.0);
        let idx = (self.items as f64 * v.powf(self.alpha)) as usize;

        idx % self.items
    }
}
