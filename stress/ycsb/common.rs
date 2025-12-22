//! Shared utilities for YCSB stress workloads (zipfian, pregens, etc.)
//!
//! Start with the Zipfian generator; additional shared helpers can be added
//! here as needed.

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

    /// Return a Zipfian-distributed index in [0, items).
    pub fn next(&self, rng: &mut impl rand::RngCore) -> usize {
        let r = rng.next_u64();
        let u: f64 = (r as f64) / 18446744073709551616.0; // 2^64
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

#[cfg(test)]
mod tests {
    use super::ZipfianGenerator;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    #[test]
    fn next_in_range() {
        let mut rng = StdRng::seed_from_u64(0xDEAD_BEEF);
        let z = ZipfianGenerator::new(1000, 0.99);
        for _ in 0..1000 {
            let v = z.next(&mut rng);
            assert!(v < 1000);
        }
    }
}
