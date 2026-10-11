//! Searching seeds for findings and shrinking them.

use crate::config::Config;
use crate::harness::{Campaign, Finding};
use crate::workload::Profile;

/// Run `profile` over `seeds`; return the first finding, shrunk.
pub(crate) fn campaign(
    profile: Profile,
    seeds: std::ops::Range<u64>,
    len: usize,
    config: Config,
) -> Option<Finding> {
    let found = seeds
        .map(|seed| (seed, profile.workload(seed, len, config.mode)))
        .find_map(|(seed, workload)| Campaign::run(seed, &workload, config).err())?;
    let class = found.kind.clone();
    Some(shrink(*found, config, |smaller| {
        smaller.kind.same_class(&class)
    }))
}

/// Drop operations one at a time while `keep` holds for the smaller finding.
fn shrink(mut finding: Finding, config: Config, keep: impl Fn(&Finding) -> bool) -> Finding {
    let mut index = 0;
    let mut attempts = 0;
    while index < finding.workload.len() && attempts < 80 {
        attempts += 1;
        let mut candidate = finding.workload.clone();
        candidate.remove(index);
        match Campaign::run(finding.seed, &candidate, config) {
            Err(smaller) if keep(&smaller) => finding = *smaller,
            _ => index += 1,
        }
    }
    finding
}
