//! Signatures of known #808 bug classes, so detection campaigns prove they
//! found the bug they claim. Each stage deletes the signatures it fixes.

use crate::harness::{Finding, FindingKind};
use cntryl_midge::__internal::simulation::Recovery;

/// A key came back that only an absent value explains.
pub(crate) fn resurrects(kind: &FindingKind) -> bool {
    matches!(kind, FindingKind::Data(violation) if violation.keys.iter().any(|mismatch| {
        mismatch.observed.is_some() && mismatch.allowed == [None]
    }))
}

/// A value recovery had to keep came back absent.
pub(crate) fn loses_required_value(kind: &FindingKind) -> bool {
    matches!(kind, FindingKind::Data(violation) if violation.keys.iter().any(|mismatch| {
        mismatch.observed.is_none() && mismatch.allowed.iter().all(Option::is_some)
    }))
}

/// `out-of-order-publication` (#818): a fresh-cache takeover recovered a
/// WAL hole, a state that commits surviving out of order explain with every
/// promised commit kept. Any such hole counts; the ledger and contiguous
/// publication of stage 2 remove them all.
pub(crate) fn publishes_out_of_order(finding: &Finding) -> bool {
    let FindingKind::Data(violation) = &finding.kind else {
        return false;
    };
    finding.image.stop.recovery() == Recovery::FreshCache
        && finding.image.history.explains_out_of_order(violation)
}
