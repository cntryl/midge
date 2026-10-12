//! Signatures of known #808 bug classes, so detection campaigns prove they
//! found the bug they claim. Each stage deletes the signatures it fixes.

use crate::harness::FindingKind;

/// A key came back that only an absent value explains.
pub(crate) fn resurrects(kind: &FindingKind) -> bool {
    matches!(kind, FindingKind::Data(violation) if violation.keys.iter().any(|mismatch| {
        mismatch.observed.is_some() && mismatch.allowed == [None]
    }))
}
