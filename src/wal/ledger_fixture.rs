//! Test fixtures that record a WAL history for segment files a test wrote
//! by hand. Recovery replays only what `wal/LEDGER` names, so a fixture that
//! writes segments must also write the ledger naming them.

use super::ledger::{LedgerEdit, SegmentId, WalLedger};
use super::ledger_store::LedgerStore;
use crate::common::MidgeResult;
use crate::io::{Fs, FsError, FsPath};
use std::sync::Arc;

/// The ledger of a WAL whose history is `sealed`, oldest first, then the
/// open segment `open`: genesis at the first id, a rotation for each next
/// consecutive id, and a set-aside across each gap.
///
/// # Panics
///
/// Panics unless the ids strictly increase.
pub(crate) fn ledger(sealed: &[SegmentId], open: SegmentId) -> WalLedger {
    let mut ids = sealed.iter().copied().chain(std::iter::once(open));
    let first = ids.next().expect("a WAL history has an open segment");
    let mut ledger = WalLedger::empty();
    ledger
        .apply(&LedgerEdit::Genesis {
            holder: "ledger-fixture".to_string(),
            open: first,
        })
        .expect("record the first WAL segment");
    for id in ids {
        let current = ledger.open_segment();
        let edit = if Some(id) == current.checked_add(1) {
            LedgerEdit::Rotate {
                seal: current,
                open: id,
            }
        } else {
            LedgerEdit::SetAside {
                from: current + 1,
                open: id,
            }
        };
        ledger
            .apply(&edit)
            .unwrap_or_else(|error| panic!("record WAL segment {id}: {error}"));
    }
    ledger
}

/// Write `wal/LEDGER` naming `sealed` then `open` into the WAL directory
/// `wal`, which is created if missing. Segment files are left to the caller.
pub(crate) fn record(
    wal: &Arc<dyn Fs>,
    sealed: &[SegmentId],
    open: SegmentId,
) -> MidgeResult<WalLedger> {
    wal.create_dir_all(&FsPath::new(""))
        .map_err(FsError::into_midge)?;
    let ledger = ledger(sealed, open);
    LedgerStore::create(Arc::clone(wal), &ledger)?;
    Ok(ledger)
}

/// [`record`] for the `wal/` directory of the database at `db_path`.
pub(crate) fn record_at(
    db_path: &std::path::Path,
    sealed: &[SegmentId],
    open: SegmentId,
) -> MidgeResult<WalLedger> {
    let wal: Arc<dyn Fs> = Arc::new(
        crate::io::RealFs::new(db_path.join(super::WAL_DIR)).map_err(FsError::into_midge)?,
    );
    record(&wal, sealed, open)
}
