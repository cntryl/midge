//! Engine opening and initialization

use super::MidgeEngine;
use crate::common::MidgeResult;
use std::path::PathBuf;

/// Open a Midge database at the given path
pub fn open_engine(db_path: PathBuf) -> MidgeResult<MidgeEngine> {
    MidgeEngine::open(db_path)
}
