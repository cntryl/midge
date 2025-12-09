pub mod planner;
pub mod strategy;
pub mod executor;
pub mod merge;

pub use planner::Planner;
pub use strategy::Strategy;
pub use executor::Executor;
pub use merge::Merge;

use crate::common::MidgeResult;

#[derive(Clone, Copy, Debug)]
pub enum CompactionStrategy {
    Leveled,
    Universal,
    Tiered,
}

pub struct Compactor;

impl Compactor {
    pub fn new() -> MidgeResult<Self> {
        Ok(Self)
    }

    pub fn compact(&mut self, _strategy: CompactionStrategy) -> MidgeResult<()> {
        todo!("Implement compaction")
    }
}
