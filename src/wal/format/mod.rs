//! The WAL segment format (#843): a header that links each segment to the
//! one before it, and a footer that seals it. See [`segment`] for the
//! layout and [`link`] for how segments chain.

pub mod link;
pub mod segment;

pub(crate) use link::{Chain, ChainLink};
pub(crate) use segment::{
    holds_no_records, Footer, ScannedFrame, SealedFacts, SegmentHeader, SegmentScan, SegmentTail,
    HEADER_LEN,
};
