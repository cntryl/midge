//! The WAL segment format (#843): a header that links each segment to the
//! one before it, and a footer that seals it. See [`segment`] for the
//! layout and [`link`] for how segments chain.

pub mod link;
pub mod segment;

pub use link::{follows, Chain, ChainLink, OpenFacts, SegmentId};
pub use segment::{
    holds_no_records, SealedFacts, SegmentHeader, SegmentScan, SegmentTail, HEADER_LEN,
};
pub(crate) use segment::{Footer, ScannedFrame, SegmentDigest, SegmentIdentity};
