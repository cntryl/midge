//! Foundational types and traits - zero external dependencies

pub mod error;
pub mod ack_policy;
pub mod singleflight;
pub mod tlv;

pub use error::{MidgeError, MidgeResult};
pub use ack_policy::AckPolicy;
pub use singleflight::{Accumulator, FlushPolicy, KeyedGroupCommit};
