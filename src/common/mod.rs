//! Foundational types and traits - zero external dependencies

pub mod ack_policy;
pub mod error;
pub mod singleflight;
pub mod tlv;

pub use ack_policy::AckPolicy;
pub use error::{MidgeError, MidgeResult};
pub use singleflight::{Accumulator, FlushPolicy, KeyedGroupCommit};
