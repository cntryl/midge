pub mod local;
pub mod hybrid;
pub mod batched_sync;
pub mod cloud;

pub use local::LocalWal;
pub use hybrid::HybridWal;
pub use batched_sync::BatchedSyncWal;
pub use cloud::CloudWal;
