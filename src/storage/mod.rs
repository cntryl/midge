pub mod filesystem;
pub mod cloud;
pub mod hybrid;
pub mod paths;

pub use filesystem::FileSystem;
pub use cloud::CloudStorage;
pub use hybrid::HybridStorage;
pub use paths::Paths;

use crate::common::MidgeResult;

pub trait StorageBackend: Send + Sync {
    fn read(&self, path: &str) -> MidgeResult<Vec<u8>>;
    fn write(&mut self, path: &str, data: &[u8]) -> MidgeResult<()>;
    fn delete(&mut self, path: &str) -> MidgeResult<()>;
    fn list(&self, prefix: &str) -> MidgeResult<Vec<String>>;
}
