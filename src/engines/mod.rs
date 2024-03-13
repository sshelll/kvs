use crate::Result;

/// The `KvsEngine` trait
pub trait KvsEngine {
    /// Set the value of a string key to a string
    fn set(&mut self, key: String, value: String) -> Result<()>;
    /// Get the string value of a string key. If the key does not exist, return `None`.
    fn get(&mut self, key: String) -> Result<Option<String>>;
    /// Remove a string key
    fn remove(&mut self, key: String) -> Result<()>;
}

mod kvs;
mod sled;

pub use kvs::KvStore;
pub use sled::SledStore;
