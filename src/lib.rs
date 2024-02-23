// use this code to force every public item to have documentation:
#![deny(missing_docs)]
//! A simple key-value store.

mod errors;
mod kvs;

pub use errors::KvsError;
pub use errors::Result;
pub use kvs::KvStore;
