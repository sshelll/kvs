// use this code to force every public item to have documentation:
#![deny(missing_docs)]
//! A simple key-value store.

mod client;
mod engines;
mod errors;
mod protocol;
mod server;

pub use client::KvsClient;
pub use engines::KvStore;
pub use engines::KvsEngine;
pub use errors::KvsError;
pub use errors::Result;
pub use server::KvsServer;
