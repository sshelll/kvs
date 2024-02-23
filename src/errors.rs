/// Error types for kvs
#[derive(Debug)]
pub enum KvsError {
    /// IO error
    Io(std::io::Error),
    /// Serde error
    Serde(serde_json::Error),
    /// Key not found
    KeyNotFound,
    /// Unexpected command type
    UnexpectedCommandType,
    /// Invalid command
    InvalidCommand,
    /// Other error
    Other(String),
}

/// Result type for the KVS project
pub type Result<T> = std::result::Result<T, KvsError>;
