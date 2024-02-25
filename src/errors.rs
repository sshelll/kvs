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
    InvalidCommand(String),
    /// Other error
    Other(String),
}

impl std::fmt::Display for KvsError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            KvsError::Io(e) => write!(f, "IO error: {}", e),
            KvsError::Serde(e) => write!(f, "Serde error: {}", e),
            KvsError::KeyNotFound => write!(f, "Key not found"),
            KvsError::UnexpectedCommandType => write!(f, "Unexpected command type"),
            KvsError::InvalidCommand(s) => write!(f, "Invalid command: {}", s),
            KvsError::Other(s) => write!(f, "Unknown error: {}", s),
        }
    }
}

/// Result type for the KVS project
pub type Result<T> = std::result::Result<T, KvsError>;
