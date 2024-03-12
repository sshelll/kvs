/// Error types for kvs
#[derive(Debug)]
pub enum KvsError {
    /// IO error
    Io(std::io::Error),
    /// Serde error
    Serde(serde_json::Error),
    /// Key not found
    KeyNotFound,
    /// Invalid command
    InvalidCommand(String),
    /// Other error
    Other(String),
}

impl From<std::io::Error> for KvsError {
    fn from(err: std::io::Error) -> KvsError {
        KvsError::Io(err)
    }
}

impl From<serde_json::Error> for KvsError {
    fn from(err: serde_json::Error) -> KvsError {
        KvsError::Serde(err)
    }
}

impl From<String> for KvsError {
    fn from(value: String) -> Self {
        KvsError::Other(value)
    }
}

impl std::fmt::Display for KvsError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            KvsError::Io(e) => write!(f, "IO error: {}", e),
            KvsError::Serde(e) => write!(f, "Serde error: {}", e),
            KvsError::KeyNotFound => write!(f, "Key not found"),
            KvsError::InvalidCommand(s) => write!(f, "Invalid command: {}", s),
            KvsError::Other(s) => write!(f, "Unknown error: {}", s),
        }
    }
}

/// Result type for the KVS project
pub type Result<T> = std::result::Result<T, KvsError>;
