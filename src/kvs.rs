use std::{collections::HashMap, io::Write, path};

use crate::errors::Result;
use crate::KvsError;
use serde::{Deserialize, Serialize};

/// The `KvStore` stores string key/value pairs.
///
/// Key/value pairs are stored in a `HashMap` in memory and not persisted to disk.
///
/// Example:
///
/// ```rust
/// # use kvs::KvStore;
/// let mut store = KvStore::new();
/// store.set("key".to_owned(), "value".to_owned());
/// let val = store.get("key".to_owned());
/// assert_eq!(val, Some("value".to_owned()));
/// ```
pub struct KvStore {
    data: HashMap<String, String>,
    file_path: String,
    file: Option<std::fs::File>,
}

impl KvStore {
    /// Creates a `KvStore`.
    pub fn new() -> KvStore {
        KvStore {
            data: HashMap::new(),
            file: None,
            file_path: String::new(),
        }
    }

    /// Opens a `KvStore` at a given path.
    pub fn open(p: &path::Path) -> Result<KvStore> {
        let result = std::fs::OpenOptions::new().read(true).append(true).open(p);
        match result {
            Ok(f) => {
                let mut store = KvStore::new();
                store.file = Some(f);
                store.file_path = p.to_str().unwrap().to_string();
                Ok(store)
            }
            Err(e) => {
                if e.kind() != std::io::ErrorKind::NotFound {
                    return Err(KvsError::Io(e));
                }
                let mut store = KvStore::new();
                store.file_path = p.to_str().unwrap().to_string();
                store.create_log_file()?;
                Ok(store)
            }
        }
    }

    /// Sets the value of a string key to a string.
    /// If the key already exists, the previous value will be overwritten.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.data.insert(key.clone(), value.clone());
        let log = KvLog::new("set".to_string(), key, Some(value));
        self.append_log(&log)?;
        Ok(())
    }

    /// Gets the string value of a given string key.
    /// If the key does not exist, returns `None`.
    pub fn get(&self, key: String) -> Result<Option<String>> {
        if let Some(v) = self.data.get(&key).cloned() {
            Ok(Some(v))
        } else {
            Ok(None)
        }
    }

    /// Removes a given string key from the store.
    pub fn remove(&mut self, key: String) -> Result<()> {
        self.data.remove(&key);
        let log = KvLog::new("rm".to_string(), key, None);
        self.append_log(&log)?;
        Ok(())
    }

    fn append_log(&mut self, log: &KvLog) -> Result<()> {
        let mut file = self.file.as_ref().unwrap();
        let serialized = log.serialize()?;
        let log_line = format!("{}\n", serialized);
        match file.write_all(log_line.as_bytes()) {
            Ok(_) => Ok(()),
            Err(e) => Err(KvsError::Io(e)),
        }
    }

    fn create_log_file(&mut self) -> Result<()> {
        let result = std::fs::OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&self.file_path);
        match result {
            Ok(f) => {
                self.file = Some(f);
                Ok(())
            }
            Err(e) => Err(KvsError::Io(e)),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct KvLog {
    cmd: String,
    key: String,
    value: Option<String>,
}

impl KvLog {
    fn new(cmd: String, key: String, value: Option<String>) -> KvLog {
        KvLog { cmd, key, value }
    }

    fn serialize(&self) -> Result<String> {
        match serde_json::to_string(&self) {
            Ok(s) => Ok(s),
            Err(e) => Err(KvsError::Serde(e)),
        }
    }
}
