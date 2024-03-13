use crate::KvsEngine;
use crate::KvsError;
use crate::Result;

/// `SledStore` is a key-value store using `sled` as the backend.
pub struct SledStore {
    db: sled::Db,
}

impl KvsEngine for SledStore {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.db.insert(key, value.into_bytes()).map(|_| ())?;
        Ok(())
    }
    fn get(&mut self, key: String) -> Result<Option<String>> {
        let v = self.db.get(key)?;
        match v {
            Some(value) => Ok(Some(String::from_utf8(value.to_vec())?)),
            None => Ok(None),
        }
    }

    fn remove(&mut self, key: String) -> Result<()> {
        self.db.remove(key)?.ok_or(KvsError::KeyNotFound)?;
        self.db.flush()?;
        Ok(())
    }
}

impl SledStore {
    /// Create a new `SledStore` from a `sled::Db`.
    pub fn new(db: sled::Db) -> Self {
        SledStore { db }
    }
}
