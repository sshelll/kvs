use crate::errors::Result;
use crate::KvsError;
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use std::io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::{collections::HashMap, path};

/// The `KvStore` stores string key/value pairs.
///
/// Key/value pairs are stored in a `HashMap` in memory and not persisted to disk.
///
/// Example:
///
/// ```rust
/// # use kvs::KvStore;
/// let mut store = KvStore::open(std::path::Path::new("kvs.log")).unwrap();
/// store.set("key".to_owned(), "value".to_owned());
/// let val = store.get("key".to_owned());
/// assert_eq!(val, Some("value".to_owned()));
/// ```
pub struct KvStore {
    file_path: path::PathBuf,

    index: HashMap<String, u64>,
    reader: Option<BufReaderWithPos<std::fs::File>>,
    writer: Option<BufWriterWithPos<std::fs::File>>,
}

impl KvStore {
    fn new() -> KvStore {
        KvStore {
            index: HashMap::new(),
            file_path: path::PathBuf::new(),
            reader: None,
            writer: None,
        }
    }

    /// Opens a `KvStore` at a given path.
    pub fn open(p: &path::Path) -> Result<KvStore> {
        let mut file_path = p.to_path_buf();
        if p.is_dir() {
            file_path.push("kvs.log");
        }
        eprintln!("[DEBUG] file_path: {:?}", file_path);

        let mut store = KvStore::new();
        store.file_path = file_path;

        let file = match std::fs::File::open(&store.file_path) {
            Ok(f) => Some(f),
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    None
                } else {
                    return Err(KvsError::Io(e));
                }
            }
        };

        // apply log file or create new one
        if let Some(f) = file {
            store.apply_log_file(f)?;
        }
        store.create_log_file()?;

        Ok(store)
    }

    /// Sets the value of a string key to a string.
    /// If the key already exists, the previous value will be overwritten.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let log = KvLog::new("set".to_string(), key.clone(), Some(value));
        self.append_log_file(&log)?;
        self.index
            .insert(key.clone(), self.writer.as_ref().unwrap().pos);
        Ok(())
    }

    /// Gets the string value of a given string key.
    /// If the key does not exist, returns `None`.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if !self.index.contains_key(&key) {
            return Ok(None);
        }
        let pos = self.index.get(&key).unwrap();
        eprintln!("[DEBUG] get found index pos: {:?}", pos);
        let reader = self.reader.as_mut().unwrap();
        match reader.seek(SeekFrom::Start(*pos)) {
            Ok(_) => {}
            Err(e) => return Err(KvsError::Io(e)),
        };
        let mut buf = String::new();
        reader.read_line(&mut buf)?;
        let log = KvLog::deserialize(&buf)?;
        match log.value {
            Some(v) => Ok(Some(v)),
            None => Err(KvsError::KeyNotFound),
        }
    }

    /// Removes a given string key from the store.
    pub fn remove(&mut self, key: String) -> Result<()> {
        if !self.index.contains_key(&key) {
            return Err(KvsError::KeyNotFound);
        }
        self.index.remove(&key);
        let log = KvLog::new("rm".to_string(), key, None);
        self.append_log_file(&log)?;
        Ok(())
    }

    fn append_log_file(&mut self, log: &KvLog) -> Result<()> {
        let serialized = log.serialize()?;
        let log_line = format!("{}\n", serialized);
        let writer = self.writer.as_mut().unwrap();
        match writer.write_all(log_line.as_bytes()) {
            Ok(_) => {}
            Err(e) => return Err(KvsError::Io(e)),
        };
        match writer.flush() {
            Ok(_) => {}
            Err(e) => return Err(KvsError::Io(e)),
        }
        match writer.seek(SeekFrom::End(0)) {
            Ok(_) => {
                eprintln!("[DEBUG] append_log_file seek pos now is {}", writer.pos);
            }
            Err(e) => return Err(KvsError::Io(e)),
        }
        Ok(())
    }

    fn create_log_file(&mut self) -> Result<()> {
        let result = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .create(true)
            .open(&self.file_path);
        match result {
            Ok(f) => {
                match std::fs::File::open(&self.file_path) {
                    Ok(f) => {
                        self.reader = Some(BufReaderWithPos::new(f)?);
                    }
                    Err(e) => {
                        return Err(KvsError::Io(e));
                    }
                }
                self.writer = Some(BufWriterWithPos::new(f)?);
                match self.writer.as_mut().unwrap().seek(SeekFrom::End(0)) {
                    Ok(_) => {}
                    Err(e) => return Err(KvsError::Io(e)),
                }
                Ok(())
            }
            Err(e) => Err(KvsError::Io(e)),
        }
    }

    fn apply_log_file(&mut self, f: std::fs::File) -> Result<()> {
        let mut reader = BufReaderWithPos::new(f)?;

        // reset pos to 0
        let mut pos = match reader.seek(SeekFrom::Start(0)) {
            Ok(p) => p,
            Err(e) => return Err(KvsError::Io(e)),
        };

        // start read and deserialize
        let mut stream = Deserializer::from_reader(reader).into_iter::<KvLog>();
        while let Some(log) = stream.next() {
            match log {
                Ok(l) => match l.cmd.as_str() {
                    "set" => {
                        // only store pos in memory
                        self.index.insert(l.key.clone(), pos);
                    }
                    "rm" => {
                        // remove from memory
                        self.index.remove(&l.key);
                    }
                    _ => return Err(KvsError::InvalidCommand(l.cmd)),
                },
                Err(e) => {
                    return {
                        eprintln!("[DEBUG] apply_log_file error: {:?}", e);
                        Err(KvsError::Serde(e))
                    }
                }
            }
            // update pos
            pos = stream.byte_offset() as u64 + 1;
        }

        Ok(())
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

    fn deserialize(s: &str) -> Result<KvLog> {
        match serde_json::from_str(s) {
            Ok(l) => Ok(l),
            Err(e) => Err(KvsError::Serde(e)),
        }
    }
}

struct BufReaderWithPos<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u64,
}

impl<R: Read + Seek> BufReaderWithPos<R> {
    fn new(mut inner: R) -> Result<Self> {
        match inner.seek(SeekFrom::Current(0)) {
            Ok(pos) => Ok(BufReaderWithPos {
                reader: BufReader::new(inner),
                pos,
            }),
            Err(e) => Err(KvsError::Io(e)),
        }
    }

    fn read_line(&mut self, buf: &mut String) -> Result<usize> {
        match self.reader.read_line(buf) {
            Ok(n) => {
                self.pos += n as u64;
                Ok(n)
            }
            Err(e) => Err(KvsError::Io(e)),
        }
    }
}

impl<R: Read + Seek> Read for BufReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self.reader.read(buf) {
            Ok(n) => {
                self.pos += n as u64;
                Ok(n)
            }
            Err(e) => Err(e),
        }
    }
}

impl<R: Read + Seek> Seek for BufReaderWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        match self.reader.seek(pos) {
            Ok(p) => {
                self.pos = p;
                Ok(p)
            }
            Err(e) => Err(e),
        }
    }
}

struct BufWriterWithPos<W: Write + Seek> {
    writer: BufWriter<W>,
    pos: u64,
}

impl<W: Write + Seek> BufWriterWithPos<W> {
    fn new(mut inner: W) -> Result<Self> {
        match inner.seek(SeekFrom::Current(0)) {
            Ok(pos) => Ok(BufWriterWithPos {
                writer: BufWriter::new(inner),
                pos,
            }),
            Err(e) => Err(KvsError::Io(e)),
        }
    }
}

impl<W: Write + Seek> Write for BufWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write + Seek> Seek for BufWriterWithPos<W> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
    }
}
