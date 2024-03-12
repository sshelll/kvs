use crate::errors::Result;
use crate::{KvsEngine, KvsError};
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::{collections::HashMap, path};

const COMPACTION_THRESHOLD: u64 = 1024 * 1024; // 1MB

/// The `KvStore` stores string key/value pairs.
pub struct KvStore {
    index: HashMap<String, IndexPos>,
    reader: HashMap<u64, BufReaderWithPos<File>>,
    writer: BufWriterWithPos<File>,

    path: path::PathBuf,
    current_gen: u64,
    uncompacted: u64,
}

impl KvsEngine for KvStore {
    /// Sets the value of a string key to a string.
    /// If the key already exists, the previous value will be overwritten.
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let log = KvLog::Set {
            key: key.clone(),
            value,
        };

        let old_pos = self.writer.pos;
        self.append_log_file(&log)?;
        let cur_pos = self.writer.pos;

        if let Some(old) = self
            .index
            .insert(key, (self.current_gen, old_pos..cur_pos).into())
        {
            self.uncompacted += old.len;
        }

        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compact()?;
        }
        Ok(())
    }

    /// Gets the string value of a given string key.
    /// If the key does not exist, returns `None`.
    fn get(&mut self, key: String) -> Result<Option<String>> {
        if !self.index.contains_key(&key) {
            return Ok(None);
        }
        let index_pos = self.index.get(&key).unwrap();
        let reader = self.reader.get_mut(&index_pos.gen).unwrap();
        if let Err(e) = reader.seek(SeekFrom::Start(index_pos.pos)) {
            return Err(KvsError::Io(e));
        }
        let mut buf = String::new();
        reader.read_line(&mut buf)?;
        let log = KvLog::deserialize(&buf)?;
        match log {
            KvLog::Set { value, .. } => Ok(Some(value)),
            KvLog::Remove { .. } => Ok(None),
        }
    }

    /// Removes a given string key from the store.
    fn remove(&mut self, key: String) -> Result<()> {
        if !self.index.contains_key(&key) {
            return Err(KvsError::KeyNotFound);
        }
        let log = KvLog::Remove { key: key.clone() };
        self.append_log_file(&log)?;
        self.index.remove(&key);
        Ok(())
    }
}

impl KvStore {
    /// Opens a `KvStore` at a given path.
    pub fn open(p: &path::Path) -> Result<KvStore> {
        let file_path = p.to_path_buf();
        if !p.is_dir() {
            return Err(KvsError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "path must be a dir",
            )));
        }

        let mut index: HashMap<String, IndexPos> = HashMap::new();
        let mut reader_map: HashMap<u64, BufReaderWithPos<File>> = HashMap::new();
        let mut uncompacted: u64 = 0;
        let gen_list = Self::get_sorted_gen_list(p)?;
        for &gen in &gen_list {
            let file_path = Self::log_file_path(p, gen);
            let mut reader = BufReaderWithPos::new(File::open(&file_path)?)?;
            uncompacted += Self::replay_log_file(gen, &mut reader, &mut index)?;
            reader_map.insert(gen, reader);
        }

        let current_gen = gen_list.last().unwrap_or(&0) + 1;

        let writer = Self::create_log_file(&file_path, current_gen, &mut reader_map)?;

        Ok(KvStore {
            index,
            reader: reader_map,
            writer,
            path: file_path,
            current_gen,
            uncompacted,
        })
    }

    fn append_log_file(&mut self, log: &KvLog) -> Result<()> {
        let serialized = log.serialize()?;
        let log_line = format!("{}\n", serialized);
        self.writer.write(log_line.as_bytes())?;
        self.writer.flush()?;
        Ok(())
    }

    fn log_file_path(p: &path::Path, gen: u64) -> path::PathBuf {
        p.join(format!("{}.log", gen))
    }

    fn get_sorted_gen_list(dir_path: &path::Path) -> Result<Vec<u64>> {
        let mut gen_list: Vec<u64> = std::fs::read_dir(dir_path)?
            .flat_map(|entry| -> Result<_> { Ok(entry?.path()) })
            .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
            .flat_map(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .map(|name| name.trim_end_matches(".log"))
                    .map(|name| name.parse::<u64>())
            })
            .flatten()
            .collect();
        gen_list.sort_unstable();
        Ok(gen_list)
    }

    fn create_log_file(
        dir_path: &path::PathBuf,
        gen: u64,
        reader_map: &mut HashMap<u64, BufReaderWithPos<File>>,
    ) -> Result<BufWriterWithPos<File>> {
        let file_path = Self::log_file_path(dir_path, gen);
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .create(true)
            .open(&file_path)?;
        let writer = BufWriterWithPos::new(file)?;
        reader_map
            .entry(gen)
            .or_insert(BufReaderWithPos::new(File::open(&file_path)?)?);
        Ok(writer)
    }

    fn replay_log_file(
        gen: u64,
        reader: &mut BufReaderWithPos<File>,
        index: &mut HashMap<String, IndexPos>,
    ) -> Result<u64> {
        let mut uncompacted = 0;

        // reset pos to 0
        let mut pos = reader.seek(SeekFrom::Start(0))?;

        // start read and deserialize
        let mut stream = Deserializer::from_reader(reader).into_iter::<KvLog>();
        while let Some(log) = stream.next() {
            let cur_pos = stream.byte_offset() as u64;
            match log? {
                KvLog::Set { key, .. } => {
                    // if key exists, 'insert' will return the old value.
                    if let Some(old_index) = index.insert(key, (gen, pos..cur_pos).into()) {
                        uncompacted += old_index.len;
                    }
                }
                KvLog::Remove { key } => {
                    if let Some(old_index) = index.remove(&key) {
                        uncompacted += old_index.len;
                    }
                    // NOTE: the remove log itself can be compacted.
                    uncompacted += cur_pos - pos;
                }
            }
            // NOTE: we need to add 1 to cur_pos to include the '\n' character
            pos = cur_pos + 1;
        }

        Ok(uncompacted)
    }

    fn compact(&mut self) -> Result<()> {
        // for example, if current_gen is 1, then compact_gen is 2 and new_gen is 3
        // after compaction, new commands will be written to gen 3
        // which means gen-2 is compacted and gen-3 is not.
        let compact_gen = self.current_gen + 1;
        self.current_gen += 2;
        self.writer = Self::create_log_file(&self.path, self.current_gen, &mut self.reader)?;

        // copy to compacted log file
        let mut compact_writer = Self::create_log_file(&self.path, compact_gen, &mut self.reader)?;
        for index_pos in self.index.values() {
            let reader = self
                .reader
                .get_mut(&index_pos.gen)
                .expect("reader not found");
            if reader.pos != index_pos.pos {
                reader.seek(SeekFrom::Start(index_pos.pos))?;
            }
            let mut buf = String::new();
            reader.read_line(&mut buf)?;
            compact_writer.write(buf.as_bytes())?;
        }
        compact_writer.flush()?;

        // remove old log files and update reader map
        let should_removed_gens: Vec<u64> = self
            .reader
            .keys()
            .filter(|&&k| k < compact_gen)
            .cloned()
            .collect();
        for gen in should_removed_gens {
            self.reader.remove(&gen);
            fs::remove_file(Self::log_file_path(&self.path, gen))?
        }

        self.uncompacted = 0;
        Ok(())
    }
}

#[allow(dead_code)]
struct IndexPos {
    gen: u64,
    pos: u64,
    len: u64,
}

impl From<(u64, Range<u64>)> for IndexPos {
    fn from((gen, range): (u64, Range<u64>)) -> Self {
        IndexPos {
            gen,
            pos: range.start,
            len: range.end - range.start,
        }
    }
}

#[derive(Serialize, Deserialize)]
enum KvLog {
    Set { key: String, value: String },
    Remove { key: String },
}

impl KvLog {
    fn serialize(&self) -> Result<String> {
        Ok(serde_json::to_string(&self)?)
    }

    fn deserialize(s: &str) -> Result<KvLog> {
        Ok(serde_json::from_str(s)?)
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
