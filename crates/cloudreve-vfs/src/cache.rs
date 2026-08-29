//! On-disk block cache.
//!
//! Pure disk logic: no HTTP, no async, no knowledge of `tree.rs`/`vfs.rs`.
//! Task 6's facade wraps a single instance in a `Mutex`, so every method
//! here takes `&mut self` even where a shared reference would technically
//! do — that keeps the locking story at the facade simple (one lock, one
//! writer at a time) instead of splitting it between interior mutability
//! here and a mutex there.
//!
//! Layout on disk, under the cache root: one subdirectory per remote file,
//! named with the first 16 hex chars of `sha256(remote_path)`, containing:
//! - `data`: a sparse file, block `i` written at offset `i * BLOCK_SIZE`.
//! - `meta.json`: `{"etag", "blocks": [u64...], "last_used_unix": i64}`.
//!
//! The directory name depends only on `remote_path`, never on the etag, so
//! a file that gets a new etag reuses the same directory (after the stale
//! contents are dropped) instead of leaking an orphaned one under the old
//! etag's hash.

use std::collections::{BTreeSet, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Every block, including the final one of a file, is capped at this size.
/// 1 MiB balances HTTP range-request overhead (too small = too many
/// requests) against wasted re-download on a partial-block cache miss (too
/// large = re-fetching a whole block for one changed byte).
pub const BLOCK_SIZE: u64 = 1_048_576;

/// Identifies one version of one remote file. The cache directory is keyed
/// by `remote_path` alone (see module docs); `etag` is what `read_block`/
/// `write_block` compare against the stored entry to detect that the
/// server has since replaced the file's content.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FileKey {
    pub remote_path: String,
    pub etag: String,
}

/// What `meta.json` holds. Deliberately does not repeat `remote_path`: the
/// directory name is already `hash(remote_path)`, so recovering the hash
/// from a key never needs a reverse lookup — `open()` only needs to know
/// which hash directories exist, not what path each one came from.
#[derive(Debug, Serialize, Deserialize)]
struct MetaFile {
    etag: String,
    blocks: Vec<u64>,
    last_used_unix: i64,
}

/// In-memory state for one cached file.
struct Entry {
    etag: String,
    /// Which block indices are actually present in `data`. Absence is not
    /// an error case: a file's blocks are typically filled in one at a
    /// time as they're read on demand, so most indices below the highest
    /// one ever written may legitimately be missing.
    blocks: BTreeSet<u64>,
    pinned: bool,
    /// Persisted so recency order survives a restart (`open()` re-derives
    /// `seq` from this). Second resolution, so two accesses in the same
    /// wall-clock second are indistinguishable on disk — `seq` is what
    /// actually orders eviction within a process; this field exists only
    /// to seed that order from a fresh `open()`.
    last_used_unix: i64,
    /// Monotonic recency counter, process-local. Wall-clock seconds are
    /// too coarse to order two accesses that land in the same second (a
    /// realistic case in tests and under load), so eviction picks the
    /// LRU victim by comparing `seq`, not `last_used_unix`.
    seq: u64,
}

pub struct BlockCache {
    root: PathBuf,
    max_bytes: u64,
    /// Keyed by the same hex hash used for the on-disk directory name, not
    /// by `remote_path` — the two are equivalent as lookup keys (the hash
    /// is a pure function of the path), and using the hash means `open()`
    /// never needs to recover `remote_path` from disk.
    entries: HashMap<String, Entry>,
    next_seq: u64,
}

impl BlockCache {
    /// Rebuilds cache state by scanning every `meta.json` under `root`, so
    /// an app restart doesn't discard blocks fetched in a previous run. A
    /// directory whose `meta.json` is missing or unreadable is treated as
    /// not part of the cache (skipped, left alone on disk) rather than
    /// failing `open()` outright: a corrupt entry should degrade to a
    /// cache miss, not take down the whole cache.
    pub fn open(root: &Path, max_bytes: u64) -> Result<Self> {
        fs::create_dir_all(root)
            .with_context(|| format!("failed to create cache root {}", root.display()))?;

        let mut loaded: Vec<(String, Entry)> = Vec::new();
        for dir_entry in fs::read_dir(root)
            .with_context(|| format!("failed to read cache root {}", root.display()))?
        {
            let dir_entry = dir_entry?;
            if !dir_entry.file_type()?.is_dir() {
                continue;
            }
            let hash = dir_entry.file_name().to_string_lossy().into_owned();
            let meta_path = dir_entry.path().join("meta.json");
            let Ok(raw) = fs::read(&meta_path) else {
                continue; // no meta.json: not a cache entry (or mid-write, being conservative)
            };
            let meta: MetaFile = match serde_json::from_slice(&raw) {
                Ok(meta) => meta,
                Err(err) => {
                    tracing::warn!(hash = %hash, %err, "cache: dropping unreadable meta.json");
                    continue;
                }
            };
            loaded.push((
                hash,
                Entry {
                    etag: meta.etag,
                    blocks: meta.blocks.into_iter().collect(),
                    pinned: false,
                    last_used_unix: meta.last_used_unix,
                    seq: 0, // assigned below, in last-used order
                },
            ));
        }

        // Hand out sequence numbers in persisted-recency order so eviction
        // right after a restart still picks the true LRU entry, even
        // though same-second timestamps can't be told apart by value alone.
        loaded.sort_by_key(|(_, entry)| entry.last_used_unix);
        let mut entries = HashMap::with_capacity(loaded.len());
        for (seq, (hash, mut entry)) in loaded.into_iter().enumerate() {
            entry.seq = seq as u64;
            entries.insert(hash, entry);
        }
        let next_seq = entries.len() as u64;

        Ok(Self { root: root.to_path_buf(), max_bytes, entries, next_seq })
    }

    /// None if the block is absent OR the stored etag differs (stale).
    pub fn read_block(&mut self, key: &FileKey, block_idx: u64) -> Result<Option<Bytes>> {
        let hash = hash_key(&key.remote_path);

        // The server rewrote this file under a new etag: every block held
        // under the old one belongs to different content and must never be
        // served, so drop the whole entry before even checking `block_idx`.
        if self.entries.get(&hash).is_some_and(|e| e.etag != key.etag) {
            self.remove_entry(&hash)?;
        }

        let Some(entry) = self.entries.get(&hash) else {
            return Ok(None);
        };
        if !entry.blocks.contains(&block_idx) {
            return Ok(None);
        }

        let max_idx = *entry.blocks.iter().max().expect("checked non-empty above");
        let data_path = self.data_path(&hash);
        let mut file = File::open(&data_path)
            .with_context(|| format!("failed to open {}", data_path.display()))?;
        let file_len = file.metadata()?.len();
        let offset = block_idx * BLOCK_SIZE;
        // Only the highest-indexed block of a file can be short (the
        // file's final, partial block); every other block was always
        // written with a full BLOCK_SIZE payload, so its length never
        // needs to be measured off the file.
        let len = if block_idx == max_idx {
            file_len.saturating_sub(offset).min(BLOCK_SIZE)
        } else {
            BLOCK_SIZE
        };
        let mut buf = vec![0u8; len as usize];
        file.seek(SeekFrom::Start(offset))?;
        match file.read_exact(&mut buf) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                // A crash left meta.json claiming this block is present
                // (at its full expected length) while the data file on
                // disk was never fully flushed to match. Per spec, cache
                // corruption is a miss, not an error: self-heal by
                // dropping the lying index so nothing keeps claiming a
                // block that isn't really there, and let the caller
                // re-download and rewrite it like any other miss.
                self.drop_block(&hash, block_idx)?;
                return Ok(None);
            }
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("failed to read {}", data_path.display()));
            }
        }

        self.touch(&hash);
        let entry = self.entries.get(&hash).expect("just touched");
        self.write_meta(&hash, entry)?;

        Ok(Some(Bytes::from(buf)))
    }

    pub fn write_block(&mut self, key: &FileKey, block_idx: u64, data: &[u8]) -> Result<()> {
        let hash = hash_key(&key.remote_path);

        if self.entries.get(&hash).is_some_and(|e| e.etag != key.etag) {
            self.remove_entry(&hash)?;
        }

        let dir = self.entry_dir(&hash);
        fs::create_dir_all(&dir)
            .with_context(|| format!("failed to create cache entry dir {}", dir.display()))?;
        let data_path = self.data_path(&hash);
        // No `truncate`: writing block N must never erase blocks the file
        // already has cached. Seeking past the current end (a fresh entry,
        // or a block index beyond what has been written so far) is exactly
        // what makes `data` a sparse file.
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&data_path)
            .with_context(|| format!("failed to open {}", data_path.display()))?;
        file.seek(SeekFrom::Start(block_idx * BLOCK_SIZE))?;
        file.write_all(data)?;
        drop(file);

        let seq = self.next_seq;
        self.next_seq += 1;
        let entry = self.entries.entry(hash.clone()).or_insert_with(|| Entry {
            etag: key.etag.clone(),
            blocks: BTreeSet::new(),
            pinned: false,
            last_used_unix: 0,
            seq: 0,
        });
        entry.etag = key.etag.clone();
        entry.blocks.insert(block_idx);
        entry.last_used_unix = now_unix();
        entry.seq = seq;

        let entry = self.entries.get(&hash).expect("just inserted/updated above");
        self.write_meta(&hash, entry)?;

        self.evict_if_over_budget()?;
        Ok(())
    }

    /// Files currently open must never be evicted; phase 2 adds drafts here.
    pub fn pin(&mut self, key: &FileKey) {
        let hash = hash_key(&key.remote_path);
        if let Some(entry) = self.entries.get_mut(&hash) {
            if entry.etag == key.etag {
                entry.pinned = true;
            }
        }
    }

    pub fn unpin(&mut self, key: &FileKey) {
        let hash = hash_key(&key.remote_path);
        if let Some(entry) = self.entries.get_mut(&hash) {
            if entry.etag == key.etag {
                entry.pinned = false;
            }
        }
    }

    /// Sum of actual stored block bytes across every entry — NOT
    /// `blocks.len() * BLOCK_SIZE`. A file's last block is very often
    /// shorter than BLOCK_SIZE (any file whose size isn't an exact
    /// multiple of it), and counting it as a full block would both lie
    /// about disk usage and make `max_bytes` stricter than the bytes it
    /// actually names. Cost: one `stat()` per entry, since the short
    /// length is derived from the real file size rather than tracked
    /// separately — cheap at the entry counts this cache deals with, and
    /// it avoids a second piece of state that could drift from disk.
    pub fn used_bytes(&self) -> u64 {
        self.entries.iter().map(|(hash, entry)| self.entry_bytes_on_disk(hash, entry)).sum()
    }

    /// Bumps an entry's recency without touching disk. Split out of
    /// `read_block` only so the borrow of `self.entries` needed to update
    /// it ends before the caller re-borrows to read the entry back for
    /// `write_meta`.
    fn touch(&mut self, hash: &str) {
        let seq = self.next_seq;
        self.next_seq += 1;
        if let Some(entry) = self.entries.get_mut(hash) {
            entry.last_used_unix = now_unix();
            entry.seq = seq;
        }
    }

    /// Deletes unpinned entries, oldest-`seq` (least recently used) first,
    /// until total usage fits `max_bytes` again. Stops rather than errors
    /// if everything left is pinned and still over budget: pinning is a
    /// hard guarantee to the caller, so this cache trades staying over
    /// budget for never evicting a file someone has open.
    fn evict_if_over_budget(&mut self) -> Result<()> {
        while self.used_bytes() > self.max_bytes {
            let victim = self
                .entries
                .iter()
                .filter(|(_, entry)| !entry.pinned)
                .min_by_key(|(_, entry)| entry.seq)
                .map(|(hash, _)| hash.clone());
            match victim {
                Some(hash) => self.remove_entry(&hash)?,
                None => break,
            }
        }
        Ok(())
    }

    fn entry_bytes_on_disk(&self, hash: &str, entry: &Entry) -> u64 {
        let Some(&max_idx) = entry.blocks.iter().max() else {
            return 0;
        };
        // Every block below the highest index is always a full BLOCK_SIZE
        // (see the comment in `read_block`); only the highest one can be
        // short, and its real length is read off the file on disk.
        let full_blocks = entry.blocks.len() as u64 - 1;
        let file_len = fs::metadata(self.data_path(hash)).map(|m| m.len()).unwrap_or(0);
        let last_len = file_len.saturating_sub(max_idx * BLOCK_SIZE).min(BLOCK_SIZE);
        full_blocks * BLOCK_SIZE + last_len
    }

    fn write_meta(&self, hash: &str, entry: &Entry) -> Result<()> {
        let meta = MetaFile {
            etag: entry.etag.clone(),
            blocks: entry.blocks.iter().copied().collect(),
            last_used_unix: entry.last_used_unix,
        };
        let json = serde_json::to_vec(&meta)?; // machine-only file, no need for pretty-printing
        let path = self.meta_path(hash);
        fs::write(&path, json).with_context(|| format!("failed to write {}", path.display()))
    }

    /// Drops one block index from an entry's in-memory set AND from its
    /// persisted meta.json, so a corrupt/truncated block never keeps being
    /// claimed as present across reads or after a restart. Leaves the rest
    /// of the entry (its other blocks, pin state) untouched — this is a
    /// per-block self-heal, not a whole-entry eviction.
    fn drop_block(&mut self, hash: &str, block_idx: u64) -> Result<()> {
        if let Some(entry) = self.entries.get_mut(hash) {
            entry.blocks.remove(&block_idx);
        }
        if let Some(entry) = self.entries.get(hash) {
            self.write_meta(hash, entry)?;
        }
        Ok(())
    }

    fn remove_entry(&mut self, hash: &str) -> Result<()> {
        self.entries.remove(hash);
        let dir = self.entry_dir(hash);
        if dir.exists() {
            fs::remove_dir_all(&dir)
                .with_context(|| format!("failed to remove cache entry {}", dir.display()))?;
        }
        Ok(())
    }

    fn entry_dir(&self, hash: &str) -> PathBuf {
        self.root.join(hash)
    }

    fn data_path(&self, hash: &str) -> PathBuf {
        self.entry_dir(hash).join("data")
    }

    fn meta_path(&self, hash: &str) -> PathBuf {
        self.entry_dir(hash).join("meta.json")
    }
}

/// Directory name for a remote path: first 16 hex chars (8 bytes) of
/// `sha256(remote_path)`. Truncated because this is a filesystem shard
/// key, not a security boundary — 64 bits of collision resistance is far
/// more than the number of files any one drive will ever cache.
fn hash_key(remote_path: &str) -> String {
    let digest = Sha256::digest(remote_path.as_bytes());
    digest[..8].iter().map(|b| format!("{b:02x}")).collect()
}

fn now_unix() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn key(path: &str, etag: &str) -> FileKey {
        FileKey { remote_path: path.into(), etag: etag.into() }
    }

    #[test]
    fn a_written_block_reads_back_identically() {
        let dir = TempDir::new().unwrap();
        let mut c = BlockCache::open(dir.path(), 100 * BLOCK_SIZE).unwrap();
        let payload = vec![7u8; BLOCK_SIZE as usize];
        c.write_block(&key("a", "e1"), 3, &payload).unwrap();
        assert_eq!(c.read_block(&key("a", "e1"), 3).unwrap().unwrap().as_ref(), &payload[..]);
        assert!(c.read_block(&key("a", "e1"), 2).unwrap().is_none(), "unwritten block");
    }

    #[test]
    fn a_new_etag_invalidates_every_cached_block_of_the_file() {
        let dir = TempDir::new().unwrap();
        let mut c = BlockCache::open(dir.path(), 100 * BLOCK_SIZE).unwrap();
        c.write_block(&key("a", "e1"), 0, &[1u8; 1024]).unwrap();
        assert!(
            c.read_block(&key("a", "e2"), 0).unwrap().is_none(),
            "the server rewrote the file: old bytes must not be served"
        );
    }

    #[test]
    fn cache_state_survives_reopening() {
        let dir = TempDir::new().unwrap();
        {
            let mut c = BlockCache::open(dir.path(), 100 * BLOCK_SIZE).unwrap();
            c.write_block(&key("a", "e1"), 0, &[9u8; 512]).unwrap();
        }
        let mut c = BlockCache::open(dir.path(), 100 * BLOCK_SIZE).unwrap();
        assert_eq!(c.read_block(&key("a", "e1"), 0).unwrap().unwrap().as_ref(), &[9u8; 512][..]);
    }

    #[test]
    fn a_block_truncated_by_a_crash_is_a_miss_not_an_error() {
        let dir = TempDir::new().unwrap();
        let mut c = BlockCache::open(dir.path(), 100 * BLOCK_SIZE).unwrap();
        let full_block = vec![5u8; BLOCK_SIZE as usize];
        c.write_block(&key("a", "e1"), 0, &full_block).unwrap();
        c.write_block(&key("a", "e1"), 1, &full_block).unwrap();

        // Simulate a crash mid-write: meta.json still lists block 0 as a
        // full BLOCK_SIZE block, but the data file on disk got truncated
        // before that block's bytes were ever fully flushed. Block 0 is
        // not the file's highest index (block 1 is), so it must always be
        // a full block — this truncation can only be corruption, not a
        // legitimate short final block.
        let hash = hash_key("a");
        let data_path = dir.path().join(&hash).join("data");
        let file = std::fs::OpenOptions::new().write(true).open(&data_path).unwrap();
        file.set_len(BLOCK_SIZE / 2).unwrap();
        drop(file);

        assert!(
            c.read_block(&key("a", "e1"), 0).unwrap().is_none(),
            "truncated block must be treated as a cache miss, not an I/O error"
        );

        // Self-healed: the lying block index must be dropped from
        // meta.json itself, not just papered over in memory, so nothing
        // keeps claiming a block that isn't really there.
        let meta_path = dir.path().join(&hash).join("meta.json");
        let meta: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&meta_path).unwrap()).unwrap();
        let blocks = meta["blocks"].as_array().unwrap();
        assert!(
            !blocks.iter().any(|b| b.as_u64() == Some(0)),
            "stale block index must be dropped from meta.json, got {blocks:?}"
        );

        // And the cache is usable again: re-downloading and rewriting the
        // block must round-trip normally.
        let fresh = vec![6u8; BLOCK_SIZE as usize];
        c.write_block(&key("a", "e1"), 0, &fresh).unwrap();
        assert_eq!(c.read_block(&key("a", "e1"), 0).unwrap().unwrap().as_ref(), &fresh[..]);
    }

    #[test]
    fn eviction_drops_the_least_recently_used_unpinned_file_first() {
        let dir = TempDir::new().unwrap();
        let mut c = BlockCache::open(dir.path(), 3 * BLOCK_SIZE).unwrap();
        let one_block = vec![1u8; BLOCK_SIZE as usize];
        c.write_block(&key("old", "e"), 0, &one_block).unwrap();
        c.write_block(&key("pinned", "e"), 0, &one_block).unwrap();
        c.pin(&key("pinned", "e"));
        c.write_block(&key("recent", "e"), 0, &one_block).unwrap();
        c.read_block(&key("old", "e"), 0).unwrap(); // old is now MRU
        c.write_block(&key("new", "e"), 0, &one_block).unwrap(); // must evict someone
        assert!(c.read_block(&key("old", "e"), 0).unwrap().is_some(), "recently used, kept");
        assert!(c.read_block(&key("pinned", "e"), 0).unwrap().is_some(), "pinned, kept");
        assert!(c.read_block(&key("recent", "e"), 0).unwrap().is_none(), "LRU victim");
    }
}
