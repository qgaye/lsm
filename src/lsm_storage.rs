use std::collections::hash_map::Keys;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use parking_lot::{Mutex, RwLock};

use crate::block::Block;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::StorageIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::mem_table::{map_bound, MemTable};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

#[derive(Clone)]
pub struct LsmStorageInner {
    /// The current memtable.
    memtable: Arc<MemTable>,
    /// Immutable memTables, from earliest to latest.
    imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SsTables, from earliest to latest.
    l0_sstables: Vec<Arc<SsTable>>,
    /// L1 - L6 SsTables, sorted by key range.
    levels: Vec<Vec<Arc<SsTable>>>,
    /// The next SSTable ID.
    next_sst_id: usize,
}

impl LsmStorageInner {
    fn create() -> Self {
        Self {
            memtable: Arc::new(MemTable::create()),
            imm_memtables: vec![],
            l0_sstables: vec![],
            levels: vec![],
            next_sst_id: 1,
        }
    }
}

/// The storage interface of the LSM tree.
pub struct LsmStorage {
    // use RwLock instead Mutex, because just write operate need mutex
    inner: Arc<RwLock<Arc<LsmStorageInner>>>,
    flush_lock: Mutex<()>,
    path: PathBuf,
    block_cache: Arc<BlockCache>,
}

impl LsmStorage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(RwLock::new(Arc::new(LsmStorageInner::create()))),
            flush_lock: Mutex::new(()),
            path: path.as_ref().to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1 << 20)), // 4GB block cache
        })
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let snapshot = {
            let guard = self.inner.read();
            Arc::clone(&guard)
        }; // drop global lock here

        // Search on the current memtable.
        if let Some(value) = snapshot.memtable.get(key) {
            if value.is_empty() {
                // found tomestone, return key not exists
                return Ok(None);
            }
            return Ok(Some(value));
        }

        // Search on immutable memtables.
        // imm_memtables is from earliest to latest, so need reverse
        for memtable in snapshot.imm_memtables.iter().rev() {
            if let Some(value) = memtable.get(key) {
                if value.is_empty() {
                    // found tomestone, return key not exists
                    return Ok(None);
                }
                return Ok(Some(value));
            }
        }

        // Search on ssTables
        let mut iters = Vec::new();
        for sstable in snapshot.l0_sstables.iter().rev() {
            let iter = SsTableIterator::create_and_seek_to_key(sstable.clone(), key)?;
            iters.push(Box::new(iter));
        }
        let merge_iter = MergeIterator::create(iters);
        if merge_iter.is_valid() {
            return Ok(Some(Bytes::copy_from_slice(merge_iter.value())));
        }

        Ok(None)
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        assert!(!value.is_empty(), "value cannot be empty");
        assert!(!key.is_empty(), "key cannot be empty");

        // the skipList in MemTable is concurrency safe, so read lock here is enough
        // and change memTable to imm_memtables use write lock, ensure that no put() called in sync()
        let guard = self.inner.read();
        guard.memtable.put(key, value);

        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        assert!(!key.is_empty(), "key cannot be empty");

        let guard = self.inner.read();
        guard.memtable.put(key, b"");
        Ok(())
    }

    /// Persist data to disk.
    ///
    /// In day 3: flush the current memtable to disk as L0 SST.
    /// In day 6: call `fsync` on WAL.
    pub fn sync(&self) -> Result<()> {
        let _flush_lock = self.flush_lock.lock();
        let flush_memtable;
        let sst_id;

        // Move mutable memtable to immutable memtables.
        {
            let mut guard = self.inner.write();
            let mut snapshot = guard.as_ref().clone();
            // Swap the current memtable with a new one.
            let memtable = std::mem::replace(&mut snapshot.memtable, Arc::new(MemTable::create()));
            flush_memtable = memtable.clone();
            sst_id = snapshot.next_sst_id;
            // Add the memtable to the immutable memtables.
            snapshot.imm_memtables.push(memtable);
            // Update the snapshot.
            *guard = Arc::new(snapshot);
        }

        // At this point, the old memtable should be disabled for write, and all write threads
        // should be operating on the new memtable. We can safely flush the old memtable to
        // disk.
        let mut builder = SsTableBuilder::new(4096);
        flush_memtable.flush(&mut builder)?;
        let sst = Arc::new(builder.build(
            sst_id,
            Some(self.block_cache.clone()),
            self.path_of_sst(sst_id),
        )?);

        // Add the flushed L0 table to the list.
        {
            let mut guard = self.inner.write();
            let mut snapshot = guard.as_ref().clone();
            // Remove the memtable from the immutable memtables.
            snapshot.imm_memtables.pop();
            // Add L0 table
            snapshot.l0_sstables.push(sst);
            // Update SST ID
            snapshot.next_sst_id += 1;
            // Update the snapshot.
            *guard = Arc::new(snapshot);
        }

        Ok(())
    }

    /// Create an iterators over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let guard = self.inner.read();
            Arc::clone(&guard)
        }; // drop global lock here

        // scan in MemTables
        let mut memtable_iters = Vec::new();
        memtable_iters.push(Box::new(snapshot.memtable.scan(lower, upper)));
        // imm_memtables is earliest to latest, merge operate need latest first, so when do merge need reverse
        for memtable in snapshot.imm_memtables.iter().rev() {
            memtable_iters.push(Box::new(memtable.scan(lower, upper)));
        }
        let memtable_merge_iter = MergeIterator::create(memtable_iters);

        // Scan in SsTables
        let mut table_iters = Vec::new();
        for ssTable in snapshot.l0_sstables.iter().rev() {
            let iter = match lower {
                Bound::Included(key) => {
                    SsTableIterator::create_and_seek_to_key(ssTable.clone(), key)?
                },
                Bound::Excluded(key) => {
                    let mut iter = SsTableIterator::create_and_seek_to_key(ssTable.clone(), key)?;
                    if iter.is_valid() && iter.key() == key {
                        iter.next()?;
                    }
                    iter
                },
                Bound::Unbounded => {
                    SsTableIterator::create_and_seek_to_first(ssTable.clone())?
                }
            };
            table_iters.push(Box::new(iter));
        }
        let table_merge_iter = MergeIterator::create(table_iters);

        let iter = TwoMergeIterator::create(memtable_merge_iter, table_merge_iter)?;

        Ok(FusedIterator::new(LsmIterator::new(iter, map_bound(upper))?))
    }

    fn path_of_sst(&self, id: usize) -> PathBuf {
        self.path.join(format!("{:05}.sst", id))
    }

}
