use std::process::id;
use std::sync::Arc;

use anyhow::{Error, Result};
use bytes::Buf;
use crate::block::{Block, BlockIterator};

use super::SsTable;
use crate::iterators::StorageIterator;

/// An iterators over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    block_idx: usize,
    block_iter: BlockIterator,
}

impl SsTableIterator {
    /// Create a new iterators and seek to the first key-value pair.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let (block_idx, block_iter) = Self::seek_to_first_inner(&table)?;
        let mut iter = Self {
            table,
            block_idx,
            block_iter,
        };
        Ok(iter)
    }

    /// Seek to the first key-value pair.
    pub fn seek_to_first(&mut self) -> Result<()> {
        let (block_idx, block_iter) = Self::seek_to_first_inner(&self.table)?;
        self.block_idx = block_idx;
        self.block_iter = block_iter;
        Ok(())
    }

    /// Create a new iterators and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: &[u8]) -> Result<Self> {
        let (block_idx, block_iter) = Self::seek_to_key_inner(&table, key)?;
        Ok(Self {
            table,
            block_idx,
            block_iter,
        })
    }

    /// Seek to the first key-value pair which >= `key`.
    pub fn seek_to_key(&mut self, key: &[u8]) -> Result<()> {
        let (block_idx, block_iter) = Self::seek_to_key_inner(&self.table, key)?;
        self.block_idx = block_idx;
        self.block_iter = block_iter;
        Ok(())
    }

    fn seek_to_first_inner(table: &Arc<SsTable>) -> Result<(usize, BlockIterator)> {
        Ok(
            (0, BlockIterator::create_and_seek_to_first(table.read_block_cached(0)?))
        )
    }

    fn seek_to_key_inner(table: &Arc<SsTable>, key: &[u8]) -> Result<(usize, BlockIterator)> {
        let mut block_idx = table.find_block_idx(key);
        let mut block_iter = BlockIterator::create_and_seek_to_key(table.read_block_cached(block_idx)?, key);
        // not find key in block[idx], return block[idx + 1] first key
        if !block_iter.is_valid() && block_idx + 1 < table.num_of_blocks() {
            block_idx += 1;
            block_iter = BlockIterator::create_and_seek_to_first(table.read_block_cached(block_idx)?);
        }
        Ok((block_idx, block_iter))
    }

}

impl StorageIterator for SsTableIterator {
    fn value(&self) -> &[u8] {
        self.block_iter.value()
    }

    fn key(&self) -> &[u8] {
        self.block_iter.key()
    }

    fn is_valid(&self) -> bool {
        self.block_iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.block_iter.next();
        if !self.block_iter.is_valid() && self.block_idx + 1 < self.table.num_of_blocks() {
            self.block_idx += 1;
            self.block_iter = BlockIterator::create_and_seek_to_first(self.table.read_block_cached(self.block_idx)?);
        }
        Ok(())
    }
}
