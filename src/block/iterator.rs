use std::cmp::Ordering;
use std::sync::Arc;
use bytes::Buf;
use crate::block::Block;

/// Iterates on a block.
pub struct BlockIterator {
    block: Arc<Block>,
    key: Vec<u8>,
    value: Vec<u8>,
    idx: usize,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: Vec::new(),
            value: Vec::new(),
            idx: 0,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: &[u8]) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    /// Returns true if the iterator is valid.
    pub fn is_valid(&self) -> bool {
        self.idx < self.block.offsets.len() && !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to_idx(0);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx += 1;
        self.seek_to_idx(self.idx);
    }

    /// Seek to the first key that >= `key`.
    pub fn seek_to_key(&mut self, key: &[u8]) {
        let mut low = 0;
        let mut high = self.block.offsets.len();
        while low < high {
            let mid = low + (high - low) / 2;
            self.seek_to_idx(mid);
            match self.key().cmp(key) {
                Ordering::Greater => high = mid,
                Ordering::Less => low = mid + 1,
                Ordering::Equal => return,
            }
        }
        self.seek_to_idx(low);
    }

    fn seek_to_idx(&mut self, idx: usize) {
        if idx >= self.block.offsets.len() {
            self.key.clear();
            self.value.clear();
        } else {
            self.seek_to_offset(self.block.offsets[idx] as usize);
            self.idx = idx;
        }
    }

    fn seek_to_offset(&mut self, offset: usize) {
        let key_len_bytes = &self.block.data[offset..(offset + 2)];
        let key_len = (((key_len_bytes[0] as u16) << 8) | key_len_bytes[1] as u16) as usize; // [u8,2] -> u16
        let key = &self.block.data[(offset + 2)..(offset + 2 + key_len)];
        let value_len_bytes = &self.block.data[(offset + 2 + key_len)..(offset + 2 + key_len + 2)];
        let value_len = (((value_len_bytes[0] as u16) << 8) | value_len_bytes[1] as u16) as usize;
        let value = &self.block.data[(offset + 2 + key_len + 2)..(offset + 2 + key_len + 2 + value_len)];
        self.key = Vec::from(key);
        self.value = Vec::from(value);
    }

}

