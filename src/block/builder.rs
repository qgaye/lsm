use bytes::BufMut;
use crate::block::{Block, SIZEOF_U16};

/// Builds a block.
pub struct BlockBuilder {
    occupy_size: usize,
    block_size: usize,
    data: Vec<u8>,
    offsets: Vec<u16>,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            occupy_size: 0,
            block_size,
            data: Vec::new(),
            offsets: Vec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> bool {
        assert!(!key.is_empty(), "key must not be empty");
        let entry_total_size = self.entry_size(key, value) + SIZEOF_U16; /* offset size */
        if self.occupy_size + entry_total_size > self.block_size - SIZEOF_U16 /* num_of_elements */
            && !self.is_empty() /* first key always can set */ {
            // println!("over block size, key: {:?}, block_size: {:?}", key, self.block_size);
            return false;
        }
        let offset = self.data.len();
        self.data.append(&mut self.entry_encode(key, value));
        self.offsets.push(u16::try_from(offset).unwrap());
        self.occupy_size += entry_total_size;
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }

    /// key & value -> entry
    /// `[key_len(2B), key, value_len(2B), value]`
    fn entry_encode(&self, key: &[u8], value: &[u8]) -> Vec<u8> {
        let mut arr = Vec::new();
        arr.put_u16(key.len() as u16);
        arr.put(key);
        arr.put_u16(value.len() as u16);
        arr.put(value);
        arr
    }

    /// entry size
    fn entry_size(&self, key: &[u8], value: &[u8]) -> usize {
        // key_len + key + value_len + value
        SIZEOF_U16 + key.len() + SIZEOF_U16 + value.len()
    }


}
