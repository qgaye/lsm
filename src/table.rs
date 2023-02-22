mod builder;
mod iterator;

use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Result};
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::lsm_storage::BlockCache;
use crate::utils::{SIZEOF_U16, SIZEOF_USIZE, two_u8_to_u16};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: Bytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    pub fn encode_block_meta(
        block_meta: &[BlockMeta],
        buf: &mut Vec<u8>,
    ) {
        let mut estimated_size = 0;
        for meta in block_meta {
            estimated_size += SIZEOF_USIZE; // offset
            estimated_size += SIZEOF_U16; // first_key_len
            estimated_size += meta.first_key.len();
        }
        buf.reserve(estimated_size);
        for meta in block_meta {
            buf.put_u32(meta.offset as u32);
            buf.put_u16(meta.first_key.len() as u16);
            buf.put_slice(&meta.first_key);
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let mut metas = Vec::new();
        while buf.has_remaining() {
            let offset = buf.get_u32() as usize;
            let first_key_len = buf.get_u16() as usize;
            let first_key = buf.copy_to_bytes(first_key_len);
            metas.push(BlockMeta {
                offset,
                first_key
            })
        }
        metas
    }
}

/// A file object.
pub struct FileObject(Bytes, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        Ok(self.0[offset as usize..(offset + len) as usize].to_vec())
    }

    pub fn size(&self) -> u64 {
        self.0.len() as u64
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        let len = data.len() as u64;
        let object = FileObject(Bytes::from(data), len);
        Ok(object)
    }

    pub fn open(path: &Path) -> Result<Self> {
        unimplemented!()
    }
}

pub struct SsTable {
    pub file: FileObject,
    pub block_metas: Vec<BlockMeta>,
    pub block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
}

impl SsTable {
    // #[cfg(test)]
    pub fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let len = file.size() as usize;
        let mut offset_bytes = file.read((len - SIZEOF_USIZE) as u64, SIZEOF_USIZE as u64)?;
        let block_meta_offset = (&offset_bytes[..]).get_u32() as usize;
        let meta_bytes = file.read(block_meta_offset as u64, (len - SIZEOF_USIZE - block_meta_offset) as u64)?;
        Ok(Self {
            file,
            block_metas: BlockMeta::decode_block_meta(&meta_bytes[..]),
            block_meta_offset,
            id,
            block_cache,
        })
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let start_offset = self.block_metas[block_idx].offset;
        let end_offset = if block_idx + 1 == self.block_metas.len() {
            self.block_meta_offset
        } else {
            self.block_metas[block_idx + 1].offset
        };
        let block_data = self.file.read(start_offset as u64, (end_offset - start_offset) as u64)?;
        Ok(Arc::new(Block::decode(&block_data[..])))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if let Some(block_cache) = &self.block_cache {
            let block = block_cache
                .try_get_with((self.id, block_idx), || self.read_block(block_idx))
                .map_err(|e| anyhow!("{}", e))?;
            Ok(block)
        } else {
            self.read_block(block_idx)
        }
    }

    /// Find the block that may contain `key`.
    pub fn find_block_idx(&self, key: &[u8]) -> usize {
        let i = self.block_metas.partition_point(|meta| meta.first_key <= key);
        if i == 0 {
            i
        } else {
            i - 1
        }
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_metas.len()
    }
}
