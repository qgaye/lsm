use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::{BufMut, Bytes};
use crate::block::{Block, BlockBuilder, BlockIterator};

use super::{BlockMeta, SsTable};
use crate::lsm_storage::BlockCache;
use crate::table::FileObject;

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    pub meta: Vec<BlockMeta>,
    pub data: Vec<u8>,
    block_builder: BlockBuilder,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            meta: Vec::new(),
            data: Vec::new(),
            block_builder: BlockBuilder::new(block_size),
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        if self.block_builder.is_empty() {
            self.meta.push(BlockMeta {
                offset: self.data.len(),
                first_key: Bytes::copy_from_slice(key),
            })
        }
        let r = self.block_builder.add(key, value);
        if !r {
            self.finish_block();
            self.add(key, value);
        }
    }

    fn finish_block(&mut self) {
        let block_builder = std::mem::replace(&mut self.block_builder, BlockBuilder::new(self.block_size));
        self.data.extend(block_builder.build().encode());
    }

    /// Get the estimated size of the SSTable.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. No need to actually write to disk until
    /// chapter 4 block cache.
    /// | block1 | ... | block99 | block meta | block meta offset |
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        self.finish_block();
        let block_meta_offset = self.data.len();
        let mut buf = self.data;
        BlockMeta::encode_block_meta(&self.meta, &mut buf);
        buf.put_u32(block_meta_offset as u32);
        let file = FileObject::create(path.as_ref(), buf)?;
        Ok(SsTable {
            file,
            block_metas: self.meta,
            block_meta_offset,
            id,
            block_cache,
        })
    }

    // #[cfg(test)]
    pub fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
