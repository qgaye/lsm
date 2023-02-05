mod builder;
mod iterator;

pub use builder::BlockBuilder;
pub use iterator::BlockIterator;
use bytes::Bytes;

pub const SIZEOF_U16: usize = std::mem::size_of::<u16>();

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted
/// key-value pairs.
pub struct Block {
    pub data: Vec<u8>,
    pub offsets: Vec<u16>,
}

impl Block {
    pub fn encode(&self) -> Bytes {
        unimplemented!()
    }

    pub fn decode(data: &[u8]) -> Self {
        unimplemented!()
    }
}

