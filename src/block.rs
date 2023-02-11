mod builder;
mod iterator;

pub use builder::BlockBuilder;
pub use iterator::BlockIterator;
use bytes::{Buf, BufMut, Bytes};
use crate::utils::{SIZEOF_U16, two_u8_to_u16};

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted
/// key-value pairs.
pub struct Block {
    pub data: Vec<u8>,
    pub offsets: Vec<u16>,
}

impl Block {
    pub fn encode(&self) -> Bytes {
        let mut buf = self.data.clone();
        for offset in &self.offsets {
            buf.put_u16(*offset);
        }
        buf.put_u16(self.offsets.len() as u16);
        buf.into()
    }

    pub fn decode(buf: &[u8]) -> Self {
        let num_of_elements = two_u8_to_u16(&buf[(buf.len() - SIZEOF_U16)..]) as usize;
        let offsets_raw = &buf[(buf.len() - SIZEOF_U16 - num_of_elements * SIZEOF_U16)..(buf.len() - SIZEOF_U16)];
        let offsets = offsets_raw
            .chunks(SIZEOF_U16)
            .map(|s| two_u8_to_u16(s))
            .collect();
        Self {
            data: buf[..(buf.len() - SIZEOF_U16 - num_of_elements * SIZEOF_U16)].to_vec(),
            offsets,
        }
    }
}

