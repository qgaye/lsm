use bytes::Bytes;
use lsm::iterators::StorageIterator;
use anyhow::Result;

#[derive(Clone)]
pub struct MockIterator {
    pub data: Vec<(Bytes, Bytes)>,
    pub index: usize,
}

impl MockIterator {
    pub fn new(data: Vec<(Bytes, Bytes)>) -> Self {
        Self { data, index: 0 }
    }
}

impl StorageIterator for MockIterator {
    fn value(&self) -> &[u8] {
        self.data[self.index].1.as_ref()
    }

    fn key(&self) -> &[u8] {
        self.data[self.index].0.as_ref()
    }

    fn is_valid(&self) -> bool {
        self.index < self.data.len()
    }

    fn next(&mut self) -> Result<()> {
        if self.index < self.data.len() {
            self.index += 1;
        }
        Ok(())
    }
}