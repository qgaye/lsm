use std::ops::Bound;
use anyhow::Result;
use bytes::Bytes;
use crate::iterators::merge_iterator::MergeIterator;

use crate::iterators::StorageIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::mem_table::MemTableIterator;
use crate::table::SsTableIterator;

type LsmIteratorInner = TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>;

pub struct LsmIterator {
    iter: LsmIteratorInner,
    end_bound: Bound<Bytes>,
    is_valid: bool,
}

impl LsmIterator {
    pub fn new(iter: LsmIteratorInner, end_bound: Bound<Bytes>) -> Result<Self> {
        let mut iter = Self {
            is_valid: iter.is_valid(),
            iter,
            end_bound,
        };
        iter.move_to_non_delete()?;
        Ok(iter)
    }

    fn next_inner(&mut self) -> Result<()> {
        self.iter.next()?;
        if !self.iter.is_valid() {
            self.is_valid = false;
            return Ok(());
        }

        match &self.end_bound {
            Bound::Included(key) => self.is_valid = self.key() <= key,
            Bound::Excluded(key) => self.is_valid = self.key() < key,
            Bound::Unbounded => (),
        };

        Ok(())
    }

    fn move_to_non_delete(&mut self) -> Result<()> {
        while self.is_valid && self.value().is_empty() {
            self.next_inner()?;
        }
        Ok(())
    }
}

impl StorageIterator for LsmIterator {
    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn key(&self) -> &[u8] {
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        self.next_inner()?;
        self.move_to_non_delete()?;
        Ok(())
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self { iter }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn key(&self) -> &[u8] {
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.is_valid() {
            self.iter.next()?;
        }
        Ok(())
    }
}
