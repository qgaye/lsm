use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use anyhow::Result;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.1.key().cmp(other.1.key()) {
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
        }
            .map(|x| x.reverse())
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(mut iters: Vec<Box<I>>) -> Self {
        let mut heap = BinaryHeap::new();

        if iters.is_empty() {
            return Self {
                iters: heap,
                current: None,
            };
        }

        if iters.iter().all(|x| !x.is_valid()) {
            // all invalid, select last one
            return Self {
                iters: heap,
                current: Some(HeapWrapper(0, iters.pop().unwrap())),
            };
        }

        for (idx, iter) in iters.into_iter().enumerate() {
            // here push iters.size HeapWrapper to heap
            // [iter1, iter2, iter3] => Heap((0, iter1), (1, iter2), (2, iter3))
            if iter.is_valid() {
                heap.push(HeapWrapper(idx, iter));
            }
        }

        let current = heap.pop();
        Self {
            iters: heap,
            current,
        }
    }
}

impl<I: StorageIterator> StorageIterator for MergeIterator<I> {
    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn key(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.key()
    }

    fn is_valid(&self) -> bool {
        self.current.as_ref()
            .map(|x| x.1.is_valid())
            .unwrap_or(false)
    }

    // iters: [
    //   (
    //     ("a", "1"), ("b", "2")
    //   ), // iter1
    //   (
    //     ("b", "3"), ("c", "4")
    //   )  // iter2
    // ]
    // heap: [(0, iter1(key: "a")), (1, iter2(key: "b"))]
    // ========== key: "a", value: "1" ===========
    // heap: [(1, iter2(key: "b"))]
    // current: iter1(key: "a")
    // ========== key: "b", value: "2" ===========
    // heap: [(1, iter2(key: "b"))]
    // current: iter1(key: "b")
    // ========== key: "c", value: "4" ===========
    // heap: []
    // current: iter2(key: "c")

    // 1、堆的排序规则：key小的优先，同样key，所在iter小的优先
    // 2、next：如果堆顶的iter和current的key相同，则堆顶iter.next，如果iter.next到底了则pop出堆
    // 3、current.next
    // 4、如果current无效，则从堆pop置为新current，返回
    // 5、反之，则和堆顶比较，如果堆顶的小于current（key&iter顺序），则置换current（pop&push）
    fn next(&mut self) -> Result<()> {
        let current = self.current.as_mut().unwrap();
        // Pop the item out of the heap if they have the same value.
        while let Some(mut inner_iter) = self.iters.peek_mut() {
            if inner_iter.1.key() == current.1.key() {
                // Case 1: an error occurred when calling `next`.
                if let e @ Err(_) = inner_iter.1.next() {
                    PeekMut::pop(inner_iter);
                    return e;
                }
                if !inner_iter.1.is_valid() {
                    PeekMut::pop(inner_iter);
                }
            } else {
                break;
            }
        }
        // current's key & value has used, so need go next
        current.1.next()?;

        // If the current iterator is invalid, pop it out of the heap and select the next one.
        if !current.1.is_valid() {
            if let Some(iter) = self.iters.pop() {
                self.current = Some(iter);
            }
            return Ok(());
        }

        // Otherwise, compare with heap top and swap if necessary.
        if let Some(mut iter) = self.iters.peek_mut() {
            // current key & idx smaller than iter, swap
            // the if condition need reverse, because heap is MaxHeap
            if !(*current >= *iter) {
                // heap.pop(); then heap.push(current);
                std::mem::swap(current, &mut *iter);
            }
        }

        Ok(())
    }
}
