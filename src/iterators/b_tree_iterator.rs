#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod
use std::{
    ops::{Bound, RangeBounds},
    sync::Arc,
};

use crate::{
    buffer::buffer_pool_manager::BufferPoolManager,
    index::{
        node::Node,
        node_type::{Key, KeyValuePair},
    },
};

use super::{iterator::StorageIterator, leaf_node_iterator::LeafNodeIterator};

pub struct BTreeIterator {
    leaf_iter: LeafNodeIterator,

    end: Bound<Key>,
}

pub(self) fn map_end(
    bpm_instance: Arc<BufferPoolManager>,
    file_id: u64,
    page: Arc<Node>,
    range: impl RangeBounds<Key>,
) -> (LeafNodeIterator, Option<Bound<Key>>) {
    let leaf_iter = match range.start_bound() {
        std::ops::Bound::Excluded(k) => {
            let mut leaf_iter = LeafNodeIterator::create_and_seek_to_key(
                bpm_instance,
                file_id,
                page.clone(),
                k.clone(),
            );

            leaf_iter.next();

            leaf_iter
        }
        std::ops::Bound::Included(k) => {
            let leaf_iter = LeafNodeIterator::create_and_seek_to_key(
                bpm_instance,
                file_id,
                page.clone(),
                k.clone(),
            );

            leaf_iter
        }

        std::ops::Bound::Unbounded => {
            let leaf_iter =
                LeafNodeIterator::create_and_seek_to_first(bpm_instance, file_id, page.clone());
            leaf_iter
        }
    };

    let end = match range.end_bound() {
        std::ops::Bound::Excluded(k) => Some(Bound::Excluded(k.clone())),
        std::ops::Bound::Included(k) => Some(Bound::Included(k.clone())),

        std::ops::Bound::Unbounded => None,
    };

    (leaf_iter, end)
}

impl BTreeIterator {
    pub fn create_and_seek_to_key(
        bpm_instance: Arc<BufferPoolManager>,
        file_id: u64,
        page: Arc<Node>,
        range: impl RangeBounds<Key>,
    ) -> Self {
        let (leaf_iter, end) = map_end(bpm_instance, file_id, page.clone(), range);

        let mut iter = Self {
            leaf_iter,
            end: Bound::Unbounded,
        };

        if end.is_some() {
            iter.end = end.unwrap();
        };

        iter
    }

    
}

impl StorageIterator for BTreeIterator {
    type Item = KeyValuePair;
    fn value(&self) -> Self::Item {
        self.leaf_iter.value()
    }

    fn is_valid(&self) -> bool {
        self.leaf_iter.is_valid()
    }

    fn advance(&mut self) -> Result<(), ()> {
        self.leaf_iter.advance()
    }
}

impl Iterator for BTreeIterator {
    type Item = KeyValuePair;

    fn next(&mut self) -> Option<Self::Item> {
        let output: Option<Self::Item>;
        output = self.leaf_iter.next();

        if let Bound::Unbounded = self.end {
            return output;
        } else {
            if output.is_none() {
                return output;
            }

            let output_ref = output.unwrap();
            match &self.end {
                Bound::Excluded(key) => {
                    if Key(output_ref.key) >= *key {
                        return None;
                    }
                }
                Bound::Included(key) => {
                    if Key(output_ref.key) > *key {
                        return None;
                    }
                }

                _ => {}
            }
        }

        output
    }
}
