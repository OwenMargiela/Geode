#![allow(unused_variables)] 
#![allow(dead_code)] 
use std::{ ops::{ Bound, RangeBounds }, sync::Arc };

use crate::{
    buffer::flusher::Flusher,
    index::tree::{
        byte_box::ByteBox,
        index_types::{ KeyValuePair, NodeKey },
        tree_node::tree_node_inner::NodeInner,
        tree_page::codec::Codec,
    },
};

use super::{ iterator::StorageIterator, leaf_iterator::LeafIterator };

pub struct BPTreeIterator {
    leaf_iter: LeafIterator,

    end: Bound<ByteBox>,
}

pub(self) fn map_end(
    flusher: Arc<Flusher>,
    file_id: u64,
    page: Arc<NodeInner>,
    codec: Codec,
    range: impl RangeBounds<ByteBox>
) -> (LeafIterator, Option<Bound<ByteBox>>) {
    let leaf_iter = match range.start_bound() {
        std::ops::Bound::Excluded(k) => {
            let mut leaf_iter = LeafIterator::create_and_seek_to_key(
                flusher,
                page.clone(),
                codec,
                NodeKey::GuidePost(k.clone()),
                file_id
            );

            leaf_iter.next();

            leaf_iter
        }
        std::ops::Bound::Included(k) => {
            let leaf_iter = LeafIterator::create_and_seek_to_key(
                flusher,
                page.clone(),
                codec,
                NodeKey::GuidePost(k.clone()),
                file_id
            );

            leaf_iter
        }

        std::ops::Bound::Unbounded => {
            let leaf_iter = LeafIterator::create_and_seek_to_first(
                flusher,
                page.clone(),
                codec,
                file_id
            );
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

impl BPTreeIterator {
    pub fn create_and_seek_to_key(
        flusher: Arc<Flusher>,
        codec: Codec,
        file_id: u64,
        page: Arc<NodeInner>,
        range: impl RangeBounds<ByteBox>
    ) -> Self {
        let (leaf_iter, end) = map_end(flusher, file_id, page, codec, range);

        let mut iter = Self {
            leaf_iter,
            end: Bound::Unbounded,
        };

        if end.is_some() {
            iter.end = end.unwrap();
        }

        iter
    }
}

impl StorageIterator for BPTreeIterator {
    type Item = KeyValuePair;
    fn value(&self) -> Self::Item {
        self.leaf_iter.value()
    }

    fn is_valid(&self) -> bool {
        self.leaf_iter.is_valid()
    }

    fn advance(&mut self) -> anyhow::Result<()> {
        self.leaf_iter.advance()
    }
}

impl Iterator for BPTreeIterator {
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

            let output_ref = output.clone().unwrap();
            match &self.end {
                Bound::Excluded(key) => {
                    if output_ref.key >= *key {
                        return None;
                    }
                }
                Bound::Included(key) => {
                    if output_ref.key > *key {
                        return None;
                    }
                }

                _ => {}
            }
        }

        output
    }
}
