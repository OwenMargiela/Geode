#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{ ops::RangeBounds, sync::Arc };

use crate::index::tree::{
    byte_box::ByteBox,
    index_types::{ KeyValuePair, NodeKey },
    iterators::bptree_iterator::BPTreeIterator,
};

use super::btree_obj::BPTree;

impl BPTree {
    pub fn get(&self, key: ByteBox) -> anyhow::Result<KeyValuePair> {
        let search_key = NodeKey::GuidePost(key);
        unimplemented!()
    }

    pub fn put(&self, kv: KeyValuePair) -> anyhow::Result<()> {
        unimplemented!()
    }

    pub fn set(&self, kv: KeyValuePair) -> anyhow::Result<()> {
        unimplemented!()
    }

    pub fn remove(&self, key: ByteBox) -> anyhow::Result<KeyValuePair> {
        let search_key = NodeKey::GuidePost(key);
        unimplemented!()
    }

    pub fn scan(&self, range: impl RangeBounds<ByteBox>) -> BPTreeIterator {
        let lower_bound_page = match range.start_bound() {
            std::ops::Bound::Excluded(key) | std::ops::Bound::Included(key) => {
                let page = self.find_node(NodeKey::GuidePost(key.clone())).unwrap();
                page
            }
            std::ops::Bound::Unbounded => {
                let page = self.find_min().unwrap();
                page
            }
        };

        let leaf_iter = BPTreeIterator::create_and_seek_to_key(
            self.flusher.clone(),
            self.codec.clone(),
            self.index_id,
            Arc::new(lower_bound_page),
            range
        );

        leaf_iter
    }
}
