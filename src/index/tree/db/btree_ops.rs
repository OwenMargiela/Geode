#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::ops::RangeBounds;

use crate::index::tree::{
    byte_box::ByteBox,
    index_types::{KeyValuePair, NodeKey},
};

use super::btree_obj::BPTree;

impl<'a> BPTree<'a> {
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

    pub fn scan(&self, range: impl RangeBounds<ByteBox>) -> anyhow::Result<KeyValuePair> {
        unimplemented!()
    }
}
