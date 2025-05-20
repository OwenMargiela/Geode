#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{ ops::RangeBounds, sync::Arc };

use crate::index::tree::{ byte_box::ByteBox, index_types::{ KeyValuePair, NodeKey } };

use super::btree_obj::BPTree;

impl BPTree {
    pub fn insert(&self, entry: KeyValuePair) -> anyhow::Result<()> {
        unimplemented!()
    }

    pub fn delete(&self, search: NodeKey) -> anyhow::Result<()> {
        unimplemented!()
    }

    pub fn scan(&self, range: impl RangeBounds<KeyValuePair>) -> anyhow::Result<()> {
        unimplemented!()
    }

    pub fn get(&self, search: NodeKey) -> anyhow::Result<ByteBox> {
        unimplemented!()
    }

    pub fn get_entry(&self, search: NodeKey) -> anyhow::Result<NodeKey> {
        unimplemented!()
    }
}
