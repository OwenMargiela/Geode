#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{ ops::RangeBounds, sync::Arc };

use anyhow::Ok;

use crate::{
    buffer::flusher::Lock,
    index::tree::{
        byte_box::ByteBox,
        db::btree_obj::WriteOperation,
        index_types::{ KeyValuePair, NodeKey },
        tree_page::{ codec::Codec, page::TreePage },
    },
};

use super::btree_obj::BPTree;

impl BPTree {
    pub fn insert(&self, entry: KeyValuePair) -> anyhow::Result<()> {
        let search_key = NodeKey::KeyValuePair(entry.clone());

        let context = self.tree_descent(search_key.clone(), Lock::EXLOCK, WriteOperation::Insert)?;
        self.flusher.aqquire_context_ex(context)?;

        let leaf_node_page = &TreePage::new(self.flusher.read_top()?);

        let mut leaf_node = self.codec.decode(leaf_node_page)?;

        if leaf_node.get_key_array_length() < self.b {
            println!("No split");

            leaf_node.insert_entry(entry.clone())?;

            let leaf_page = Codec::encode(&leaf_node)?;
            self.flusher.pop_flush_test(leaf_page.get_data())?;
        } else {
            self.flusher.release_ex()?;

            println!("Full: implement Split");
        }

        Ok(())
    }

    pub fn delete(&self, search: NodeKey) -> anyhow::Result<()> {
        unimplemented!()
    }

    pub fn get_entry(&self, search: NodeKey) -> anyhow::Result<NodeKey> {
        unimplemented!()
    }
}
