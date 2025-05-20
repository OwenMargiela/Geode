#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

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
        self.flusher.aqquire_context_ex(context.clone())?;

        let leaf_node_page = &TreePage::new(self.flusher.read_top()?);

        let mut leaf_node = self.codec.decode(leaf_node_page)?;

        leaf_node.insert_entry(entry.clone())?;

        if leaf_node.get_key_array_length() < 2 * self.b {
            let leaf_page = Codec::encode(&leaf_node)?;
            self.flusher.pop_flush_test(leaf_page.get_data())?;
            self.flusher.release_ex()?;

            return Ok(());
        } else {
            self.propogate_upwards(leaf_node)?;
            self.flusher.release_ex()?;
            return Ok(());
        }
    }

    pub fn delete(&self, search: NodeKey) -> anyhow::Result<()> {
        unimplemented!()
    }

    pub fn get_entry(&self, search: NodeKey) -> anyhow::Result<NodeKey> {
        unimplemented!()
    }
}
