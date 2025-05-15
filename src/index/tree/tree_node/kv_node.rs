#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use crate::index::tree::{
    byte_box::ByteBox,
    index_types::{KeyValuePair, NodeKey},
   
};

use super::{node_type::NodeType, tree_node_inner::{KvNode, NodeInner}};

impl KvNode {
    /// Inserts a key value pair entry into a leaf node
    ///
    pub fn insert_entry(&mut self, new_entry: KeyValuePair) -> anyhow::Result<()> {
        let search_value = new_entry.key.clone();

        let new_pair = NodeKey::KeyValuePair(new_entry);

        match self.node_type {
            NodeType::Leaf(ref mut entries, _, _) => {
                let idx = entries
                    .binary_search_by(|entry| {
                        let (key, _) = NodeInner::deconstruct_value(entry);
                        key.cmp(&search_value)
                    })
                    .unwrap_or_else(|x| x);

                let entry = entries.get(idx).unwrap();

                if entry.to_kv_pair()? == new_pair.to_kv_pair()? {
                    return Err(anyhow::Error::msg("Entry already exists"));
                }

                entries.insert(idx, new_pair);

                Ok(())
            }

            _ => return Err(anyhow::Error::msg("Unexpected Error")),
        }
    }

    /// Removes a key value pair entry from a leaf node
    pub fn remove_entry(&mut self, key: &ByteBox) -> anyhow::Result<()> {
        let search_value = key;

        match self.node_type {
            NodeType::Leaf(ref mut entries, _, _) => {
                let idx = entries
                    .binary_search_by(|entry| {
                        let (key, _) = NodeInner::deconstruct_value(entry);
                        key.cmp(&search_value)
                    })
                    .unwrap_or_else(|x| x);

                entries.remove(idx);

                Ok(())
            }

            _ => return Err(anyhow::Error::msg("Unexpected Error")),
        }
    }

    /// Removes key-value pairs from leaf nodes
    pub fn remove_key_value_at_index(&mut self, idx: usize) -> anyhow::Result<NodeKey> {
        match self.node_type {
            NodeType::Leaf(ref mut entries, _, _) => {
                let entry = entries.remove(idx);

                Ok(entry)
            }

            _ => return Err(anyhow::Error::msg("Unexpected Error")),
        }
    }

    pub fn get_key_value_at(&self, idx: usize) -> anyhow::Result<KeyValuePair> {
        match self.node_type {
            NodeType::Leaf(ref entries, _, _) => {
                if let NodeKey::KeyValuePair(kv) = NodeInner::get_key_at_index(idx, &entries)? {
                    return Ok(kv);
                }
            }
            _ => {}
        }

        return Err(anyhow::Error::msg("Unexpected Error"));
    }

    pub fn get_key_value(&self, search: NodeKey) -> anyhow::Result<KeyValuePair> {
        let node_type = self.node_type.clone();
        match self.node_type {
            NodeType::Leaf(ref entries, _, _) => {
                let idx = NodeInner::find_key(node_type, search, entries)?;

                if let NodeKey::KeyValuePair(kv) = NodeInner::get_key_at_index(idx, &entries)? {
                    return Ok(kv);
                }
            }
            _ => {}
        }

        return Err(anyhow::Error::msg("Unexpected Error"));
    }

    pub fn get_key_value_index(&self, search: NodeKey) -> anyhow::Result<usize> {
        let node_type = self.node_type.clone();
        match self.node_type {
            NodeType::Leaf(ref entries, _, _) => {
                let idx = NodeInner::find_key(node_type, search, entries);
                return idx;
            }
            _ => {}
        }

        return Err(anyhow::Error::msg("Unexpected Error"));
    }
}
