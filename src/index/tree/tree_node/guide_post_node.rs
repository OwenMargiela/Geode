#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use crate::index::tree::{ byte_box::ByteBox, index_types::NodeKey };

use super::{ node_type::{ NodeType, PagePointer }, tree_node_inner::{ GuidePostNode, NodeInner } };

impl GuidePostNode {
    /// Removes a key from an internal node
    pub fn remove_key(&mut self, search: NodeKey) -> anyhow::Result<()> {
        let (search_data, discard) = NodeInner::deconstruct_value(&search);

        if discard.is_some() {
            return Err(
                anyhow::Error::msg(
                    "Invalid Node Key Type.Kv node where Guidepost should be present"
                )
            );
        }
        let node_type = self.node_type.clone();
        match self.node_type {
            NodeType::Internal(_, ref mut keys, _) => {
                let idx = NodeInner::binary_search_in_key(&search, &keys)?;
                keys.remove(idx);
                Ok(())
            }

            _ => {
                return Err(anyhow::Error::msg("Unexpected Error"));
            }
        }
    }

    /// Inserts a key into an internal node
    pub fn insert_key(&mut self, key: NodeKey) -> anyhow::Result<()> {
        let node_type = self.node_type.clone();
        match self.node_type {
            NodeType::Internal(_, ref mut keys, _) => {
                let idx = NodeInner::binary_search_in_key(&key, &keys)?;

                keys.insert(idx, key);
                Ok(())
            }

            _ => Err(anyhow::Error::msg("Unexpected Error")),
        }
    }

    /// Removes key and child pointers from internal nodes
    ///
    /// The take right bool parameter controls wether to take the child pointer to the keys left or right
    pub fn remove_inner_node_entry(
        &mut self,
        idx: usize,
        take_right: bool
    ) -> anyhow::Result<(NodeKey, PagePointer)> {
        match self.node_type {
            NodeType::Internal(ref mut children, ref mut keys, _) => {
                let key = keys.remove(idx);
                let child_pointer: PagePointer;
                if take_right {
                    child_pointer = children.remove(idx + 1);
                } else {
                    child_pointer = children.remove(idx);
                }

                Ok((key, child_pointer))
            }

            _ => Err(anyhow::Error::msg("Unexpected Error")),
        }
    }

    /// Inserts a child pointer into an internal node
    pub fn insert_child_pointer(&mut self, child: PagePointer) -> anyhow::Result<()> {
        let node_type = self.node_type.clone();
        match self.node_type {
            NodeType::Internal(ref mut children, _, _) => {
                let idx = children.binary_search(&child).unwrap_or_else(|x| x);

                children.insert(idx, child);
                Ok(())
            }

            _ => Err(anyhow::Error::msg("Unexpected Error")),
        }
    }

    pub fn remove_sibling_node(&mut self, key: NodeKey, child: PagePointer) -> anyhow::Result<()> {
        match self.node_type {
            NodeType::Internal(_, _, _) => {
                self.remove_key(key)?;
                self.remove_child_pointer(child)?;

                Ok(())
            }
            _ => Err(anyhow::Error::msg("Unexpected Error")),
        }
    }
    /// Removes a key from an internal node
    pub fn remove_child_pointer(&mut self, child: PagePointer) -> anyhow::Result<()> {
        let node_type = self.node_type.clone();
        match self.node_type {
            NodeType::Internal(ref mut children, _, _) => {
                let idx = children.binary_search(&child).unwrap_or_else(|x| x);
                let dat = children.remove(idx);

                println!("Removed this {}", dat);

                Ok(())
            }

            _ => {
                return Err(anyhow::Error::msg("Unexpected Error"));
            }
        }
    }

    /// Inserts a key and pointer into an internal node
    pub fn insert_sibling_node(&mut self, key: NodeKey, child: PagePointer) -> anyhow::Result<()> {
        match self.node_type {
            NodeType::Internal(_, _, _) => {
                self.insert_key(key)?;

                self.insert_child_pointer(child)?;

                Ok(())
            }
            _ => Err(anyhow::Error::msg("Unexpected Error")),
        }
    }

    pub fn get_key_at(&self, idx: usize) -> anyhow::Result<ByteBox> {
        match self.node_type {
            NodeType::Internal(_, ref keys, _) => {
                if let NodeKey::GuidePost(k) = NodeInner::get_key_at_index(idx, &keys)? {
                    return Ok(k);
                }
            }
            _ => {}
        }

        return Err(anyhow::Error::msg("Unexpected Error"));
    }

    pub fn get_key(&self, search: NodeKey) -> anyhow::Result<ByteBox> {
        let node_type = self.node_type.clone();
        match self.node_type {
            NodeType::Internal(_, ref keys, _) => {
                let idx = NodeInner::find_key(search, &keys)?;

                if let NodeKey::GuidePost(k) = NodeInner::get_key_at_index(idx, &keys)? {
                    return Ok(k);
                }
            }
            _ => {}
        }

        return Err(anyhow::Error::msg("Unexpected Error"));
    }

    pub fn get_key_idx(&self, search: NodeKey) -> anyhow::Result<usize> {
        let node_type = self.node_type.clone();
        match self.node_type {
            NodeType::Internal(_, ref keys, _) => {
                let idx = NodeInner::find_key(search, &keys);

                return idx;
            }
            _ => {}
        }

        return Err(anyhow::Error::msg("Unexpected Error"));
    }

    pub fn child_ptr_len(&self) -> usize {
        match &self.node_type {
            NodeType::Internal(children, _, _) => {
                return children.len();
            }
            _ => {
                panic!("Fatal Error: Mismatch between node type and wrapper type");
            }
        }
    }
}
