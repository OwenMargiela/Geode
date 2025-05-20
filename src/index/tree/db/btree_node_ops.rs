#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use crate::index::tree::{
    index_types::NodeKey,
    tree_node::{ node_type::{ NodeType, PagePointer }, tree_node_inner::NodeInner },
};

// Helper functions for btree node operations
impl NodeInner {
    ///
    /// The current implementation uses the b param as an arbiturary predicate by which splits and merges occur
    ///
    /// Future optimizations will account for estimated page size as well as minimum and maximum capacity
    pub fn split(&mut self, b: usize) -> anyhow::Result<(NodeKey, NodeInner)> {
        match self.node_type {
            NodeType::Internal(ref mut children, ref mut keys, _) => {
                // Populate siblings keys.
                let mut sibling_keys = keys.split_off(b);

                // Pop median key - to be added to the parent..
                let median_key = sibling_keys.remove(0);

                // Populate siblings children.

                let sibling_children = children.split_off(b + 1);

                self.is_root = false;

                let new_node = NodeInner::new(
                    NodeType::Internal(sibling_children, sibling_keys, u32::default()),
                    self.is_root,
                    self.pointer,
                    None
                );

                anyhow::Ok((median_key, new_node))
            }

            NodeType::Leaf(ref mut pairs, _, current_page) => {
                // Populate siblings pairs
                let sibling_pair = pairs.split_off(b);

                // copy median key.
                let median_pair = sibling_pair
                    .get(0)
                    .ok_or(anyhow::Error::msg("Unexpected Error"))?
                    .clone();
                self.is_root = false;

                let new_node = NodeInner::new(
                    NodeType::Leaf(sibling_pair, u32::default(), None),
                    self.is_root,
                    u32::default(),
                    None
                );

                let (key, _) = NodeInner::deconstruct_value(&median_pair);

                anyhow::Ok((NodeKey::GuidePost(key), new_node))
            }

            NodeType::Unexpected => {
                return Err(anyhow::Error::msg("Unexpected Error"));
            }
        }
    }

    /// Is this page sufficient large enough to borrow from
    ///
    pub fn can_borrow(&self, b_param: usize) -> bool {
        return self.get_key_array_length() >= b_param;

        // Future implementations will have the following predicate

        // return self.get_key_array_length() >= Page::GetMinCapacity;
    }

    pub fn borrow(
        &mut self,
        current_parent_node: &mut NodeInner,
        current_candidate: &mut NodeInner,
        is_left: bool,
        separator_key: NodeKey
    ) -> anyhow::Result<()> {
        let promotion_key: NodeKey;
        let is_leaf = current_parent_node.is_leaf;
        let node_type = &mut current_parent_node.node_type;

        // Guide Post Key array in the parent node
        let mut key_vec = &mut NodeInner::get_key_vec_mut(node_type)?;

        match self.node_type {
            NodeType::Leaf(_, _, _) => {
                if is_left {
                    // Remove the right most value
                    let popped = current_candidate.pop_back()?;

                    // Set promotion key
                    self.insert_entry(popped.pop_key.to_kv_pair().unwrap())?;
                    promotion_key = popped.promotion_key.clone();
                } else {
                    // Remove the left most value
                    let popped = current_candidate.pop_front()?;

                    // Set promotion key
                    self.insert_entry(popped.pop_key.to_kv_pair().unwrap())?;
                    promotion_key = popped.promotion_key.clone();
                }

                // Update the separator key

                NodeInner::find_and_update_key(separator_key, promotion_key, &mut key_vec)?;
                return anyhow::Ok(());
            }
            NodeType::Internal(_, _, _) => {
                if is_left {
                    // Remove the right most value
                    let popped = current_candidate.pop_back()?;

                    // Set promotion key
                    promotion_key = popped.promotion_key;
                    self.insert_child_pointer(popped.child_pointer.unwrap())?;
                    self.insert_key(separator_key.clone())?;
                } else {
                    let popped = current_candidate.pop_front()?;

                    // Remove the left most value

                    promotion_key = popped.promotion_key;
                    self.insert_child_pointer(popped.child_pointer.unwrap())?;
                    self.insert_key(separator_key.clone())?;
                }

                // Update the separator key
                NodeInner::find_and_update_key(separator_key, promotion_key, &mut key_vec)?;

                return Ok(());
            }
            _ => {
                return Err(anyhow::Error::msg("Unexpected Error"));
            }
        }
    }

    pub fn merge(&mut self, sibling: &NodeInner) -> anyhow::Result<()> {
        match self.node_type {
            NodeType::Internal(ref mut children, ref mut keys, _) => {
                if let NodeType::Internal(sibling_pointers, sibling_keys, _) = &sibling.node_type {
                    let mut merged_pointers: Vec<PagePointer> = children
                        .iter()
                        .chain(sibling_pointers.iter())
                        .cloned()
                        .collect();

                    let mut merged_keys: Vec<NodeKey> = keys
                        .iter()
                        .chain(sibling_keys.iter())
                        .cloned()
                        .collect();

                    // Is not viable for large sets
                    merged_pointers.sort();
                    merged_keys.sort();

                    *children = merged_pointers;
                    *keys = merged_keys;

                    anyhow::Ok(())
                } else {
                    return Err(anyhow::Error::msg("Unexpected Error"));
                }
            }

            NodeType::Leaf(ref mut entries, ref mut current_next, _) => {
                if let NodeType::Leaf(sibling_entries, sibling_next, _) = &sibling.node_type {
                    let mut merged_entries: Vec<NodeKey> = entries
                        .iter()
                        .chain(sibling_entries.iter())
                        .cloned()
                        .collect();

                    // Is not viable for large sets
                    merged_entries.sort();

                    *entries = merged_entries;
                    *current_next = sibling_next.clone();

                    anyhow::Ok(())
                } else {
                    return Err(anyhow::Error::msg("Unexpected Error"));
                }
            }

            NodeType::Unexpected => {
                return Err(anyhow::Error::msg("Unexpected Error"));
            }
        }
    }
}
