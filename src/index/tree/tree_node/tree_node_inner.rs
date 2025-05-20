#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use crate::index::tree::{ byte_box::ByteBox, index_types::{ KeyValuePair, NodeKey } };

use super::node_type::{ NodeType, PagePointer };

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeInner {
    pub node_type: NodeType,
    pub is_root: bool,
    pub pointer: PagePointer,
    pub next_pointer: Option<PagePointer>,
    pub is_leaf: bool,
}

pub type KvNode = NodeInner;
pub type GuidePostNode = NodeInner;
pub enum Node {
    InnerNode(GuidePostNode),
    LeafNode(KvNode),
}

pub struct PopResult {
    pub(crate) pop_key: NodeKey,
    pub(crate) promotion_key: NodeKey,
    pub(crate) child_pointer: Option<PagePointer>,
}

impl NodeInner {
    pub fn new(
        node_type: NodeType,
        is_root: bool,
        pointer: PagePointer,
        next_pointer: Option<PagePointer>
    ) -> NodeInner {
        match node_type {
            NodeType::Internal(_, _, _) =>
                NodeInner {
                    node_type,
                    is_root,
                    pointer,
                    next_pointer,
                    is_leaf: false,
                },
            _ =>
                NodeInner {
                    node_type,
                    is_root,
                    pointer,
                    next_pointer,
                    is_leaf: true,
                },
        }
    }

    pub fn pop_front(&mut self) -> anyhow::Result<PopResult> {
        match &self.node_type {
            NodeType::Internal(_, keys, _) => {
                let len = keys.len();
                let promotion_key = keys
                    .get(len - 1)
                    .unwrap()
                    .clone();
                let (key, page_id) = self.remove_inner_node_entry(0, false)?;

                let (promotion_key, _) = NodeInner::deconstruct_value(&promotion_key);

                let (key_box, _) = NodeInner::deconstruct_value(&key);

                let res = PopResult {
                    pop_key: NodeKey::GuidePost(key_box),
                    promotion_key: NodeKey::GuidePost(promotion_key),
                    child_pointer: Some(page_id),
                };

                Ok(res)
            }
            NodeType::Leaf(entries, _, _) => {
                let len = entries.len();
                let promotion_key = entries
                    .get(len - 1)
                    .unwrap()
                    .clone();
                let entry = self.remove_key_value_at_index(0)?;

                let (promotion_key, _) = NodeInner::deconstruct_value(&promotion_key);

                let (key_box, value_box) = NodeInner::deconstruct_value(&entry);

                let kv_pair = KeyValuePair {
                    key: key_box,
                    value: value_box.unwrap(),
                };

                Ok(PopResult {
                    pop_key: NodeKey::KeyValuePair(kv_pair),
                    promotion_key: NodeKey::GuidePost(promotion_key),
                    child_pointer: None,
                })
            }
            NodeType::Unexpected => {
                return Err(anyhow::Error::msg("Unexpected Error"));
            }
        }
    }

    pub fn pop_back(&mut self) -> anyhow::Result<PopResult> {
        match self.node_type {
            NodeType::Internal(_, ref mut keys, _) => {
                let len = keys.len();

                let promotion_key = keys.get(0).unwrap().clone();
                let (key, page_id) = self.remove_inner_node_entry(len - 1, true)?;

                let (promotion_key, _) = NodeInner::deconstruct_value(&promotion_key);
                let (key, _) = NodeInner::deconstruct_value(&key);

                Ok(PopResult {
                    pop_key: NodeKey::GuidePost(key),
                    promotion_key: NodeKey::GuidePost(promotion_key),
                    child_pointer: Some(page_id),
                })
            }
            NodeType::Leaf(ref mut entries, _, _) => {
                let len = entries.len();

                let promotion_key = entries.get(0).unwrap().clone();
                let (promotion_key, _) = NodeInner::deconstruct_value(&promotion_key);

                let entry = self.remove_key_value_at_index(len)?;
                let (key, value) = NodeInner::deconstruct_value(&entry);

                let kv_pair = KeyValuePair {
                    key: key,
                    value: value.unwrap(),
                };

                Ok(PopResult {
                    pop_key: NodeKey::KeyValuePair(kv_pair),
                    promotion_key: NodeKey::GuidePost(promotion_key),
                    child_pointer: None,
                })
            }
            NodeType::Unexpected => {
                return Err(anyhow::Error::msg("Unexpected Error"));
            }
        }
    }

    pub fn get_key_array_length(&self) -> usize {
        match &self.node_type {
            NodeType::Internal(_, keys, _) => keys.len(),
            NodeType::Leaf(entries, _, _) => entries.len(),

            NodeType::Unexpected => 0,
        }
    }
}

// Node helper functions
impl NodeInner {
    pub fn binary_search_in_key(node_key: &NodeKey, vec: &Vec<NodeKey>) -> anyhow::Result<usize> {
        let (search_key, _) = NodeInner::deconstruct_value(&node_key);

        let idx = vec
            .binary_search_by_key(&search_key, |p| {
                let (key, _) = NodeInner::deconstruct_value(&p);
                key
            })
            .unwrap_or_else(|x| x);

        Ok(idx)
    }

    /// Returns a key and optional value
    pub fn deconstruct_value(value: &NodeKey) -> (ByteBox, Option<ByteBox>) {
        match value {
            NodeKey::GuidePost(key) => (key.clone(), None),
            NodeKey::KeyValuePair(kv_pair) => (kv_pair.key.clone(), Some(kv_pair.value.clone())),
        }
    }

    pub fn find_key(search: NodeKey, vec: &Vec<NodeKey>) -> Result<usize, anyhow::Error> {
        let (search_data, _) = NodeInner::deconstruct_value(&search);

        let res = NodeInner::binary_search_in_key(&search, &vec);

        res
    }

    pub fn get_key_at_index(idx: usize, vec: &Vec<NodeKey>) -> anyhow::Result<NodeKey> {
        Ok(vec.get(idx).unwrap().clone())
    }

    pub fn find_and_update_key(
        search_key: NodeKey,
        update_key: NodeKey,
        vec: &mut Vec<NodeKey>
    ) -> anyhow::Result<()> {
        let idx = NodeInner::find_key(search_key, vec)?;
        vec[idx] = update_key;
        Ok(())
    }

    /// Returns key_value array for leaf nodes and guide post keys for internal nodes
    pub fn get_key_vec(node_type: &NodeType) -> anyhow::Result<&Vec<NodeKey>> {
        let vec = match node_type {
            NodeType::Internal(_, keys, _) => keys,
            NodeType::Leaf(pairs, _, _) => pairs,

            NodeType::Unexpected => {
                return Err(anyhow::Error::msg("Unexpected Error"));
            }
        };

        return Ok(vec);
    }
    /// Returns a mutable reference to key_value array for leaf nodes and guide post keys for internal nodes
    pub fn get_key_vec_mut(node_type: &mut NodeType) -> anyhow::Result<&mut Vec<NodeKey>> {
        match node_type {
            NodeType::Internal(_, ref mut keys, _) => Ok(keys),
            NodeType::Leaf(ref mut pairs, _, _) => Ok(pairs), // if this is Vec<NodeKey> too
            NodeType::Unexpected => Err(anyhow::Error::msg("Unexpected Error")),
        }
    }
}
