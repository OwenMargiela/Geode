#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use bytes::{Buf, BufMut};

use crate::{
    index::tree::{
        byte_box::DataType, index_types::{KeyValuePair, NodeKey}, tree_node::{node_type::NodeType, tree_node_inner::NodeInner}, tree_page::tree_page_layout::{
            FromByte, InternalNodeHeader, LeafNodeHeader, NodeHeader, INTERNAL_NODE_HEADER_SIZE,
            PAGE_SIZE,
        }
    },
    storage::page::btree_page_layout::ToByte,
};

use super::page::TreePage;

pub struct Codec {
    key_type: DataType,
    value_type: DataType,
}

impl Codec {
    pub fn decode(&self, page: &TreePage) -> anyhow::Result<NodeInner> {
        let raw = page.get_data();
        let mut node_type = NodeType::from(raw[NodeHeader::NodeType.offset()]);

        let is_root = raw[NodeHeader::IsRoot.offset()].from_byte();

        let pointer = page
            .get_sized_value_from_offset(NodeHeader::Pointer.offset(), NodeHeader::Pointer.size())
            .get_u32();

        match node_type {
            NodeType::Internal(ref mut children, ref mut keys, _) => {
                let num_children = page
                    .get_sized_value_from_offset(
                        InternalNodeHeader::NumChildren.offset(),
                        InternalNodeHeader::NumChildren.size(),
                    )
                    .get_u32();

                let mut offset = INTERNAL_NODE_HEADER_SIZE;

                for _ in 0..num_children {
                    let mut entry = &raw[offset..];
                    let pointer = entry.get_u32();
                    children.push(pointer);

                    if entry.has_remaining() {
                        let key_len = entry.get_u32() as usize;
                        let key = &entry[..key_len];

                        let data_box = self.key_type.to_byte_box(key);

                        keys.push(NodeKey::GuidePost(data_box));

                        offset = (size_of::<u32>() * 2) + key_len;
                    }
                }

                return Ok(NodeInner {
                    node_type,
                    is_root,
                    pointer,

                    next_pointer: None,
                });
            }
            NodeType::Leaf(ref mut entries, _, _) => {
                let next_pointer = page
                    .get_sized_value_from_offset(
                        LeafNodeHeader::NextLeafPointer.size(),
                        LeafNodeHeader::NextLeafPointer.offset(),
                    )
                    .get_u32();

                let num_entries = page
                    .get_sized_value_from_offset(
                        LeafNodeHeader::NumPairs.offset(),
                        LeafNodeHeader::NumPairs.size(),
                    )
                    .get_u32();

                let mut offset = INTERNAL_NODE_HEADER_SIZE;

                for _ in 0..num_entries {
                    let mut entry = &raw[offset..];
                    let key_len = entry.get_u32() as usize;
                    let key = &entry[..key_len];

                    let key_box = self.key_type.to_byte_box(key);

                    entry.advance(key_len);

                    let data_len = entry.get_u32() as usize;
                    let data = &entry[..data_len];

                    let value_box = self.value_type.to_byte_box(data);

                    entries.push(NodeKey::KeyValuePair(KeyValuePair {
                        key: key_box,
                        value: value_box,
                    }));

                    offset = (size_of::<u32>() * 4) + key_len;
                }

                return Ok(NodeInner {
                    node_type,
                    is_root,
                    pointer,

                    next_pointer: Some(next_pointer),
                });
            }
            NodeType::Unexpected => {
                return Err(anyhow::Error::msg("Unexpected Error"));
            }
        }
    }

    pub fn encode(node: &NodeInner) -> anyhow::Result<TreePage> {
        let is_root = node.is_root;
        let pointer = node.pointer;

        let mut raw: Vec<u8> = Vec::with_capacity(PAGE_SIZE);

        // is_root byte
        raw[NodeHeader::IsRoot.offset()] = is_root.to_byte();

        // node_type byte
        raw[NodeHeader::NodeType.offset()] = u8::from(&node.node_type);

        match node.node_type {
            NodeType::Internal(ref children, ref keys, _) => {
                // Header
                let mut start = InternalNodeHeader::NumChildren.offset();
                let mut end = start + InternalNodeHeader::NumChildren.size();

                let len = children.len() as u32;
                raw[start..end].clone_from_slice(&len.to_le_bytes());

                start = NodeHeader::Pointer.offset();
                end = start + NodeHeader::Pointer.size();

                raw[start..end].clone_from_slice(&pointer.to_le_bytes());

                // Data Section

                for pointer in children.iter().into_iter() {
                    raw.put_u32_le(*pointer);

                    if keys.is_empty() {
                        continue;
                    }

                    let key = match keys.iter().next().unwrap() {
                        NodeKey::GuidePost(key) => key,
                        _ => {
                            return Err(anyhow::Error::msg("Unexpected Error"));
                        }
                    };

                    let key_len = key.data_length as u32;
                    raw.put_u32(key_len);
                    raw.put_slice(&key.data);
                }

                // Fits the page to a fixed size of 4KB
                let extend_size: usize = PAGE_SIZE - raw.len();
                let extend_vec: Vec<u8> = vec![0u8; extend_size];

                raw.extend(extend_vec);
                return Ok(TreePage::new(raw.try_into().unwrap()));
            }

            NodeType::Leaf(ref entries, _, ref next) => {
                // Header
                let mut start = LeafNodeHeader::NumPairs.offset();
                let mut end = start + LeafNodeHeader::NumPairs.size();

                let entry_len = entries.len() as u32;
                raw[start..end].clone_from_slice(&entry_len.to_le_bytes());

                if let Some(next) = next {
                    start = LeafNodeHeader::NextLeafPointer.offset();
                    end = start + LeafNodeHeader::NextLeafPointer.size();

                    raw[start..end].clone_from_slice(&next.to_le_bytes());
                }

                // Data Section

                for pair in entries {
                    match pair {
                        NodeKey::KeyValuePair(kv_pair) => {
                            let key = &kv_pair.key;
                            let value = &kv_pair.value;
                            raw.put_u32(key.data_length as u32);
                            raw.put_slice(&key.data);

                            raw.put_u32(value.data_length as u32);
                            raw.put_slice(&value.data);
                        }
                        _ => return Err(anyhow::Error::msg("Unexpected Error")),
                    }
                }

                // Fits the page to a fixed size of 4KB
                let extend_size: usize = PAGE_SIZE - raw.len();
                let extend_vec: Vec<u8> = vec![0u8; extend_size];

                raw.extend(extend_vec);
                return Ok(TreePage::new(raw.try_into().unwrap()));
            }

            NodeType::Unexpected => {
                return Err(anyhow::Error::msg("Unexpected Error"));
            }
        }
    }
}
