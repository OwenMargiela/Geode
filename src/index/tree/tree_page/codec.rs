#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::io::{ Cursor, Read, Seek, SeekFrom, Write };

use byteorder::{ LittleEndian, ReadBytesExt, WriteBytesExt };

use crate::{
    index::tree::{
        byte_box::DataType,
        index_types::{ KeyValuePair, NodeKey },
        tree_node::{ node_type::NodeType, tree_node_inner::NodeInner },
        tree_page::tree_page_layout::{
            FromByte,
            InternalNodeHeader,
            LeafNodeHeader,
            NodeHeader,
            INTERNAL_NODE_HEADER_SIZE,
            PAGE_SIZE,
        },
    },
    storage::page::btree_page_layout::ToByte,
};

use super::{ page::TreePage, tree_page_layout::LEAF_NODE_HEADER_SIZE };

#[derive(Clone)]
pub struct Codec {
    pub(crate) key_type: DataType,
    pub(crate) value_type: DataType,
}

impl Codec {
    pub fn decode(&self, page: &TreePage) -> anyhow::Result<NodeInner> {
        let mut raw = page.get_data();
        let mut cursor = Cursor::new(&mut raw[..]);

        cursor.seek(SeekFrom::Start(NodeHeader::IsRoot.offset() as u64))?;
        let is_root_byte = cursor.read_u8()?;
        let is_root = is_root_byte.from_byte();

        cursor.seek(SeekFrom::Start(NodeHeader::NodeType.offset() as u64))?;
        let mut node_type = NodeType::from(cursor.read_u8()?);

        cursor.seek(SeekFrom::Start(NodeHeader::Pointer.offset() as u64))?;

        let pointer = cursor.read_u32::<LittleEndian>()?;
        match node_type {
            NodeType::Internal(ref mut children, ref mut keys, _) => {
                cursor.seek(SeekFrom::Start(InternalNodeHeader::NumChildren.offset() as u64))?;
                let num_children = cursor.read_u32::<LittleEndian>()?;

                let offset = INTERNAL_NODE_HEADER_SIZE;
                cursor.seek(SeekFrom::Start(offset as u64))?;

                for key_idx in 0..num_children {
                    let pointer = cursor.read_u32::<LittleEndian>()?;

                    children.push(pointer);

                    if key_idx == num_children - 1 {
                        break;
                    }

                    let key_len = cursor.read_u32::<LittleEndian>()?;
                    let mut buf = vec![0u8; key_len as usize];

                    cursor.read_exact(&mut buf)?;
                    let data_box = self.key_type.to_byte_box(&buf);

                    keys.push(NodeKey::GuidePost(data_box));
                }

                return Ok(NodeInner {
                    node_type,
                    is_root,
                    pointer,

                    next_pointer: None,
                    is_leaf: false,
                });
            }
            NodeType::Leaf(ref mut entries, _, _) => {
                cursor.seek(SeekFrom::Start(LeafNodeHeader::NextLeafPointer.offset() as u64))?;

                let mut next_pointer: Option<u32> = Some(cursor.read_u32::<LittleEndian>()?);
                if let Some(next) = next_pointer {
                    if next == 0 {
                        next_pointer = None;
                    }
                }

                cursor.seek(SeekFrom::Start(LeafNodeHeader::NumPairs.offset() as u64))?;
                let num_entries = cursor.read_u32::<LittleEndian>()?;

                let offset = LEAF_NODE_HEADER_SIZE;
                cursor.seek(SeekFrom::Start(offset as u64))?;

                for _ in 0..num_entries {
                    let key_len = cursor.read_u32::<LittleEndian>()?;
                    let mut buf = vec![0u8; key_len as usize];

                    cursor.read_exact(&mut buf)?;
                    let key_box = self.key_type.to_byte_box(&buf);

                    let data_len = cursor.read_u32::<LittleEndian>()?;
                    buf = vec![0u8; data_len as usize];

                    cursor.read_exact(&mut buf)?;
                    let value_box = self.value_type.to_byte_box(&buf);

                    entries.push(
                        NodeKey::KeyValuePair(KeyValuePair {
                            key: key_box,
                            value: value_box,
                        })
                    );
                }

                return Ok(NodeInner {
                    node_type,
                    is_root,
                    pointer,

                    next_pointer,
                    is_leaf: true,
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

        let next = node.next_pointer;

        let mut raw: Vec<u8> = vec![0; PAGE_SIZE];
        let mut cursor = Cursor::new(&mut raw[..]);

        // node_type byte
        cursor.seek(SeekFrom::Start(NodeHeader::NodeType.offset() as u64))?;
        cursor.write_u8(u8::from(&node.node_type))?;

        // is_root byte
        cursor.seek(SeekFrom::Start(NodeHeader::IsRoot.offset() as u64))?;
        cursor.write_u8(is_root.to_byte())?;

        // Pointer
        cursor.seek(SeekFrom::Start(NodeHeader::Pointer.offset() as u64))?;
        cursor.write_u32::<LittleEndian>(pointer)?;

        match node.node_type {
            NodeType::Internal(ref children, ref keys, _) => {
                // Header
                cursor.seek(SeekFrom::Start(InternalNodeHeader::NumChildren.offset() as u64))?;
                cursor.write_u32::<LittleEndian>(children.len() as u32)?;

                // Data Section
                cursor.seek(SeekFrom::Start(INTERNAL_NODE_HEADER_SIZE as u64))?;

                for (key_idx, pointer) in children.iter().into_iter().enumerate() {
                    cursor.write_u32::<LittleEndian>(*pointer)?;

                    if key_idx == children.len() - 1 {
                        continue;
                    }

                    let key = match keys.get(key_idx).unwrap() {
                        NodeKey::GuidePost(key) => key,
                        _ => {
                            return Err(anyhow::Error::msg("Unexpected Error"));
                        }
                    };
                    // println!("{:?}", key);
                    let key_len = key.data_length as u32;
                    cursor.write_u32::<LittleEndian>(key_len)?;

                    cursor.write_all(&key.data)?;
                }

                // Fits the page to a fixed size of 4KB
                assert_eq!(raw.len(), PAGE_SIZE);
                return Ok(TreePage::new(raw.try_into().unwrap()));
            }

            NodeType::Leaf(ref entries, _, _) => {
                // Header
                cursor.seek(SeekFrom::Start(LeafNodeHeader::NumPairs.offset() as u64))?;
                cursor.write_u32::<LittleEndian>(entries.len() as u32)?;

                cursor.seek(SeekFrom::Start(LeafNodeHeader::NextLeafPointer.offset() as u64))?;
                if let Some(next) = next {
                    cursor.write_u32::<LittleEndian>(next)?;
                } else {
                    cursor.write_u32::<LittleEndian>(0)?;
                }

                // Data Section
                cursor.seek(SeekFrom::Start(LEAF_NODE_HEADER_SIZE as u64))?;

                for pair in entries {
                    match pair {
                        NodeKey::KeyValuePair(kv_pair) => {
                            let key = &kv_pair.key;
                            let value = &kv_pair.value;

                            let key_len = key.data_length as u32;
                            cursor.write_u32::<LittleEndian>(key_len)?;
                            cursor.write_all(&key.data)?;

                            let value_len = value.data_length as u32;
                            cursor.write_u32::<LittleEndian>(value_len)?;
                            cursor.write_all(&value.data)?;
                        }
                        _ => {
                            return Err(anyhow::Error::msg("Unexpected Error"));
                        }
                    }
                }

                // Fits the page to a fixed size of 4KB
                assert_eq!(raw.len(), PAGE_SIZE);
                return Ok(TreePage::new(raw.try_into().unwrap()));
            }

            NodeType::Unexpected => {
                return Err(anyhow::Error::msg("Unexpected Error"));
            }
        }
    }
}
