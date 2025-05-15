#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use crate::index::tree::index_types::NodeKey;


pub type PagePointer = u32;

#[derive(PartialEq, Eq, Clone, Debug)]
pub enum NodeType {
    /// Internal nodes contain a vector of pointers to their children and a vector of keys.
    Internal(Vec<PagePointer>, Vec<NodeKey>, PagePointer),

    /// Leaf nodes contain a vector of Keys and values.
    Leaf(Vec<NodeKey>, PagePointer, Option<PagePointer>),

    Unexpected,
}

// Converts a byte to a NodeType.
impl From<u8> for NodeType {
    fn from(orig: u8) -> NodeType {
        match orig {
            0x01 => NodeType::Internal(
                Vec::<PagePointer>::new(),
                Vec::<NodeKey>::new(),
                u32::default(),
            ),
            0x02 => NodeType::Leaf(Vec::<NodeKey>::new(), u32::default(), None),
            _ => NodeType::Unexpected,
        }
    }
}

// Converts a NodeType to a byte.
impl From<&NodeType> for u8 {
    fn from(orig: &NodeType) -> u8 {
        match orig {
            NodeType::Internal(_, _, _) => 0x01,
            NodeType::Leaf(_, _, _) => 0x02,
            NodeType::Unexpected => 0x03,
        }
    }
}
