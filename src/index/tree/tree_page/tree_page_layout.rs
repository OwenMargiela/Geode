#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

// Branching factor
pub const MAX_BRANCHING_FACTOR: usize = 32;

/// A single page size.
///
/// Each page represents a node in the BTree.
pub const PAGE_SIZE: usize = 4096;

/// The size of page pointors and/or any u32 value
pub const PTR_SIZE: usize = size_of::<u32>();

#[derive(Debug)]
pub enum NodeHeader {
    IsRoot,
    NodeType,
    Pointer,
}

impl NodeHeader {
    pub fn size(&self) -> usize {
        match *self {
            NodeHeader::IsRoot => IS_ROOT_SIZE,
            NodeHeader::NodeType => NODE_TYPE_SIZE,
            NodeHeader::Pointer => POINTER_SIZE,
        }
    }

    pub fn offset(&self) -> usize {
        match *self {
            NodeHeader::IsRoot => IS_ROOT_OFFSET,
            NodeHeader::NodeType => NODE_TYPE_OFFSET,
            NodeHeader::Pointer => POINTER_OFFSET,
        }
    }
}

// Defining the constants
pub const IS_ROOT_SIZE: usize = 1;
pub const IS_ROOT_OFFSET: usize = 0;
pub const NODE_TYPE_SIZE: usize = 1;
pub const NODE_TYPE_OFFSET: usize = 1;
pub const POINTER_OFFSET: usize = 2;
pub const POINTER_SIZE: usize = PTR_SIZE;

pub const COMMON_NODE_HEADER_SIZE: usize = NODE_TYPE_SIZE + IS_ROOT_SIZE + POINTER_SIZE;

#[derive(Debug)]
pub enum LeafNodeHeader {
    NumPairs,
    NextLeafPointer,
}

impl LeafNodeHeader {
    pub fn size(&self) -> usize {
        match *self {
            LeafNodeHeader::NumPairs => LEAF_NODE_NUM_PAIRS_SIZE,
            LeafNodeHeader::NextLeafPointer => NEXT_LEAF_POINTER_SIZE,
        }
    }

    pub fn offset(&self) -> usize {
        match *self {
            LeafNodeHeader::NumPairs => LEAF_NODE_NUM_PAIRS_OFFSET,
            LeafNodeHeader::NextLeafPointer => NEXT_LEAF_POINTER_OFFSET,
        }
    }
}

// Defining the constants specific to the leaf node header
pub const LEAF_NODE_NUM_PAIRS_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
pub const LEAF_NODE_NUM_PAIRS_SIZE: usize = PTR_SIZE;

pub const NEXT_LEAF_POINTER_OFFSET: usize = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_PAIRS_SIZE;
pub const NEXT_LEAF_POINTER_SIZE: usize = PTR_SIZE;

pub const LEAF_NODE_HEADER_SIZE: usize =
    COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_PAIRS_SIZE + NEXT_LEAF_POINTER_SIZE;

#[derive(Debug)]
pub enum InternalNodeHeader {
    NumChildren,
}

impl InternalNodeHeader {
    pub fn size(&self) -> usize {
        match *self {
            InternalNodeHeader::NumChildren => INTERNAL_NODE_NUM_CHILDREN_SIZE,
        }
    }

    pub fn offset(&self) -> usize {
        match *self {
            InternalNodeHeader::NumChildren => INTERNAL_NODE_NUM_CHILDREN_OFFSET,
        }
    }
}

// Defining the constants specific to the internal node header
pub const INTERNAL_NODE_NUM_CHILDREN_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
pub const INTERNAL_NODE_NUM_CHILDREN_SIZE: usize = PTR_SIZE;

pub const INTERNAL_NODE_HEADER_SIZE: usize =
    COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_CHILDREN_SIZE;

pub trait FromByte {
    fn from_byte(&self) -> bool;
}

impl FromByte for u8 {
    fn from_byte(&self) -> bool {
        matches!(self, 0x01)
    }
}
