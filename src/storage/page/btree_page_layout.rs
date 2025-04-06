// Branching factor
pub const MAX_BRANCHING_FACTOR: usize = 32;

/// A single page size.
/// Each page represents a node in the BTree.
pub const PAGE_SIZE: usize = 4096;

pub const PTR_SIZE: usize = size_of::<usize>();

/// Common Node header layout (Ten bytes in total)
pub const IS_ROOT_SIZE: usize = 1;
pub const IS_ROOT_OFFSET: usize = 0;
pub const NODE_TYPE_SIZE: usize = 1;
pub const NODE_TYPE_OFFSET: usize = 1;
pub const POINTER_OFFSET: usize = 2; // Changed to contain the pointer of the current page
pub const POINTER_SIZE: usize = PTR_SIZE; // 8 
pub const COMMON_NODE_HEADER_SIZE: usize = NODE_TYPE_SIZE + IS_ROOT_SIZE + POINTER_SIZE;
//               8                       =       1        +       1      +           8

/// Leaf node header layout (Eighteen bytes in total)
///
/// Space for keys and values: PAGE_SIZE - LEAF_NODE_HEADER_SIZE = 4096 - 26 = 4070 bytes.
/// Which leaves 4076 / keys_limit = 20 (ten for key and 10 for value).
pub const LEAF_NODE_NUM_PAIRS_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
pub const LEAF_NODE_NUM_PAIRS_SIZE: usize = PTR_SIZE;

pub const NEXT_LEAF_POINTER_OFFSET: usize =  COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_PAIRS_SIZE ;
pub const NEXT_LEAF_POINTER_SIZE : usize = PTR_SIZE;

pub const LEAF_NODE_HEADER_SIZE: usize = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_PAIRS_SIZE + NEXT_LEAF_POINTER_SIZE;
//                 26                  =           10            +           8              +           8

/// Internal header layout (Eighteen bytes in total)
///
// Space for children and keys: PAGE_SIZE - INTERNAL_NODE_HEADER_SIZE = 4096 - 18 = 4078 bytes.
pub const INTERNAL_NODE_NUM_CHILDREN_OFFSET: usize = COMMON_NODE_HEADER_SIZE;
pub const INTERNAL_NODE_NUM_CHILDREN_SIZE: usize = PTR_SIZE;
pub const INTERNAL_NODE_HEADER_SIZE: usize =
//           18         =         
COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_CHILDREN_SIZE;
//           10         +           8

/// On a 64 bit machine the maximum space to keep all of the pointer
/// is 200 * 8 = 1600 bytes.
#[allow(dead_code)]
pub const MAX_SPACE_FOR_CHILDREN: usize = MAX_BRANCHING_FACTOR * PTR_SIZE;

/// This leaves the keys of an internal node 2478 bytes:
/// We use 1990 bytes for keys which leaves 488 bytes as junk.
/// This means each key is limited to 12 bytes. (2476 / keys limit = ~12)
/// Rounded down to 10 to accomodate the leaf node.
#[allow(dead_code)]
pub const MAX_SPACE_FOR_KEYS: usize =
    PAGE_SIZE - INTERNAL_NODE_HEADER_SIZE - MAX_SPACE_FOR_CHILDREN;

/// Key, Value sizes.
pub const KEY_SIZE: usize = 10;
pub const VALUE_SIZE: usize = 10;

// RowID segment size
pub const ROW_ID_SEGMENT_SIZE : usize = 4;

// 4-Byte length 
// pub const LENGTH_4_BYTE_SLICE: usize = 4;


/// Wrappers for converting byte to bool and back.
pub trait FromByte {
    fn from_byte(&self) -> bool;
}

pub trait ToByte {
    fn to_byte(&self) -> u8;
}

impl FromByte for u8 {
    fn from_byte(&self) -> bool {
        matches!(self, 0x01)
    }
}

impl ToByte for bool {
    fn to_byte(&self) -> u8 {
        match self {
            true => 0x01,
            false => 0x00,
        }
    }
}
