use std::cmp::{Eq, Ord, Ordering, PartialOrd};

use super::errors::Error;

#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub struct PageId(pub u64);

#[derive(Clone, Eq, PartialEq, PartialOrd, Ord, Debug)]
pub struct Key(pub [u8; 10]);

impl Key {
    pub fn new(key: u64) -> Self {
        fn serialize_int_to_10_bytes(n: u64) -> [u8; 10] {
            let mut bytes = [0u8; 10]; // Create a 10-byte array initialized with zeros.
            let n_bytes = n.to_le_bytes(); // Convert the integer to bytes (little-endian).

            // Copy the integer bytes into the fixed-size array (first 8 bytes for u64).
            bytes[..n_bytes.len()].copy_from_slice(&n_bytes);

            bytes
        }

        Key(serialize_int_to_10_bytes(key))
    }
}

#[derive(Clone, Eq, PartialEq, Debug)]
// Page Number and Slot Index
pub struct RowID {
    pub page_id: [u8; 4],
    pub slot_index: [u8; 4],
}

#[derive(Clone, Eq, PartialEq, Debug)]

// Next pointer is currently 8 bytes when it really should be 8
pub struct NextPointer(pub Option<[u8; 8]>);

impl Ord for RowID {
    fn cmp(&self, other: &Self) -> Ordering {
        // Compare first by the page_id, then by slot_index
        self.page_id
            .cmp(&other.page_id)
            .then(self.slot_index.cmp(&other.slot_index))
    }
}

impl PartialOrd for RowID {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl RowID {
    pub fn new(page_id: u32, slot_index: u32) -> Self {
        fn serialize_int_to_4_bytes(n: u32) -> [u8; 4] {
            let mut bytes = [0u8; 4]; // Create a 10-byte array initialized with zeros.
            let n_bytes = n.to_le_bytes(); // Convert the integer to bytes (little-endian).

            // Copy the integer bytes into the fixed-size array (first 8 bytes for u64).
            bytes[..n_bytes.len()].copy_from_slice(&n_bytes);

            bytes
        }

        RowID {
            page_id: serialize_int_to_4_bytes(page_id),
            slot_index: serialize_int_to_4_bytes(slot_index),
        }
    }
}

#[derive(Clone, Eq, Debug)]
pub struct KeyValuePair {
    pub key: [u8; 10], // Changed from String to Vec<u8>
    pub value: RowID,
}

impl Ord for KeyValuePair {
    fn cmp(&self, other: &Self) -> Ordering {
        // Compare first by the key (as Vec<u8>), and then by the value (RowID)
        self.key.cmp(&other.key).then(self.value.cmp(&other.value))
    }
}

impl PartialOrd for KeyValuePair {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for KeyValuePair {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.value == other.value
    }
}
impl KeyValuePair {
    // New constructor that accepts a Vec<u8> for the key and RowID for the value
    pub fn new(key: u64, value: RowID) -> KeyValuePair {
        fn serialize_int_to_10_bytes(n: u64) -> [u8; 10] {
            let mut bytes = [0u8; 10]; // Create a 10-byte array initialized with zeros.
            let n_bytes = n.to_le_bytes(); // Convert the integer to bytes (little-endian).

            // Copy the integer bytes into the fixed-size array (first 8 bytes for u64).
            bytes[..n_bytes.len()].copy_from_slice(&n_bytes);

            bytes
        }

        KeyValuePair {
            key: serialize_int_to_10_bytes(key),
            value,
        }
    }
}

// NodeType Represents different node types in the BTree.
#[derive(PartialEq, Eq, Clone, Debug)]
pub enum NodeType {
    /// Internal nodes contain a vector of pointers to their children and a vector of keys.
    Internal(Vec<PageId>, Vec<Key>),

    /// Leaf nodes contain a vector of Keys and values.
    Leaf(Vec<KeyValuePair>, NextPointer),

    Unexpected,
}

pub fn update_next_pointer(node: &mut NodeType, next_page_pointer: [u8; 8]) -> Result<(), Error> {
    match node {
        NodeType::Leaf(_, next_pointer) => {
            *next_pointer = NextPointer(Some(next_page_pointer));
            Ok(())
        }
        _ => Err(Error::UnexpectedError),
    }
}

// Converts a byte to a NodeType.
impl From<u8> for NodeType {
    fn from(orig: u8) -> NodeType {
        match orig {
            0x01 => NodeType::Internal(Vec::<PageId>::new(), Vec::<Key>::new()),
            0x02 => NodeType::Leaf(Vec::<KeyValuePair>::new(), NextPointer(None)),
            _ => NodeType::Unexpected,
        }
    }
}

// Converts a NodeType to a byte.
impl From<&NodeType> for u8 {
    fn from(orig: &NodeType) -> u8 {
        match orig {
            NodeType::Internal(_, _) => 0x01,
            NodeType::Leaf(_, _) => 0x02,
            NodeType::Unexpected => 0x03,
        }
    }
}
