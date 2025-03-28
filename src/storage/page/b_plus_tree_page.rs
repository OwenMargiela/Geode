use crate::index::{
    errors::Error,
    node::Node,
    node_type::{self, NodeType, PageId},
};

use super::{
    btree_page_layout::{
        ToByte, IS_ROOT_OFFSET, NEXT_LEAF_POINTER_OFFSET, NEXT_LEAF_POINTER_SIZE, NODE_TYPE_OFFSET,
        PARENT_POINTER_OFFSET, PARENT_POINTER_SIZE, PTR_SIZE,
    },
    page::{page_constants::PAGE_SIZE, Page},
};

/// Value is a wrapper for a value in the page.
pub struct Value(pub usize);

/// Attempts to convert a slice to an array of a fixed size (PTR_SIZE),
/// and then return the BigEndian value of the byte array.

impl TryFrom<&[u8]> for Value {
    type Error = Error;

    fn try_from(arr: &[u8]) -> Result<Self, Self::Error> {
        if arr.len() > PTR_SIZE {
            return Err(Error::TryFromSliceError("Unexpected Error: Array recieved is larger than the maximum allowed size of: 4096B."));
        }

        let mut truncated_arr = [0u8; PTR_SIZE];
        for (i, item) in arr.iter().enumerate() {
            truncated_arr[i] = *item;
        }

        Ok(Value(usize::from_be_bytes(truncated_arr)))
    }
}

pub struct BTreePage {
    pub data: [u8; PAGE_SIZE],
}

impl BTreePage {
    pub fn new(data: [u8; PAGE_SIZE]) -> BTreePage {
        BTreePage { data }
    }

    /// write_value_at_offset writes a given value (as BigEndian) at a certain offset
    /// overriding values at that offset.
    pub fn write_value_at_offset(&mut self, offset: usize, value: usize) -> Result<(), Error> {
        if offset > PAGE_SIZE - PTR_SIZE {
            return Err(Error::UnexpectedError);
        }
        let bytes = value.to_be_bytes();
        self.data[offset..offset + PTR_SIZE].clone_from_slice(&bytes);
        Ok(())
    }

    /// get_value_from_offset Fetches a value calculated as BigEndian, sized to usize.
    /// This function may error as the value might not fit into a usize.
    pub fn get_value_from_offset(&self, offset: usize) -> Result<u64, Error> {
        let bytes = &self.data[offset..offset + PTR_SIZE];
        let Value(res) = Value::try_from(bytes)?;
        Ok(res.try_into().unwrap())
    }

    /// insert_bytes_at_offset pushes #size bytes from offset to end_offset
    /// inserts #size bytes from given slice.

    pub fn insert_bytes_at_offset(
        &mut self,
        bytes: &[u8],
        offset: usize,
        end_offset: usize,
        size: usize,
    ) -> Result<(), Error> {
        // This Should not occur - better verify.
        if end_offset + size > self.data.len() {
            return Err(Error::UnexpectedError);
        }
        for idx in (offset..=end_offset).rev() {
            self.data[idx + size] = self.data[idx]
        }
        self.data[offset..offset + size].clone_from_slice(bytes);
        Ok(())
    }

    /// write_bytes_at_offset write bytes at a certain offset overriding previous values.
    pub fn write_bytes_at_offset(
        &mut self,
        bytes: &[u8],
        offset: usize,
        size: usize,
    ) -> Result<(), Error> {
        self.data[offset..offset + size].clone_from_slice(bytes);
        Ok(())
    }

    /// get_ptr_from_offset Fetches a slice of bytes from certain offset and of certain size.
    pub fn get_ptr_from_offset(&self, offset: usize, size: usize) -> &[u8] {
        &self.data[offset..offset + size]
    }

    // gets the next leaf pointer for leaf nodes

    pub fn get_next_leaf_pointer(&self, is_leaf: bool) -> &[u8] {
        let next =
            &self.data[NEXT_LEAF_POINTER_OFFSET..NEXT_LEAF_POINTER_OFFSET + NEXT_LEAF_POINTER_SIZE];
        next
    }
    /// get_data returns the underlying array.
    pub fn get_data(&self) -> [u8; PAGE_SIZE] {
        self.data
    }
}

/// Implement TryFrom<Box<Node>> for Page allowing for easier
/// serialization of data from a Node to an on-disk formatted page.
///
impl TryFrom<&Node> for Page {
    type Error = Error;
    fn try_from(node: &Node) -> Result<Self, Self::Error> {
        let mut data: [u8; PAGE_SIZE] = [0x00; PAGE_SIZE];

        // is_root byte
        data[IS_ROOT_OFFSET] = node.is_root.to_byte();

        // node_type byte
        data[NODE_TYPE_OFFSET] = u8::from(&node.node_type);

        // parent offset

        if !node.is_root {
            match node.parent_id {
                Some(PageId(parent_id)) => data
                    [PARENT_POINTER_OFFSET..PARENT_POINTER_OFFSET + PARENT_POINTER_SIZE]
                    .clone_from_slice(&parent_id.to_be_bytes()),
                // Expected an offset of an inner / leaf node.
                None => return Err(Error::UnexpectedError),
            };
        }

        match node.node_type {
            NodeType::Internal(child_offsets, keys ) => {

            }
            NodeType::Leaf(kv_pair, NextPointer ) => {

            }

            NodeType::Unexpected => return  Err(Error::UnexpectedError),

        }

        Ok(())
    }
}
