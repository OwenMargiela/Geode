use crate::index::{
    errors::Error,
    node::Node,
    node_type::{Key, NodeType, PageId},
};

use super::{
    btree_page_layout::{
        ToByte, INTERNAL_NODE_HEADER_SIZE, INTERNAL_NODE_NUM_CHILDREN_OFFSET,
        INTERNAL_NODE_NUM_CHILDREN_SIZE, IS_ROOT_OFFSET, KEY_SIZE, LEAF_NODE_HEADER_SIZE,
        LEAF_NODE_NUM_PAIRS_OFFSET, LEAF_NODE_NUM_PAIRS_SIZE, NEXT_LEAF_POINTER_OFFSET,
        NEXT_LEAF_POINTER_SIZE, NODE_TYPE_OFFSET, PARENT_POINTER_OFFSET, PARENT_POINTER_SIZE,
        PTR_SIZE, VALUE_SIZE,
    },
    page::page_constants::PAGE_SIZE,
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

    pub fn get_next_leaf_pointer(&self) -> &[u8] {
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
impl TryFrom<&Node> for BTreePage {
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

        match &node.node_type {
            NodeType::Internal(child_ids, keys) => {
                data[INTERNAL_NODE_NUM_CHILDREN_OFFSET
                    ..INTERNAL_NODE_NUM_CHILDREN_OFFSET + INTERNAL_NODE_NUM_CHILDREN_SIZE]
                    .clone_from_slice(&child_ids.len().to_be_bytes());

                let mut page_offset = INTERNAL_NODE_HEADER_SIZE;
                for PageId(child_pointer) in child_ids {
                    data[page_offset..page_offset + PTR_SIZE]
                        .clone_from_slice(&child_pointer.to_be_bytes());

                    page_offset += PTR_SIZE;
                }

                for Key(key) in keys {
                    if key.len() > KEY_SIZE {
                        return Err(Error::KeyOverflowError);
                    } else {
                        data[page_offset..page_offset + KEY_SIZE].clone_from_slice(key);
                        page_offset += KEY_SIZE
                    }
                }
            }
            NodeType::Leaf(kv_pairs, next_pointer) => {
                // num of pairs
                data[LEAF_NODE_NUM_PAIRS_OFFSET
                    ..LEAF_NODE_NUM_PAIRS_OFFSET + LEAF_NODE_NUM_PAIRS_SIZE]
                    .clone_from_slice(&kv_pairs.len().to_be_bytes());

                match next_pointer.0 {
                    Some(next_pointer) => {
                        data[NEXT_LEAF_POINTER_OFFSET
                            ..NEXT_LEAF_POINTER_OFFSET + NEXT_LEAF_POINTER_SIZE]
                            .clone_from_slice(&next_pointer);
                    }

                    _ => {}
                }

                let mut page_offset = LEAF_NODE_HEADER_SIZE;

                for pair in kv_pairs {
                    if pair.key.len() > KEY_SIZE {
                        return Err(Error::KeyOverflowError);
                    }

                    let raw_key = pair.key;
                    data[page_offset..page_offset + KEY_SIZE].clone_from_slice(&raw_key);
                    page_offset += KEY_SIZE;

                    let mut value = Vec::with_capacity(VALUE_SIZE - 2);
                    value.extend(pair.value.page_id);
                    value.extend(pair.value.slot_index);
                    value.extend([0, 0]);

                    if value.len() > VALUE_SIZE {
                        return Err(Error::ValueOverflowError);
                    }

                    data[page_offset..page_offset + VALUE_SIZE].clone_from_slice(&value);
                    page_offset += VALUE_SIZE;
                }
            }

            NodeType::Unexpected => return Err(Error::UnexpectedError),
        }

        Ok(BTreePage::new(data))
    }
}

#[cfg(test)]

mod tests {

    use crate::{
        index::{
            errors::Error,
            node::Node,
            node_type::{Key, KeyValuePair, NextPointer, NodeType, PageId, RowID},
        },
        storage::page::b_plus_tree_page::BTreePage,
    };

    #[test]
    fn node_to_page_works_for_leaf_node() -> Result<(), Error> {

        let kv_pair_one = KeyValuePair::new(42, RowID::new(0, 0));
        let kv_pair_two = KeyValuePair::new(3, RowID::new(0, 1));
        let kv_pair_three = KeyValuePair::new(8, RowID::new(0, 2));

        let some_leaf = Node::new(
            NodeType::Leaf(
                vec![kv_pair_one, kv_pair_two, kv_pair_three],
                NextPointer(Some([0, 0, 0, 0, 0, 0, 0, 1])),
            ),
            true,
        );

        // Serialize data
        let page = BTreePage::try_from(&some_leaf)?;

        let data = Node::try_from(page)?;

        assert_eq!(data.is_root, some_leaf.is_root);
        assert_eq!(data.node_type, some_leaf.node_type);
        assert_eq!(data.parent_id, some_leaf.parent_id);

        Ok(())
    }

    #[test]
    
    fn node_to_page_works_for_internal_node() -> Result<(), Error> {
        fn serialize_int_to_10_bytes(n: u64) -> [u8; 10] {
            let mut bytes = [0u8; 10]; // Create a 10-byte array initialized with zeros.
            let n_bytes = n.to_le_bytes(); // Convert the integer to bytes (little-endian).

            // Copy the integer bytes into the fixed-size array (first 8 bytes for u64).
            bytes[..n_bytes.len()].copy_from_slice(&n_bytes);

            bytes
        }

        let key = serialize_int_to_10_bytes(42);
        let key_two = serialize_int_to_10_bytes(3);
        let key_three = serialize_int_to_10_bytes(8);

        let node = Node::new(
            NodeType::Internal(
                vec![PageId(1), PageId(2), PageId(3), PageId(4)],
                vec![Key(key), Key(key_two), Key(key_three)],
            ),
            true,
        );

        // Serialize data
        let page = BTreePage::try_from(&node)?;

        // Deserialize back the page
        let res = Node::try_from(page)?;

        assert_eq!(res.is_root, node.is_root);
        assert_eq!(res.node_type, node.node_type);
        assert_eq!(res.parent_id, node.parent_id);

        Ok(())
    }
}
