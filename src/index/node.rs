use crate::storage::page::b_plus_tree_page::BTreePage;
use crate::storage::page::btree_page_layout::{
    FromByte, INTERNAL_NODE_HEADER_SIZE, INTERNAL_NODE_NUM_CHILDREN_OFFSET, IS_ROOT_OFFSET,
    KEY_SIZE, LEAF_NODE_HEADER_SIZE, LEAF_NODE_NUM_PAIRS_OFFSET, NODE_TYPE_OFFSET,
    PARENT_POINTER_OFFSET, PTR_SIZE, ROW_ID_SEGMENT_SIZE, VALUE_SIZE,
};

use super::errors::Error;
use super::node_type::{
    update_next_pointer, Key, KeyValuePair, NextPointer, NodeType, PageId, RowID,
};

/// Node represents a node in the BTree occupied by a single page in memory.
#[derive(Clone, Debug)]
pub struct Node {
    pub node_type: NodeType,
    pub is_root: bool,
    pub parent_id: Option<PageId>,
}

impl Node {
    pub fn new(node_type: NodeType, is_root: bool) -> Node {
        Node {
            node_type,
            is_root,
            parent_id: None,
        }
    }

    /// returns the pointer to the sibling node
    pub fn get_next_pointer(&self) -> Result<[u8; 8], Error> {
        let node_type = self.node_type.clone();

        match node_type {
            NodeType::Leaf(_, next) => match next.0 {
                Some(raw) => {
                    let mut dst = [0u8; 8];
                    dst.copy_from_slice(&raw);

                    Ok(dst)
                }
                None => Err(Error::UnexpectedError),
            },
            _ => Err(Error::UnexpectedError),
        }
    }

    /// sets the pointer to the sibling node

    pub fn set_next_pointer(&self, next_page_pointer: [u8; 8]) -> Result<(), Error> {
        let mut node_type = self.node_type.clone();

        match node_type {
            NodeType::Leaf(_, _) => {
                update_next_pointer(&mut node_type, next_page_pointer).unwrap();
                Ok(())
            }
            _ => Err(Error::UnexpectedError),
        }
    }

    /// split creates a sibling node from a given node by splitting the node in two around a median.
    /// split will split the child at b leaving the [0, b-1] keys
    /// while moving the set of [b, 2b-1] keys to the sibling.

    pub fn split(&mut self, b: usize) -> Result<(Key, Node), Error> {
        match self.node_type {
            NodeType::Internal(ref mut children, ref mut keys) => {
                // Populate siblings keys.
                let mut sibling_keys = keys.split_off(b - 1);

                // Pop median key - to be added to the parent..
                let median_key = sibling_keys.remove(0);

                // Populate siblings children.
                let sibling_children = children.split_off(b);

                Ok((
                    median_key,
                    Node::new(NodeType::Internal(sibling_children, sibling_keys), false),
                ))
            }

            NodeType::Leaf(ref mut pairs, _) => {
                // Populate siblings pairs
                let sibling_pair = pairs.split_off(b);
                // pop median key.
                let median_pair = pairs.get(b - 1).ok_or(Error::UnexpectedError)?.clone();

                Ok((
                    Key(median_pair.key),
                    Node::new(NodeType::Leaf(sibling_pair, NextPointer(None)), false),
                ))
            }

            NodeType::Unexpected => Err(Error::UnexpectedError),
        }
    }
}

impl TryFrom<BTreePage> for Node {
    type Error = Error;

    fn try_from(page: BTreePage) -> Result<Node, Self::Error> {
        let raw = page.get_data();
        let node_type = NodeType::from(raw[NODE_TYPE_OFFSET]);
        let is_root = raw[IS_ROOT_OFFSET].from_byte();
        let _parent_offset: Option<PageId>;

        if is_root {
            _parent_offset = None
        } else {
            _parent_offset = Some(PageId(page.get_value_from_offset(PARENT_POINTER_OFFSET)?));
        }

        match node_type {
            NodeType::Internal(mut children, mut keys) => {
                let num_children = page.get_value_from_offset(INTERNAL_NODE_NUM_CHILDREN_OFFSET)?;
                let mut offset = INTERNAL_NODE_HEADER_SIZE;

                for _ in 1..=num_children {
                    let child_offset = page.get_value_from_offset(offset)?;
                    children.push(PageId(child_offset));

                    offset += PTR_SIZE;
                }

                // Number of keys is always one less than the number of children (i.e. branching factor)
                for _ in 1..num_children {
                    let key_raw = page.get_ptr_from_offset(offset, KEY_SIZE);
                    offset += KEY_SIZE;

                    keys.push(Key(key_raw.try_into().unwrap()));
                }

                Ok(Node::new(NodeType::Internal(children, keys), is_root))
            }

            NodeType::Leaf(mut pairs, _) => {
                let mut offset = LEAF_NODE_NUM_PAIRS_OFFSET;
                let num_keys_val_pairs = page.get_value_from_offset(offset)?;
                offset = LEAF_NODE_HEADER_SIZE;
                let next_pointer: [u8; 8] = page.get_next_leaf_pointer().try_into().unwrap();

                for _i in 0..num_keys_val_pairs {
                    let key_raw = page.get_ptr_from_offset(offset, KEY_SIZE);

                    offset += KEY_SIZE;

                    let value_raw = page.get_ptr_from_offset(offset, VALUE_SIZE);

                    let value: [u8; 10] = value_raw.try_into().unwrap();
                    let mut page_id: [u8; 4] = [0u8; 4];
                    let mut slot_index: [u8; 4] = [0u8; 4];

                    page_id.copy_from_slice(&value[0..ROW_ID_SEGMENT_SIZE]);

                    slot_index
                        .copy_from_slice(&value[ROW_ID_SEGMENT_SIZE..ROW_ID_SEGMENT_SIZE + 4]);

                    let row_id = RowID::new(page_id, slot_index);
                    offset += VALUE_SIZE;

                    // Trim leading or trailing zeros.
                    pairs.push(KeyValuePair::new(key_raw.try_into().unwrap(), row_id))
                }

                Ok(Node::new(
                    NodeType::Leaf(pairs, NextPointer(Some(next_pointer))),
                    is_root,
                ))
            }

            NodeType::Unexpected => Err(Error::UnexpectedError),
        }
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
        storage::page::{
            b_plus_tree_page::BTreePage,
            btree_page_layout::{
                INTERNAL_NODE_HEADER_SIZE, KEY_SIZE, LEAF_NODE_HEADER_SIZE, PAGE_SIZE, PTR_SIZE,
                VALUE_SIZE,
            },
        },
    };

    #[test]
    fn page_to_node_works_for_leaf_node() -> Result<(), Error> {
        const DATA_LEN: usize = LEAF_NODE_HEADER_SIZE + KEY_SIZE + VALUE_SIZE;
        let page_data: [u8; DATA_LEN] = [
            0x01, // Is-Root byte.
            0x02, // Leaf Node type byte.
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Parent id.
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // Number of Key-Value pairs.
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // Next leaf pointer size
            0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x00, 0x00, 0x00, 0x00, 0x00, // "hello"
            0x77, 0x6f, 0x72, 0x6c, 0x64, 0x00, 0x00, 0x00, 0x00, 0x00, // "world"
        ];

        let junk: [u8; PAGE_SIZE - DATA_LEN] = [0x00; PAGE_SIZE - DATA_LEN];
        let mut page = [0x00; PAGE_SIZE];
        for (to, from) in page.iter_mut().zip(page_data.iter().chain(junk.iter())) {
            *to = *from
        }

        let btree = BTreePage::new(page);

        let node = Node::try_from(btree).unwrap();

        let next_pointer = node.get_next_pointer().unwrap();
        assert_eq!(next_pointer, [0, 0, 0, 0, 0, 0, 0, 1]);

        assert_eq!(node.is_root, true);

        Ok(())
    }

    #[test]
    fn page_to_node_works_for_internal_node() -> Result<(), Error> {
        const DATA_LEN: usize = INTERNAL_NODE_HEADER_SIZE + 3 * PTR_SIZE + 2 * KEY_SIZE;
        let page_data: [u8; DATA_LEN] = [
            0x01, // Is-Root byte.
            0x01, // Internal Node type byte.
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Parent id.
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, // Number of children.
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, // 4096  (2nd Page id )
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20, 0x00, // 8192  (3rd Page id )
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x30, 0x00, // 12288 (4th Page id)
            0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x00, 0x00, 0x00, 0x00, 0x00, // "hello"
            0x77, 0x6f, 0x72, 0x6c, 0x64, 0x00, 0x00, 0x00, 0x00, 0x00, // "world"
        ];
        let junk: [u8; PAGE_SIZE - DATA_LEN] = [0x00; PAGE_SIZE - DATA_LEN];

        // Concatenate the two arrays; page_data and junk.
        let mut page = [0x00; PAGE_SIZE];
        for (to, from) in page.iter_mut().zip(page_data.iter().chain(junk.iter())) {
            *to = *from
        }

        let node = Node::try_from(BTreePage::new(page))?;

        if let NodeType::Internal(_, keys) = node.node_type {
            assert_eq!(keys.len(), 2);

            let Key(first_key) = match keys.get(0) {
                Some(key) => key,
                None => return Err(Error::UnexpectedError),
            };

            let key_string = String::from_utf8_lossy(first_key).into_owned();
            let value = key_string.trim_matches('\0');

            assert_eq!(value, "hello");

            let Key(second_key) = match keys.get(1) {
                Some(key) => key,
                None => return Err(Error::UnexpectedError),
            };

            let key_string = String::from_utf8_lossy(second_key).into_owned();
            let value = key_string.trim_matches('\0');

            assert_eq!(value, "world");

            return Ok(());
        }

        Err(Error::UnexpectedError)
    }

    #[test]
    fn split_leaf_works() -> Result<(), Error> {
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

        let kv_pair_one = KeyValuePair::new(key, RowID::new([0u8; 4], [0u8; 4]));
        let kv_pair_two = KeyValuePair::new(key_two, RowID::new([0u8; 4], [1u8; 4]));
        let kv_pair_three = KeyValuePair::new(key_three, RowID::new([0u8; 4], [2u8; 4]));

        let mut node = Node::new(
            NodeType::Leaf(
                vec![kv_pair_one, kv_pair_two, kv_pair_three],
                NextPointer(Some([0, 0, 0, 0, 0, 0, 0, 1])),
            ),
            true,
        );

        let (median, sibling) = node.split(2)?;

        assert_eq!(median, Key([3, 0, 0, 0, 0, 0, 0, 0, 0, 0]));

        assert_eq!(
            node.node_type,
            NodeType::Leaf(
                vec![
                    KeyValuePair::new(key, RowID::new([0u8; 4], [0u8; 4])),
                    KeyValuePair::new(key_two, RowID::new([0u8; 4], [1u8; 4]))
                ],
                NextPointer(Some([0, 0, 0, 0, 0, 0, 0, 1])),
            )
        );

        assert_eq!(
            sibling.node_type,
            NodeType::Leaf(
                vec![KeyValuePair::new(key_three, RowID::new([0u8; 4], [2u8; 4]))],
                NextPointer(None)
            )
        );

        Ok(())
    }

    #[test]

    fn split_internal_works() -> Result<(), Error> {
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

        let mut node = Node::new(
            NodeType::Internal(
                vec![PageId(1), PageId(2), PageId(3), PageId(4)],
                vec![Key(key), Key(key_two), Key(key_three)],
            ),
            true,
        );

        let (median, sibling) = node.split(2)?;

        assert_eq!(median, Key(key_two));

        assert_eq!(
            node.node_type,
            NodeType::Internal(vec![PageId(1), PageId(2)], vec![Key(key)])
        );
        assert_eq!(
            sibling.node_type,
            NodeType::Internal(vec![PageId(3), PageId(4)], vec![Key(key_three)])
        );

        Ok(())
    }
}
