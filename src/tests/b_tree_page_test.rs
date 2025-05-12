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
                PageId(2_u32.try_into().unwrap()),
            ),
            PageId(2_u32.try_into().unwrap()),
            true,
        );

        // Serialize data
        let page = BTreePage::try_from(&some_leaf)?;

        let data = Node::try_from(page)?;

        assert_eq!(data.is_root, some_leaf.is_root);
        assert_eq!(data.node_type, some_leaf.node_type);
        assert_eq!(data.pointer, some_leaf.pointer);

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
                PageId(2_u32.try_into().unwrap()),
            ),
            PageId(2_u32.try_into().unwrap()),
            true,
        );

        // Serialize data
        let page = BTreePage::try_from(&node)?;

        // Deserialize back the page
        let res = Node::try_from(page)?;
        assert_eq!(res.is_root, node.is_root);
        assert_eq!(res.node_type, node.node_type);
        assert_eq!(res.pointer, node.pointer);

        Ok(())
    }
}
