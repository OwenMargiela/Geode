#[cfg(test)]

mod tests {
    use crate::{
        index::{
            errors::Error,
            node::Node,
            node_type::{Key, NodeType, PageId},
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
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // id.
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // Number of Key-Value pairs.
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, // Next leaf pointer size
            0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x00, 0x00, 0x00, 0x00, 0x00, // "hello"
            0x77, 0x6f, 0x72, 0x6c, 0x64, 0x00, 0x00, 0x00, 0x00, 0x00, // "world"
        ];

        let junk: [u8; PAGE_SIZE - DATA_LEN] = [0x00; PAGE_SIZE - DATA_LEN];
        let mut page = [0x00; PAGE_SIZE];
        for (to, from) in page.iter_mut().zip(page_data.iter().chain(junk.iter())) {
            *to = *from
        }

        let btree = BTreePage::new(page);

        let mut node = Node::try_from(btree).unwrap();

        let next_pointer = node.get_next_pointer().unwrap();
        let cur_page_pointer = node.get_pointer()?;

        assert_eq!(cur_page_pointer, PageId(0));
        assert_eq!(next_pointer, [0, 0, 0, 0, 0, 0, 0, 7]);
        assert_eq!(node.is_root, true);

        node.set_next_pointer([0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08])?;
        let next_pointer = node.get_next_pointer().unwrap();

        println!("{:?}", next_pointer);

        Ok(())
    }

    #[test]
    fn page_to_node_works_for_internal_node() -> Result<(), Error> {
        const DATA_LEN: usize = INTERNAL_NODE_HEADER_SIZE + 3 * PTR_SIZE + 2 * KEY_SIZE;
        let page_data: [u8; DATA_LEN] = [
            0x01, // Is-Root byte.
            0x01, // Internal Node type byte.
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // id.
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

        if let NodeType::Internal(_, keys, current_page) = node.node_type {
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

            assert_eq!(current_page, PageId(0));

            return Ok(());
        }

        Err(Error::UnexpectedError)
    }
}
