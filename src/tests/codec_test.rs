#[cfg(test)]
pub mod test {
    use crate::index::tree::{
        byte_box::{ ByteBox, DataType },
        index_types::{ KeyValuePair, NodeKey },
        tree_node::{ node_type::NodeType, tree_node_inner::NodeInner },
        tree_page::codec::Codec,
    };

    #[test]
    fn codec_works_internal() {
        let codec = Codec {
            key_type: DataType::SmallInt,
            value_type: DataType::Varchar(15),
        };

        let key_one = NodeKey::GuidePost(ByteBox::small_int(10));
        let key_two = NodeKey::GuidePost(ByteBox::small_int(20));
        let key_three = NodeKey::GuidePost(ByteBox::small_int(30));

        let tree_node = NodeInner::new(
            NodeType::Internal(vec![1, 2, 3, 4], vec![key_one, key_two, key_three], u32::default()),
            false,
            u32::default(),
            None
        );

        let page = Codec::encode(&tree_node).unwrap();
        let node = codec.decode(&page).unwrap();

        assert_eq!(node, tree_node);
    }

    #[test]
    fn codec_works_leaf() {
        let codec = Codec {
            key_type: DataType::SmallInt,
            value_type: DataType::Varchar(15),
        };

        let kv_vec = get_kv_vec();

        let leaf_node = NodeInner::new(
            NodeType::Leaf(kv_vec, u32::default(), None),
            false,
            u32::default(),
            None
        );

        let page = Codec::encode(&leaf_node).unwrap();
        let node = codec.decode(&page).unwrap();
        assert_eq!(node, leaf_node);
    }

    fn get_kv_vec() -> Vec<NodeKey> {
        vec![
            NodeKey::KeyValuePair(KeyValuePair {
                key: ByteBox::small_int(10),
                value: ByteBox::varchar("Andre", 15),
            }),
            NodeKey::KeyValuePair(KeyValuePair {
                key: ByteBox::small_int(20),
                value: ByteBox::varchar("Akio", 15),
            }),
            NodeKey::KeyValuePair(KeyValuePair {
                key: ByteBox::small_int(30),
                value: ByteBox::varchar("Chaque", 15),
            }),
            NodeKey::KeyValuePair(KeyValuePair {
                key: ByteBox::small_int(40),
                value: ByteBox::varchar("Te'juan", 15),
            }),
            NodeKey::KeyValuePair(KeyValuePair {
                key: ByteBox::small_int(50),
                value: ByteBox::varchar("Rolando", 15),
            })
        ]
    }
}
