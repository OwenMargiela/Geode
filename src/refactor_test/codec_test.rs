#[cfg(test)]
pub mod test {
    use crate::index::tree::{
        byte_box::{ ByteBox, DataType },
        index_types::NodeKey,
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
}
