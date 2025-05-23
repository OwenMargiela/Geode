#[cfg(test)]
pub mod test {
    use std::{ fs::{ remove_dir_all, remove_file }, path::PathBuf, sync::Arc };

    use crate::index::tree::{
        byte_box::{ ByteBox, DataType },
        db::btree_obj::BTreeBuilder,
        index_types::{ KeyValuePair, NodeKey },
        iterators::leaf_iterator::LeafIterator,
        tree_page::codec::Codec,
    };

    #[test]
    fn leaf_iterator_test() {
        let tree = BTreeBuilder::new()
            .b_parameter(2)
            .tree_schema(Codec {
                key_type: DataType::SmallInt,
                value_type: DataType::Varchar(15),
            })
            .build()
            .unwrap();

        let key_vec = get_kv_vec();

        for (_, key) in key_vec.into_iter().enumerate() {
            tree.insert(key.clone()).unwrap();
        }

        let page = tree.find_node(NodeKey::GuidePost(ByteBox::small_int(10))).unwrap();

        let leaf_iter = LeafIterator::create_and_seek_to_first(
            Arc::clone(&tree.flusher),
            Arc::new(page),
            tree.codec,
            tree.index_id
        );

        for entry in leaf_iter.into_iter() {
            println!("{:?}", entry);
        }

        teardown();
    }

    #[test]
    fn tree_range_test() {
        let tree = BTreeBuilder::new()
            .b_parameter(2)
            .tree_schema(Codec {
                key_type: DataType::SmallInt,
                value_type: DataType::Varchar(15),
            })
            .build()
            .unwrap();

        let key_vec = get_kv_vec();

        for (_, key) in key_vec.into_iter().enumerate() {
            tree.insert(key.clone()).unwrap();
        }


        let iter = tree.scan(..);

        for entry in iter.into_iter() {
            println!("Entry {:?}", entry);
        }
    }

    fn teardown() {
        let log_file_path = PathBuf::from("log_file_path.bin");
        let db_path = PathBuf::from("geodeData");
        remove_file(log_file_path).unwrap();
        remove_dir_all(db_path).unwrap();
    }

    fn get_kv_vec() -> Vec<KeyValuePair> {
        vec![
            KeyValuePair {
                key: ByteBox::small_int(10),
                value: ByteBox::varchar("Andre", 15),
            },
            KeyValuePair {
                key: ByteBox::small_int(20),
                value: ByteBox::varchar("Akio", 15),
            },
            KeyValuePair {
                key: ByteBox::small_int(30),
                value: ByteBox::varchar("Chaque", 15),
            },
            KeyValuePair {
                key: ByteBox::small_int(40),
                value: ByteBox::varchar("Te'juan", 15),
            },
            KeyValuePair {
                key: ByteBox::small_int(50),
                value: ByteBox::varchar("Tarique", 15),
            },
            KeyValuePair {
                key: ByteBox::small_int(60),
                value: ByteBox::varchar("Tai", 15),
            },
            KeyValuePair {
                key: ByteBox::small_int(70),
                value: ByteBox::varchar("Chrishane", 15),
            },
            KeyValuePair {
                key: ByteBox::small_int(80),
                value: ByteBox::varchar("Darren", 15),
            },
            KeyValuePair {
                key: ByteBox::small_int(90),
                value: ByteBox::varchar("Justin", 15),
            },
            KeyValuePair {
                key: ByteBox::small_int(100),
                value: ByteBox::varchar("Karson", 15),
            },
            KeyValuePair {
                key: ByteBox::small_int(110),
                value: ByteBox::varchar("Marc", 15),
            },
            KeyValuePair {
                key: ByteBox::small_int(120),
                value: ByteBox::varchar("Dominigga", 15),
            },
            KeyValuePair {
                key: ByteBox::small_int(130),
                value: ByteBox::varchar("Sarah", 15),
            },
            KeyValuePair {
                key: ByteBox::small_int(140),
                value: ByteBox::varchar("Megatron", 15),
            },
            KeyValuePair {
                key: ByteBox::small_int(150),
                value: ByteBox::varchar("Headcliff", 15),
            },
            KeyValuePair {
                key: ByteBox::small_int(160),
                value: ByteBox::varchar("Stephanie", 15),
            },
            KeyValuePair {
                key: ByteBox::small_int(170),
                value: ByteBox::varchar("Tyresse", 15),
            },
            KeyValuePair {
                key: ByteBox::small_int(180),
                value: ByteBox::varchar("Riven", 15),
            },
            KeyValuePair {
                key: ByteBox::small_int(190),
                value: ByteBox::varchar("Annie", 15),
            },
            KeyValuePair {
                key: ByteBox::small_int(200),
                value: ByteBox::varchar("Diamond", 15),
            },
            KeyValuePair {
                key: ByteBox::small_int(210),
                value: ByteBox::varchar("Aneesia", 15),
            },
            KeyValuePair {
                key: ByteBox::small_int(220),
                value: ByteBox::varchar("Ashna", 15),
            },
            KeyValuePair {
                key: ByteBox::small_int(230),
                value: ByteBox::varchar("Leah", 15),
            },
            KeyValuePair {
                key: ByteBox::small_int(240),
                value: ByteBox::varchar("Matthew", 15),
            },
            KeyValuePair {
                key: ByteBox::small_int(250),
                value: ByteBox::varchar("Nashua", 15),
            },
            KeyValuePair {
                key: ByteBox::small_int(260),
                value: ByteBox::varchar("Van", 15),
            },
            KeyValuePair {
                key: ByteBox::small_int(270),
                value: ByteBox::varchar("Gabby", 15),
            },
            KeyValuePair {
                key: ByteBox::small_int(280),
                value: ByteBox::varchar("Tarun", 15),
            },
            KeyValuePair {
                key: ByteBox::small_int(290),
                value: ByteBox::varchar("Abby", 15),
            },
            KeyValuePair {
                key: ByteBox::small_int(300),
                value: ByteBox::varchar("Roshaun", 15),
            }
        ]
    }
}
