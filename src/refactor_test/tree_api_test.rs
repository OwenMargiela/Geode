#[cfg(test)]
pub mod test {
    use crate::index::tree::{
        byte_box::{ ByteBox, DataType },
        db::btree_obj::BTreeBuilder,
        index_types::KeyValuePair,
        tree_page::codec::Codec,
    };

    #[test]
    fn refactor_insert_works() {
        let tree = BTreeBuilder::new()
            .b_parameter(2)
            .tree_schema(Codec {
                key_type: DataType::SmallInt,
                value_type: DataType::Varchar(15),
            })
            .build()
            .unwrap();

        let key_vec = get_kv_vec();

        for (idx, key) in key_vec.into_iter().enumerate() {
            tree.insert(key).unwrap();

            if idx == 9 {
                break;
            }
        }

        tree.print();
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
            }
        ]
    }
}
