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

        let key_one = get_kv_vec().get(3).unwrap().clone();
        let key_two = get_kv_vec().get(0).unwrap().clone();
        let key_three = get_kv_vec().get(1).unwrap().clone();
        // let key_four = get_kv_vec().get(4).unwrap().clone();
        // let key_five = get_kv_vec().get(5).unwrap().clone();

        tree.insert(key_one).unwrap();
        tree.insert(key_two).unwrap();
        tree.insert(key_three).unwrap();
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
                value: ByteBox::varchar("Rolando", 15),
            }
        ]
    }
}
