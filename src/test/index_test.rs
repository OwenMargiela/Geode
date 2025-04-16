#[cfg(test)]

pub mod test {
    use std::{
        fs::{remove_dir_all, remove_file},
        path::PathBuf,
    };

    use crate::{
        buffer::buffer_pool_manager::BufferPoolManager,
        index::{
            btree::{self, BTreeBuilder, Context, Set},
            node_type::{Key, KeyValuePair, PageId, RowID},
        },
        storage::disk::manager::Manager,
    };
    const NUM_FRAMES: usize = 10;
    const K_DIST: usize = 2;

    #[test]

    fn insert_works() {
        let (log_io, log_file_path) = Manager::open_log();
        let manager = Manager::new(log_io, log_file_path);

        let bpm = BufferPoolManager::new(NUM_FRAMES, manager, K_DIST);

        let index = BTreeBuilder::new()
            .b_parameter(2)
            .build("test".to_string(), bpm)
            .unwrap();
        let celing = 30;

        for i in 0..celing {
            index
                .insert(KeyValuePair::new(
                    i + 1,
                    RowID::new(0, i.try_into().unwrap()),
                ))
                .unwrap();
        }

        for i in 0..celing {
            let entry = KeyValuePair::new(i + 1, RowID::new(0, i.try_into().unwrap()));
            let key = Key(entry.key);

            assert_eq!(index.get_entry(key.clone()).unwrap(), entry);
        }

        // let entry_one = KeyValuePair::new(1, RowID::new(0, 0));
        // let entry_two = KeyValuePair::new(2, RowID::new(0, 1));
        // let entry_three = KeyValuePair::new(3, RowID::new(0, 2));
        // let entry_four = KeyValuePair::new(4, RowID::new(0, 3));
        // let entry_five = KeyValuePair::new(5, RowID::new(0, 4));
        // let entry_six = KeyValuePair::new(6, RowID::new(0, 5));
        // let entry_seven = KeyValuePair::new(7, RowID::new(0, 6));
        // let entry_eight = KeyValuePair::new(8, RowID::new(0, 7));
        // let entry_nine = KeyValuePair::new(9, RowID::new(0, 9));
        // let entry_ten = KeyValuePair::new(10, RowID::new(0, 10));
        // let entry_eleven = KeyValuePair::new(11, RowID::new(0, 11));

        // let key_one = Key(entry_one.key);
        // let key_two = Key(entry_two.key);
        // let key_three = Key(entry_three.key);
        // let key_four = Key(entry_four.key);
        // let key_five = Key(entry_five.key);
        // let key_six = Key(entry_six.key);
        // let key_seven = Key(entry_seven.key);
        // let key_eight = Key(entry_eight.key);
        // let key_nine = Key(entry_nine.key);
        // let key_ten = Key(entry_ten.key);
        // let key_eleven = Key(entry_eleven.key);

        // println!("\n");
        // index.insert(entry_one).unwrap();
        // println!("\n");
        // index.insert(entry_two).unwrap();
        // println!("\n");
        // index.insert(entry_three).unwrap();
        // println!("\n");

        // assert_eq!(index.get_entry(key_one.clone()).unwrap(), entry_one);
        // assert_eq!(index.get_entry(key_two.clone()).unwrap(), entry_two);
        // assert_eq!(index.get_entry(key_three.clone()).unwrap(), entry_three);

        // index.insert(entry_four).unwrap();
        // index.insert(entry_five).unwrap();

        // assert_eq!(index.get_entry(key_one.clone()).unwrap(), entry_one);
        // assert_eq!(index.get_entry(key_two.clone()).unwrap(), entry_two);
        // assert_eq!(index.get_entry(key_three.clone()).unwrap(), entry_three);
        // assert_eq!(index.get_entry(key_four.clone()).unwrap(), entry_four);
        // assert_eq!(index.get_entry(key_five.clone()).unwrap(), entry_five);

        // index.insert(entry_six).unwrap();
        // assert_eq!(index.get_entry(key_six.clone()).unwrap(), entry_six);

        // index.insert(entry_seven).unwrap();
        // assert_eq!(index.get_entry(key_seven.clone()).unwrap(), entry_seven);

        // index.insert(entry_eight).unwrap();
        // assert_eq!(index.get_entry(key_eight.clone()).unwrap(), entry_eight);

        // index.insert(entry_nine).unwrap();
        // assert_eq!(index.get_entry(key_nine.clone()).unwrap(), entry_nine);

        // index.insert(entry_ten).unwrap();
        // assert_eq!(index.get_entry(key_ten.clone()).unwrap(), entry_ten);

        // index.insert(entry_eleven).unwrap();
        // assert_eq!(index.get_entry(key_eleven.clone()).unwrap(), entry_eleven);

        teardown();
    }

    #[test]
    fn teardown() {
        let log_file_path = PathBuf::from("log_file_path.bin");
        let db_path = PathBuf::from("geodeData");
        remove_file(log_file_path).unwrap();
        remove_dir_all(db_path).unwrap();
    }
}
