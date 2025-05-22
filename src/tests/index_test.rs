#[cfg(test)]
pub mod test {
    use std::{ fs::{ remove_dir_all, remove_file }, path::PathBuf, sync::Arc };

    use crate::{
        buffer::buffer_pool_manager::BufferPoolManager,
        index::{ btree::BTreeBuilder, node_type::{ Key, KeyValuePair, RowID } },
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
            .build("test".to_string(), Arc::new(bpm))
            .unwrap();
        let celing = 30;
        let keys = vec![
            10,
            20,
            30,
            40,
            50,
            60,
            70,
            80,
            90,
            100,
            110,
            120,
            130,
            140,
            150,
            160,
            170,
            180,
            190,
            200,
            210,
            220,
            230,
            240,
            250,
            260,
            270,
            280,
            290,
            300
        ];
        for (i, key) in keys.iter().enumerate() {
            index.insert(KeyValuePair::new(*key, RowID::new(0, i.try_into().unwrap()))).unwrap();
        }
        index.print();

        // for i in 0..celing {
        //     let entry = KeyValuePair::new(i + 1, RowID::new(0, i.try_into().unwrap()));
        //     let key = Key(entry.key);
        //     assert_eq!(index.get_entry(key.clone()).unwrap(), entry);
        // }

        teardown();
    }

    #[test]
    fn deletion_works() {
        let (log_io, log_file_path) = Manager::open_log();
        let manager = Manager::new(log_io, log_file_path);

        let bpm = BufferPoolManager::new(NUM_FRAMES, manager, K_DIST);

        let index = BTreeBuilder::new()
            .b_parameter(2)
            .build("test".to_string(), Arc::new(bpm))
            .unwrap();
        let celing = 50;

        for i in 0..celing {
            index.insert(KeyValuePair::new(i + 1, RowID::new(0, i.try_into().unwrap()))).unwrap();
        }

        index.print();

        for i in 1..celing + 1 {
            index.delete(&Key::new(i)).unwrap();
            let key_nine = index.find_node(&Key::new(i));
            assert_eq!(key_nine.is_err(), true);
        }
    }

    #[test]
    fn teardown() {
        let log_file_path = PathBuf::from("log_file_path.bin");
        let db_path = PathBuf::from("geodeData");
        remove_file(log_file_path).unwrap();
        remove_dir_all(db_path).unwrap();
    }
}
