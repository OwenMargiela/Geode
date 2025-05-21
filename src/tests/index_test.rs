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
        let celing = 16;

        for i in 1..celing {
            index.insert(KeyValuePair::new(i * 10, RowID::new(0, i.try_into().unwrap()))).unwrap();
        }
        index.print();

        for i in 1..celing {
            let entry = KeyValuePair::new(i * 10, RowID::new(0, i.try_into().unwrap()));
            let key = Key(entry.key);
            assert_eq!(index.get_entry(key.clone()).unwrap(), entry);
        }

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
        let celing = 16;

        for i in 1..celing {
            index.insert(KeyValuePair::new(i * 10, RowID::new(0, i.try_into().unwrap()))).unwrap();
        }

        for i in 1..celing {
            println!("Key {:?}", i * 10);

            index.delete(&Key::new(i * 10)).unwrap();
            let key_nine = index.find_node(&Key::new(i + 10));
            assert_eq!(key_nine.is_err(), true);

            if i == 9 {
                break;
            }
        }
        index.print();
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
