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
            errors::Error,
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
        index.print();

        for i in 0..celing {
            let entry = KeyValuePair::new(i + 1, RowID::new(0, i.try_into().unwrap()));
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
            .build("test".to_string(), bpm)
            .unwrap();
        let celing = 40;

        for i in 0..celing {
            index
                .insert(KeyValuePair::new(
                    i + 1,
                    RowID::new(0, i.try_into().unwrap()),
                ))
                .unwrap();
        }

        for i in 1..celing + 1  {
            println!("\n\n\n Deleting {}", i);
            index.delete(&Key::new(i)).unwrap();
            let key_nine = index.find_node(&Key::new(i));
            assert_eq!(key_nine.is_err(), true);
        }

        index.print();
    }

    #[test]
    fn teardown() {
        let log_file_path = PathBuf::from("log_file_path.bin");
        let db_path = PathBuf::from("geodeData");
        remove_file(log_file_path).unwrap();
        remove_dir_all(db_path).unwrap();
    }
}
