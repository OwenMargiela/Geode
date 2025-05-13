#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

#[cfg(test)]
pub mod test {
    use std::{
        fs::{remove_dir_all, remove_file},
        path::PathBuf,
        sync::Arc,
    };

    use crate::{
        buffer::buffer_pool_manager::BufferPoolManager,
        index::{
            btree::{BTree, BTreeBuilder},
            node_type::{Key, KeyValuePair, RowID},
        },
        iterators::leaf_node_iterator::LeafNodeIterator,
        storage::disk::manager::Manager,
    };

    #[test]
    fn leaf_iterator_test() {
        let (log_io, log_file_path) = Manager::open_log();
        let manager = Manager::new(log_io, log_file_path);
        let bpm = Arc::new(BufferPoolManager::new(NUM_FRAMES, manager, K_DIST));
        let tree = instantiate_tree(bpm.clone());

        let page = tree.find_node(&Key::new(1)).unwrap();

        let leaf_iter = LeafNodeIterator::create_and_seek_to_first(
            bpm.clone(),
            tree.index_file_id,
            Arc::new(page),
        );

        for entry in leaf_iter.into_iter() {
            println!("{:?}", entry);
        }

        teardown();
    }

    const NUM_FRAMES: usize = 10;
    const K_DIST: usize = 2;

    fn instantiate_tree(bpm: Arc<BufferPoolManager>) -> Arc<BTree> {
        let (log_io, log_file_path) = Manager::open_log();
        let manager = Manager::new(log_io, log_file_path);

        let index = Arc::new(
            BTreeBuilder::new()
                .b_parameter(2)
                .build("test".to_string(), bpm.clone())
                .unwrap(),
        );

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

        index.clone()
    }

    #[test]

    fn leaf_iterator_range_test() {
        let (log_io, log_file_path) = Manager::open_log();

        let manager = Manager::new(log_io, log_file_path);
        let bpm = Arc::new(BufferPoolManager::new(NUM_FRAMES, manager, K_DIST));

        let tree = instantiate_tree(bpm.clone());
        let range = ..=Key::new(26);

        let iter = tree.scan(range);

        for entry in iter.into_iter() {
            println!("{:?}", entry);
        }

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
