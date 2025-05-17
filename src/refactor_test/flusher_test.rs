#[cfg(test)]

pub mod test {

    use std::{
        collections::VecDeque,
        fs::{remove_dir_all, remove_file},
        path::PathBuf,
        sync::Arc,
    };

    use crate::{
        buffer::{buffer_pool_manager::BufferPoolManager, flusher::Flusher},
        index::tree::tree_page::tree_page_layout::PAGE_SIZE,
        storage::disk::manager::Manager,
    };

    const NUM_FRAMES: usize = 10;
    const K_DIST: usize = 2;

    #[test]

    fn flush_test() {
        let (log_io, log_file_path) = Manager::open_log();

        let manager = Manager::new(log_io, log_file_path);

        let bpm = BufferPoolManager::new(NUM_FRAMES, manager, K_DIST);
        let file_id = bpm.allocate_file();

        let flusher = Flusher::new(Arc::new(bpm), file_id);
        let mut deque: VecDeque<u32> = VecDeque::new();

        let page_1 = flusher.new_page();
        let page_2 = flusher.new_page();
        let page_3 = flusher.new_page();
        let page_4 = flusher.new_page();

        deque.push_front(page_1);
        deque.push_front(page_2);
        deque.push_front(page_3);

        flusher.aqquire_context_ex(deque.clone()).unwrap();

        flusher.pop_flush_test([3u8; PAGE_SIZE]).unwrap();
        flusher.pop_flush_test([2u8; PAGE_SIZE]).unwrap();
        flusher.pop_flush_test([1u8; PAGE_SIZE]).unwrap();

        let _page_data_three = flusher.read_drop(page_3);
        let _page_data_two = flusher.read_drop(page_2);
        let _page_data = flusher.read_drop(page_1);

        assert_eq!(_page_data_three, [3u8; PAGE_SIZE]);
        assert_eq!(_page_data_two, [2u8; PAGE_SIZE]);
        assert_eq!(_page_data, [1u8; PAGE_SIZE]);

        flusher.write_flush([4u8; PAGE_SIZE], page_4).unwrap();
        let _page_data_four = flusher.read_drop(page_4);
        assert_eq!(_page_data_four, [4u8; PAGE_SIZE]);

        flusher.aqquire_context_ex(deque).unwrap();

        let _page_data_top =  flusher.read_top().unwrap();

        assert_eq!(_page_data_top, _page_data_three);

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
