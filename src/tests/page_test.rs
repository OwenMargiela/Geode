#[cfg(test)]

pub mod test {
    use std::{
        fs::{remove_dir_all, remove_file},
        path::PathBuf,
    };

    use crate::{
        buffer::buffer_pool_manager::BufferPoolManager,
        storage::{disk::manager::Manager, page::page::page_constants::PAGE_SIZE},
    };

    const NUM_FRAMES: usize = 10;
    const K_DIST: usize = 2;

    #[test]

    fn disable_drop_test() {
        let (log_io, log_file_path) = Manager::open_log();

        let manager = Manager::new(log_io, log_file_path);

        let mut bpm = BufferPoolManager::new(NUM_FRAMES, manager, K_DIST);

        let file_id = bpm.allocate_file();

        {
            let pid_0 = bpm.new_page(file_id);
            let guard = bpm.write_page(file_id, pid_0);

            assert_eq!(1, bpm.get_pin_count(file_id, pid_0));
            drop(guard);

            assert_eq!(0, bpm.get_pin_count(file_id, pid_0));
        }

        let pid_1 = bpm.new_page(file_id);
        let pid_2 = bpm.new_page(file_id);

        {
            let read_guard = bpm.read_page(file_id, pid_1);
            assert_eq!(1, bpm.get_pin_count(file_id, pid_1));
            drop(read_guard);

            let write_guard = bpm.write_page(file_id, pid_2);
            assert_eq!(1, bpm.get_pin_count(file_id, pid_2));
            drop(write_guard);
        }

        {
            let _write_test1 = bpm.write_page(file_id, pid_1);
            let _write_test2 = bpm.read_page(file_id, pid_1);
            let _write_test_3 = bpm.read_page(file_id, pid_1);
        }

        let mut page_ids: Vec<u32> = Vec::new();

        {
            // Fill up the BPM.
            for _ in 0..NUM_FRAMES {
                let pid = bpm.new_page(file_id);
                let _ = bpm.write_page(file_id, pid);

                page_ids.push(pid);
            }
        } // This drops all of the guards.

        for i in 0..NUM_FRAMES {
            let pin = bpm.get_pin_count(file_id, page_ids[i]);
            assert_eq!(0, pin);
        }

        // Get a new write page and edit it. We will retrieve it later

        let mutable_page_id = bpm.new_page(file_id);
        println!("Mutable Page id  = {}", mutable_page_id);

        {
            // Writing Data
            {
                let mutable_guard = bpm.write_page(file_id, mutable_page_id);
                let frame_data = &mut mutable_guard.get_frame().data;

                if frame_data.len() != PAGE_SIZE {
                    *frame_data = [0u8; PAGE_SIZE]; // Reallocate with correct size
                }

                let new_page_data = vec![1; PAGE_SIZE].into_boxed_slice();

                frame_data.copy_from_slice(&new_page_data);
            }
        }

        {
            // Reading Data
            let mutable_guard = bpm.read_page(file_id, mutable_page_id);
            let frame_data = &mutable_guard.get_frame().data;

            assert_eq!(frame_data, &[1; PAGE_SIZE]);
        }

        // Data persistence check

        // Fill up the BPM again.
        for _ in 0..NUM_FRAMES {
            let pid = bpm.new_page(file_id);
            let _ = bpm.write_page(file_id, pid);

            page_ids.push(pid);
        }

        println!("Mutable Page id  = {}", mutable_page_id);

        {
            let immutable_guard = bpm.read_page(file_id, mutable_page_id);

            assert_eq!(&immutable_guard.get_frame().data, &[1; PAGE_SIZE]);
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
