#[cfg(test)]
pub mod test {
    use std::{
        fs::{remove_dir_all, remove_file},
        path::PathBuf,
        sync::{Arc, Mutex},
    };

    use crate::storage::{
        disk::{
            manager::Manager,
            scheduler::{DiskData, DiskRequest, DiskScheduler},
        },
        page::page::page_constants::PAGE_SIZE,
    };

    #[tokio::main]
    #[test]
    async fn scheduler_test() {
        let (log_file, log_file_path) = Manager::open_log();
        let manager = Arc::new(Mutex::new(Manager::new(log_file, log_file_path)));
        let scheduler = DiskScheduler::new(Arc::clone(&manager));
        let future_one = scheduler.create_future();

        let (file_id, _) = scheduler
            .manager
            .lock()
            .unwrap()
            .create_db_file()
            .expect("File made");

        // Lock contention allow for functionality to check if a page needs to be allocated thats automatic
        let mut gurad = scheduler.manager.lock().unwrap();
        let (page_id, _) = gurad.allocate_page(file_id);
        drop(gurad);

        // Write Request
        let data = [1; PAGE_SIZE];
        let page_data = Manager::aligned_buffer(&data);

        let request = DiskRequest {
            data: DiskData::Write(Some(page_data)), // Move the buffer
            done_flag: Arc::clone(&future_one.flag),
            file_id,
            is_write: true,
            page_id,
            waker: Arc::clone(&future_one.waker),
        };

        scheduler.schedule(request);

        // Read Request
        let future_two = scheduler.create_future();
        let page_buffer = Arc::new(Mutex::new(Manager::aligned_buffer(&vec![0; PAGE_SIZE])));

        let request = DiskRequest {
            data: DiskData::Read(Some(Arc::clone(&page_buffer))), // Shared buffer reference
            done_flag: Arc::clone(&future_two.flag),
            file_id,
            is_write: false,
            page_id,
            waker: Arc::clone(&future_two.waker),
        };

        scheduler.schedule(request);

        future_one.await;
        future_two.await;

        // Verify Read &Write
        let read_data = page_buffer.lock().unwrap();

        assert_eq!(&**read_data, &data, "Page read mismatch!");

        teardown();
    }

    fn teardown() {
        let log_file_path = PathBuf::from("log_file_path.bin");
        let db_path = PathBuf::from("geodeData");
        remove_file(log_file_path).unwrap();
        remove_dir_all(db_path).unwrap();
    }
}
