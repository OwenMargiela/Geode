#[cfg(test)]
pub mod test {

    use std::{
        fs::{remove_dir_all, remove_file},
        path::PathBuf,
    };

    use crate::storage::disk::manager::Manager;

    use crate::storage::page::page::page_constants::PAGE_SIZE;

    #[test]
    fn db_io_test() {
        let (log_file, log_file_path) = Manager::open_log();
        let mut manager = Manager::new(log_file, log_file_path);

        let data = [1; PAGE_SIZE];
        let buffer = Vec::with_capacity(PAGE_SIZE);

        let mut page_buffer = Manager::aligned_buffer(&buffer);
        let page_data = Manager::aligned_buffer(&data);

        let (file_id, _) = manager.create_db_file().expect("File made");

        let (page_id, _) = manager.allocate_page(file_id);
        manager.write_page(file_id, page_id, &page_data).unwrap();
        manager
            .read_page(file_id, page_id, &mut page_buffer)
            .expect("Failed to read page");

        assert_eq!(page_data, page_buffer, "Page read mismatch!");
        println!("{:?}", page_buffer);
        teardown();
    }

   
    fn teardown() {
        let log_file_path = PathBuf::from("log_file_path.bin");
        let db_path = PathBuf::from("geodeData");
        remove_file(log_file_path).unwrap();
        remove_dir_all(db_path).unwrap();
    }
}
