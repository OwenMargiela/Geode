#[cfg(test)]
pub mod test {
    use std::{
        fs::{remove_dir_all, remove_file},
        path::PathBuf,
    };

    use crate::{
        buffer::buffer_pool_manager::BufferPoolManager,
        storage::{disk::manager::Manager, page::page_constants::PAGE_SIZE},
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
                    *frame_data = vec![0u8; PAGE_SIZE].into_boxed_slice(); // Reallocate with correct size
                }

                let new_page_data = vec![1; PAGE_SIZE].into_boxed_slice();

                frame_data.copy_from_slice(&new_page_data);
            }
        }

        {
            // Reading Data
            let mutable_guard = bpm.read_page(file_id, mutable_page_id);
            let frame_data = &mutable_guard.get_frame().data;

            assert_eq!(frame_data, &vec![1; PAGE_SIZE].into_boxed_slice());
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
            assert_eq!(
                &immutable_guard.get_frame().data,
                &vec![1; PAGE_SIZE].into_boxed_slice()
            );
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

/*

TEST(PageGuardTest, DISABLED_MoveTest) {
  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(FRAMES, disk_manager.get(), K_DIST);

  const auto pid0 = bpm->NewPage();
  const auto pid1 = bpm->NewPage();
  const auto pid2 = bpm->NewPage();
  const auto pid3 = bpm->NewPage();
  const auto pid4 = bpm->NewPage();
  const auto pid5 = bpm->NewPage();

  auto guard0 = bpm->ReadPage(pid0);
  auto guard1 = bpm->ReadPage(pid1);
  ASSERT_EQ(1, bpm->GetPinCount(pid0));
  ASSERT_EQ(1, bpm->GetPinCount(pid1));

  // This shouldn't change pin counts...
  auto &guard0_r = guard0;
  guard0 = std::move(guard0_r);
  ASSERT_EQ(1, bpm->GetPinCount(pid0));

  // Invalidate the old guard0 by move assignment.
  guard0 = std::move(guard1);
  ASSERT_EQ(0, bpm->GetPinCount(pid0));
  ASSERT_EQ(1, bpm->GetPinCount(pid1));

  // Invalidate the old guard0 by move construction.
  auto guard0a(std::move(guard0));
  ASSERT_EQ(0, bpm->GetPinCount(pid0));
  ASSERT_EQ(1, bpm->GetPinCount(pid1));

  auto guard2 = bpm->ReadPage(pid2);
  auto guard3 = bpm->ReadPage(pid3);
  ASSERT_EQ(1, bpm->GetPinCount(pid2));
  ASSERT_EQ(1, bpm->GetPinCount(pid3));

  // This shouldn't change pin counts...
  auto &guard2_r = guard2;
  guard2 = std::move(guard2_r);
  ASSERT_EQ(1, bpm->GetPinCount(pid2));

  // Invalidate the old guard3 by move assignment.
  guard2 = std::move(guard3);
  ASSERT_EQ(0, bpm->GetPinCount(pid2));
  ASSERT_EQ(1, bpm->GetPinCount(pid3));

  // Invalidate the old guard2 by move construction.
  auto guard2a(std::move(guard2));
  ASSERT_EQ(0, bpm->GetPinCount(pid2));
  ASSERT_EQ(1, bpm->GetPinCount(pid3));

  // This will hang if page 2 was not unlatched correctly.
  { const auto temp_guard2 = bpm->WritePage(pid2); }

  auto guard4 = bpm->WritePage(pid4);
  auto guard5 = bpm->WritePage(pid5);
  ASSERT_EQ(1, bpm->GetPinCount(pid4));
  ASSERT_EQ(1, bpm->GetPinCount(pid5));

  // This shouldn't change pin counts...
  auto &guard4_r = guard4;
  guard4 = std::move(guard4_r);
  ASSERT_EQ(1, bpm->GetPinCount(pid4));

  // Invalidate the old guard5 by move assignment.
  guard4 = std::move(guard5);
  ASSERT_EQ(0, bpm->GetPinCount(pid4));
  ASSERT_EQ(1, bpm->GetPinCount(pid5));

  // Invalidate the old guard4 by move construction.
  auto guard4a(std::move(guard4));
  ASSERT_EQ(0, bpm->GetPinCount(pid4));
  ASSERT_EQ(1, bpm->GetPinCount(pid5));

  // This will hang if page 4 was not unlatched correctly.
  { const auto temp_guard4 = bpm->ReadPage(pid4); }

  // Test move constructor with invalid that
  {
    ReadPageGuard invalidread0;
    const auto invalidread1{std::move(invalidread0)};
    WritePageGuard invalidwrite0;
    const auto invalidwrite1{std::move(invalidwrite0)};
  }

  // Test move assignment with invalid that
  {
    const auto pid = bpm->NewPage();
    auto read = bpm->ReadPage(pid);
    ReadPageGuard invalidread;
    read = std::move(invalidread);
    auto write = bpm->WritePage(pid);
    WritePageGuard invalidwrite;
    write = std::move(invalidwrite);
  }

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

}  // namespace bustub

*/
