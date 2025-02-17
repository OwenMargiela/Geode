use std::{collections::HashMap, fs::File, os::unix::fs::MetadataExt};

use super::replacer::{LRUKReplacer, Replacer};

struct FdPool {
    descriptors: HashMap<u64, File>,
    replacer: LRUKReplacer<u64>,
}

impl FdPool {
    fn new(number_of_entries: usize, k: usize) -> Self {
        let replacer = LRUKReplacer::new(number_of_entries, k);

        FdPool {
            descriptors: HashMap::new(),
            replacer: replacer,
        }
    }

    // Returns the file identifier and the a potentially evicted file identifier
    fn set(&mut self, file_descriptor: File) -> (Option<u64>, Option<u64>) {
        let file_id = file_descriptor.metadata().expect("File exist").ino();

        match self.replacer.record_access(file_id) {
            Some(_) => {
                self.replacer.set_evictable(file_id, true);

                if self.descriptors.get(&file_id).is_none() {
                    self.descriptors.insert(file_id, file_descriptor);
                }

                let result_tuple = (Some(file_id), None);
                return result_tuple;
            }
            None => {
                let removed_val = self.replacer.evict().expect("Key found");
                self.descriptors.remove(&removed_val);

                self.replacer.record_access(file_id);
                return (Some(file_id), Some(removed_val));
            }
        }
    }

    fn get(&mut self, entry_id: u64) -> Option<&File> {
        match self.descriptors.get(&entry_id) {
            Some(file) => {
                self.replacer.record_access(entry_id);
                Some(file)
            }
            None => return None,
        }
    }

    // Toggles the evictability to the opposite
    // fn toggle_evictable(entry_id: u64) -> bool {}
}

#[cfg(test)]
pub mod tests {
    use std::fs::{remove_file, File};

    use crate::utils::replacer::Replacer;

    use super::FdPool;

    #[test]
    fn cache_test() {
        const MAX_SIZE: usize = 2;
        let mut fd_pool = FdPool::new(MAX_SIZE, 2);
        let file_1: File = File::create("path_1.txt").expect("File open");
        let file_2: File = File::create("path_2.txt").expect("File open");
        let file_3: File = File::create("path_3.txt").expect("File open");

        // Add six file_descriptors to the replacer. We now have frames [1, 2, 3, 4, 5, 6]

        let (id_1, _) = fd_pool.set(file_1);
        let (id_2, _) = fd_pool.set(file_2);

        fd_pool.get(id_1.unwrap());

        assert_eq!(MAX_SIZE, fd_pool.replacer.size());

        let (_, evicted_file_id_3) = fd_pool.set(file_3);

        // // Reaching the maximum size should evict the entry id_2
        assert_eq!(id_2.unwrap(), evicted_file_id_3.unwrap());

        assert_eq!(true, fd_pool.get(id_2.unwrap()).is_none());

        for i in 1..4 {
            let string = format!("path_{i}.txt");
            remove_file(string).expect("Failed to delete the test file");
        }
    }

    #[test]
    fn clean() {
        for i in 1..4 {
            let string = format!("path_{i}.txt");
            remove_file(string).expect("Failed to delete the test file");
        }
    }
}
