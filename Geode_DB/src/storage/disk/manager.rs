use std::{
    alloc::{alloc, Layout},
    collections::{HashMap, VecDeque},
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    slice,
    sync::{
        atomic::{AtomicBool, AtomicU64},
        Arc,
    },
};

use hashlink::LinkedHashMap;

use crate::{storage::page::page_constants::PAGE_SIZE, utils::fdpool::FdPool};

// eventually put all constants in a a designated file
const O_DIRECT: i32 = 0x4000;
// Make these env values
const NUMBER_OF_ENTRIES: usize = 8;
const K_TH_VALUE: usize = 2;

struct FileMetadata {
    // A mapping from page_id to its offset on disk
    // This is an in memory structure whose duty might be relieved of
    // by the page directory

    // The option is used to represent deleted/deallocated pages
    //      - The page_id and offset of 'dead' pages are recycled and given to newly
    //      allocated blocks of memory
    // Dealocated pages are recorded in the free slot list
    pages: LinkedHashMap<u32, Option<u64>>, // Per-file page mappings

    // Records the free slots in the db file if pages are deleted, indicated by offset.
    // A tuple containing the Id and offsets of 'dead' pages.
    free_slots: VecDeque<(u32, u64)>,
}

// By right the Manager should only have to keep tracking on ensuring data is written to the
// appropriate sector on disk. Maintaining its own internal mapping of pages to page_ids
// isn't the most optimal solution. However, it does provide a point from which to gain
// insights on which pages have been allocated and deallocatedy o

pub struct Manager {
    // Mapping of inode numbers to file paths
    // Might be replaced by a table within the system catalogue
    file_map: HashMap<u64, PathBuf>,
    // Pool of open file descriptors
    file_descriptors: FdPool,          // File ID → File Descriptor Pool
    files: HashMap<u64, FileMetadata>, // File ID → File Metadata (Pages & Free Slots)
    flush_logs: Arc<AtomicBool>,

    // Log File
    log_io: File,
    log_file_name: PathBuf,

    num_flushes: i32,
    num_deletes: i32,
    num_writes: i32,

    // Table monotomically increasing identifier
    mono_id: AtomicU64,
}

impl Manager {
    pub fn new(log_io: File, log_file_path: PathBuf) -> Self {
        Manager {
            file_map: HashMap::new(),
            file_descriptors: FdPool::new(NUMBER_OF_ENTRIES, K_TH_VALUE),
            files: HashMap::new(),
            flush_logs: Arc::new(AtomicBool::new(false)),
            log_io: log_io,
            log_file_name: log_file_path,
            num_flushes: 0,
            num_deletes: 0,
            num_writes: 0,
            mono_id: AtomicU64::new(0),
        }
    }

    pub fn allocate_page(&mut self, file_id: u64) -> (u32, u64) {
        // Instantiating meta data for a db file
        let file_meta = self.files.entry(file_id).or_insert(FileMetadata {
            pages: LinkedHashMap::new(),
            free_slots: VecDeque::new(),
        });

        let pages_len = file_meta.pages.len();

        // Try to pop a free slot
        if let Some((page_id, offset)) = file_meta.free_slots.pop_front() {
            file_meta.pages.replace(page_id, Some(offset));
            return (pages_len as u32, offset);
        }

        // No free slots, allocate a new page
        let offset = (pages_len * PAGE_SIZE) as u64;
        file_meta.pages.insert(pages_len as u32, Some(offset));

        (pages_len as u32, offset)
    }

    pub fn write_page(
        &mut self,
        file_id: u64,
        page_id: u32,
        page_data: &[u8],
    ) -> Result<(), String> {
        let file_meta = self
            .files
            .get_mut(&file_id)
            .ok_or_else(|| format!("File {} not found", file_id))?;

        let offset = match file_meta.pages.get(&page_id) {
            Some(page_offset) => {
                // Deleted pages are marked as None
                if let Some(offset) = &page_offset {
                    offset
                } else {
                    let (_, offset) = self.allocate_page(file_id);
                    &offset.clone()
                }
            }
            None => {
                // Page does not exist, allocate a new page. We use a free slot if available.
                return Err(String::from("Page has not been allocated"));
            }
        };

        if offset % PAGE_SIZE as u64 != 0 {
            return Err(format!(
                "Invalid write offset {} (must be 4KB aligned)",
                offset
            ));
        }

        let mut db_io = self
            .file_descriptors
            .get(file_id)
            .ok_or_else(|| format!("File descriptor for {} not found", file_id))?;

        db_io
            .seek(SeekFrom::Start(*offset))
            .map_err(|err| format!("I/O error while seeking page {}: {}", page_id, err))?;

        db_io
            .write_all(&page_data)
            .map_err(|err| format!("I/O error while writing page {}: {}", page_id, err))?;

        db_io
            .flush()
            .map_err(|err| format!("Error flushing page {}: {}", page_id, err))?;

        Ok(())
    }

    pub fn read_page(
        &mut self,
        file_id: u64,
        page_id: u32,
        page_data: &mut [u8],
    ) -> Result<(), String> {
        let file_meta = self
            .files
            .get_mut(&file_id)
            .ok_or_else(|| format!("File {} not found", file_id))?;

        let offset = match file_meta.pages.get(&page_id) {
            Some(Some(offset)) => *offset,
            Some(None) => return Err(String::from("Page has been deallocated")),
            None => return Err(String::from("Page has not been allocated")),
        };

        if offset % PAGE_SIZE as u64 != 0 {
            return Err(format!(
                "Invalid write offset {} (must be 4KB aligned)",
                offset
            ));
        }

        let mut db_io = self
            .file_descriptors
            .get(file_id)
            .ok_or_else(|| format!("File descriptor for {} not found", file_id))?;

        db_io
            .seek(SeekFrom::Start(offset))
            .map_err(|err| format!("I/O error while seeking page {}: {}", page_id, err))?;

        db_io
            .read_exact(page_data)
            .map_err(|err| format!("I/O error while read page {} : {}", page_id, err))?;

        Ok(())
    }

    pub fn delete_page(&mut self, file_id: u64, page_id: u32) -> Result<(), String> {
        let file_meta = self
            .files
            .get_mut(&file_id)
            .ok_or_else(|| format!("File {} not found", file_id))?;

        if let Some(page_data) = file_meta.pages.get(&page_id).copied() {
            match page_data {
                Some(offset) => {
                    file_meta.pages.replace(page_id, None);
                    file_meta.free_slots.push_front((page_id, offset));
                }
                None => return Err(format!("Page already deallocated in db file")),
            }
        } else {
            return Err(format!("Page not allocated in db file"));
        }

        Ok(())
    }

    pub fn aligned_buffer(data: &[u8]) -> Box<[u8]> {
        assert!(data.len() <= PAGE_SIZE, "Data exceeds PAGE_SIZE!");

        let layout = Layout::from_size_align(PAGE_SIZE, PAGE_SIZE).unwrap();
        let ptr = unsafe { alloc(layout) };

        if ptr.is_null() {
            panic!("Failed to allocate aligned buffer!");
        }

        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), ptr, data.len());
            let slice = slice::from_raw_parts_mut(ptr, PAGE_SIZE);
            Box::from_raw(slice)
        }
    }

    // Mayber these should not be functions of the disk manager
    // WAL
    pub fn open_log() -> (File, PathBuf) {
        let log_file_path = PathBuf::from("log_file_path.bin");
        let log_file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&log_file_path)
            .or_else(|_| {
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(&log_file_path)
            })
            .unwrap();

        (log_file, log_file_path)
    }

    pub fn create_db_file(&mut self) -> Result<(u64, PathBuf), i8> {
        std::fs::create_dir_all("geodeData/base").unwrap();

        let oid = self
            .mono_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let path_string = format!("geodeData/base/{}.bin", oid);
        let path = Path::new(&path_string);

        let new_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .custom_flags(O_DIRECT)
            .open(&path)
            .map_err(|_| -1)?;

        let file_ino = self.file_descriptors.set(new_file).0.ok_or(-1)?;
        self.file_map.insert(file_ino, path.to_path_buf());
        self.files.insert(
            file_ino,
            FileMetadata {
                pages: LinkedHashMap::new(),
                free_slots: VecDeque::new(),
            },
        );

        Ok((file_ino, path.to_path_buf()))
    }

    // TODO
    // Add a shutdown to empty  the file descriptor pool
}

#[cfg(test)]
pub mod test {

    use std::{
        fs::{remove_dir_all, remove_file},
        path::PathBuf,
    };

    use super::Manager;
    use crate::storage::page::page_constants::PAGE_SIZE;

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
    }

    #[test]
    fn teardown() {
        let log_file_path = PathBuf::from("log_file_path.bin");
        let db_path = PathBuf::from("geodeData");
        remove_file(log_file_path).unwrap();
        remove_dir_all(db_path).unwrap();
    }
}
