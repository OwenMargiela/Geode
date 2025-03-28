use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
        Arc, Mutex, RwLock,
    },
};

use hashlink::LinkedHashMap;

use crate::{
    storage::{
        disk::{
            manager::Manager,
            scheduler::{DiskData, DiskRequest, DiskScheduler},
        },
        page::page::page_constants::PAGE_SIZE,
        page::page_guard::{FrameGuard, PageGuard, ReadGuard, WriteGuard},
    },
    utils::replacer::{LRUKReplacer, Replacer},
};

pub type FrameId = u32;
pub type PageId = u32;
pub type FileId = u64;

pub struct FrameHeader {
    pub frame_id: FrameId,
    pub pin_count: AtomicU32,
    pub writing_to: AtomicBool,
    pub is_dirty: AtomicBool,
    pub page_id: PageId,
    pub file_id: FileId,

    pub data: Box<[u8]>,
}

// Tracks page allocations in a File
// Maps every page to possible allocated frame
type FilePageMap = HashMap<PageId, Option<FrameId>>;

pub struct BufferPoolManager {
    num_frames: usize,
    next_page_id: AtomicU32,

    // The frame headers of the frames that this buffer pool manages
    frames: Arc<RwLock<LinkedHashMap<FrameId, Option<RwLock<FrameHeader>>>>>,

    // File ID to file page map
    file_page_map: Arc<RwLock<HashMap<FileId, FilePageMap>>>,

    free_frames: Arc<Mutex<VecDeque<FrameId>>>,

    // The replacer to find unpinned / candidate Frames for eviction.
    replacer: Arc<Mutex<LRUKReplacer<FrameId>>>,

    // A pointer to the disk scheduler. Shared with the page guards for flushing.
    disk_scheduler: Arc<Mutex<DiskScheduler>>,

    pub manager: Arc<Mutex<Manager>>,
}
// pub struct BufferPoolHandle {
//     inner: Arc<BufferPoolManager>,
// }


pub enum AccessType {
    Read,
    Write,
}

impl BufferPoolManager {
    pub fn new(num_frames: usize, manager: Manager, k_dist: usize) -> Self {
        let manager = Arc::new(Mutex::new(manager));

        let mut frames: LinkedHashMap<FrameId, Option<RwLock<FrameHeader>>> =
            LinkedHashMap::with_capacity(num_frames);

        let mut free_frames: VecDeque<FrameId> = VecDeque::with_capacity(num_frames);

        let file_page_map: HashMap<FileId, FilePageMap> = HashMap::new();

        for i in 0..num_frames {
            frames.insert(i as u32, None);

            // The maximum amount of frames are all allocated at once

            free_frames.push_back(i as u32);
        }

        Self {
            num_frames,
            next_page_id: AtomicU32::new(0),
            frames: Arc::new(RwLock::new(frames)),
            file_page_map: Arc::new(RwLock::new(file_page_map)),
            free_frames: Arc::new(Mutex::new(free_frames)),
            replacer: Arc::new(Mutex::new(LRUKReplacer::new(num_frames as usize, k_dist))),
            disk_scheduler: Arc::new(Mutex::new(DiskScheduler::new(Arc::clone(&manager)))),
            manager: Arc::clone(&manager),
        }
    }

    // Allocates a new File on disk

    pub fn allocate_file(&mut self) -> FileId {
        let manager = &self.manager;
        let mut manager_guard = manager.lock().unwrap();

        let (file_id, _) = manager_guard.create_db_file().unwrap();

        drop(manager_guard);

        {
            let mut file_map_guard = self.file_page_map.write().unwrap();

            file_map_guard.insert(file_id, HashMap::new());
        }

        file_id
    }

    pub fn new_page(&self, file_id: FileId) -> PageId {
        // Lock Contention
        // Not the most viable algorithm

        let manager = &self.manager;
        let mut manager_guard = manager.lock().unwrap();
        dbg!("Aqquired manager guard used to create new page");

        let (page_id, _) = manager_guard.allocate_page(file_id);
        dbg!("Dropping manager guard used to create new page");

        self.next_page_id.fetch_add(1, Ordering::Relaxed);

        let mut file_page_map_guard = self.file_page_map.write().unwrap();

        // Inititial frame_id for page
        let page_frame_map = file_page_map_guard.get_mut(&file_id).unwrap();

        // Inittialize Default Page
        {
            println!("Initialize File {} Page {}", file_id, page_id);

            let buffer: Box<[u8]> = Box::new([0; PAGE_SIZE]);
            let mut page_buffer = Manager::aligned_buffer(&buffer);
            manager_guard
                .write_page(file_id, page_id, &mut page_buffer)
                .unwrap();
        }

        drop(manager_guard);

        // Page does not an allocated frame
        // Therefore, value is initialized to zero
        page_frame_map.insert(page_id, None);
        page_id
    }

    // Study this
    pub fn delete_page(&self, file_id: FileId, page_id: PageId) -> bool {
        let mut page_frame_map_guard = self.file_page_map.write().unwrap();
        let page_frame_map = match page_frame_map_guard.get_mut(&file_id) {
            Some(map) => map,
            None => return false,
        };

        let frame_id = match page_frame_map.get(&page_id).and_then(|id| id.clone()) {
            Some(id) => id,
            None => return false,
        };

        let mut frame_guard = self.frames.write().unwrap();
        let frame = match frame_guard.get_mut(&frame_id).and_then(|f| f.take()) {
            Some(f) => f,
            None => return false,
        };

        if frame.read().unwrap().pin_count.load(Ordering::Relaxed) > 0 {
            frame_guard.insert(frame_id, Some(frame)); // Restore frame if pinned
            return false;
        }

        if self
            .manager
            .lock()
            .unwrap()
            .delete_page(file_id, page_id)
            .is_ok()
        {
            page_frame_map.insert(page_id, None);
            self.free_frames
                .lock()
                .unwrap()
                .push_front(frame.read().unwrap().frame_id);
            return true;
        }

        frame_guard.insert(frame_id, Some(frame)); // Rollback if deletion fails
        false
    }

    fn check_page(
        &self,
        file_id: FileId,
        page_id: PageId,
        access_type: AccessType,
    ) -> Option<PageGuard> {
        dbg!("Checking page");

        match access_type {
            AccessType::Read => {
                dbg!("Reading");
                if page_id == 13 {
                    println!("\n\n\n Reading page ID {} \n\n\n", page_id);
                }
            }
            _ => {}
        }
        let frame_id: Option<u32>;

        {
            let file_map_guard = self.file_page_map.read().unwrap();
            let file_map = file_map_guard.get(&file_id)?; // Error if file has not been allocated
            frame_id = *file_map.get(&page_id).unwrap();
        }

        if frame_id.is_none() {
            // Cases
            dbg!("Frame not initialized Page id not in memory");

            // 1.
            // Free frames are available no eviction needed

            if let Some(frame_id) = self.free_frames.lock().unwrap().pop_front() {
                dbg!("Free frames are available no eviction needed");

                self.init_frame_data(file_id, page_id, frame_id);
                return Some(self.create_guard(frame_id, access_type));
            }

            // 2.
            // Free frames are not available eviction needed
            // The buffer pool is tasked with finding memory that it can use to bring
            // in a page of memory, using the replacement algorithm you implemented
            // previously to find candidate frames for eviction.

            dbg!("Free frames not available eviction needed");

            let eviction_id = {
                let mut replacer_guard = self.replacer.lock().unwrap();
                replacer_guard.evict()? // Returns None as ther is no page to evict
            };
            println!("Evicting {}", eviction_id);

            // Data to be evicticted needs to be persisted on disk
            // using the evict() function, obtain the frame_id and by extention the
            // page_id as well as the file_id to find the location of persistence

            // Once the buffer pool has identified a frame for eviction, several I/O operations may be necessary
            // to bring in the page of data we want into the frame. Flush the data of the frame to evicted to disk

            let mut frame_guard = self.frames.write().unwrap();

            dbg!("Aqquired lock");

            let frame = frame_guard
                .get_mut(&eviction_id)
                .unwrap()
                .as_ref()
                .expect("Valid frame");

            // Flush page to disk if dirty
            let evicted_page_guard = frame.write().unwrap();
            let page_data = &evicted_page_guard.data;

            {
                let mut file_map_guard = self.file_page_map.write().unwrap();
                let file_map = file_map_guard.get_mut(&evicted_page_guard.file_id)?;
                file_map.insert(evicted_page_guard.page_id, None);
            }

            let res = self.flush_page_sync(
                evicted_page_guard.file_id,
                evicted_page_guard.page_id,
                &page_data,
            );

            println!("Flush == {} ", res);

            drop(evicted_page_guard);
            drop(frame_guard);

            // Init new frame to be written to
            // It now takes the ID of the evicted frame
            self.init_frame_data(file_id, page_id, eviction_id);

            // Construct page guard around the data then return it
            // Edit the frames header's is_dirty flag
            // It indicates whether or not the page's data has been modified

            return Some(self.create_guard(eviction_id, access_type));
        }
        // Page has been allocated a frame
        // Page in memory
        else if let Some(frame_id) = frame_id {
            if self.frames.read().unwrap().get(&frame_id).is_some() {
                return Some(self.create_guard(frame_id, access_type));
            }
        }

        None
    }

    pub fn flush_page_sync(&self, file_id: u64, page_id: PageId, frame_data: &[u8]) -> bool {
        // Does file and page exist ?

        let exists = self
            .file_page_map
            .try_read()
            .unwrap()
            .get(&file_id) // Error if file has not beeen allocated
            .and_then(|file_map| file_map.get(&page_id)) // Error if page as not allocated
            .is_some();

        if !exists {
            println!("Does not exist");
            return false;
        }
        println!("exists == {}", exists);

        {
            let mut manager_guard = self.manager.lock().unwrap();

            let alingend_frame_data = Manager::aligned_buffer(frame_data);

            if manager_guard
                .write_page(file_id, page_id, &alingend_frame_data)
                .is_ok()
            {
                return true;
            }
        }

        false
    }
    pub async fn flush_page_async(&self, file_id: FileId, page_id: PageId, frame: &[u8]) -> bool {
        // Does file and page exist ?

        let exists = self
            .file_page_map
            .try_read()
            .unwrap()
            .get(&file_id) // Error if file has not beeen allocated
            .and_then(|file_map| file_map.get(&page_id)) // Error if page as not allocated
            .is_some();

        if !exists {
            return false;
        }

        let scheduler = self.disk_scheduler.lock().unwrap();
        let future = scheduler.create_future();

        // Write Request
        let page_data = Manager::aligned_buffer(&frame);
        println!("Writing data {:?}\n\n", page_data);
        let request = DiskRequest {
            data: DiskData::Write(Some(page_data)), // Move the buffer
            done_flag: Arc::clone(&future.flag),
            file_id,
            is_write: true,
            page_id,
            waker: Arc::clone(&future.waker),
        };

        scheduler.schedule(request);
        drop(scheduler);

        future.await;

        true
    }

    fn init_frame_data(&self, file_id: FileId, page_id: PageId, frame_id: FrameId) {
        // Read page data into memory

        let mut frame_data = vec![0u8; PAGE_SIZE].into_boxed_slice();

        {
            let mut manager = self.manager.lock().unwrap();

            println!("Reading File {} Page {}", file_id, page_id);

            let buffer: Box<[u8]> = Box::new([0; PAGE_SIZE]);
            let mut page_buffer = Manager::aligned_buffer(&buffer);
            manager
                .read_page(file_id, page_id, &mut page_buffer)
                .unwrap();

            if page_buffer.len() == PAGE_SIZE {
                frame_data.copy_from_slice(&page_buffer);
            }
        }

        // initialize frame
        let frame = FrameHeader {
            data: frame_data,
            frame_id,
            writing_to: AtomicBool::new(false),
            is_dirty: AtomicBool::new(false), // I forgot what dirty means. It means data on disk is not consistent with data in memory >_O
            pin_count: AtomicU32::new(0),
            file_id,
            page_id,
        };

        {
            let mut file_page_map_gaurd = self.file_page_map.write().unwrap();

            let file_map = file_page_map_gaurd.get_mut(&file_id).unwrap();

            // Maps the Page_ID to a Frame_Id
            file_map.insert(page_id, Some(frame_id));
        }

        // Update a Shared frame to a non None value
        self.frames
            .write()
            .unwrap()
            .insert(frame_id, Some(RwLock::new(frame)));
    }

    fn create_guard(&self, frame_id: FrameId, access_type: AccessType) -> PageGuard {
        // Acquire the read lock for the frames map
        let frame_guard = self.frames.read().unwrap();
        FrameGuard::new(frame_id, self.replacer.clone(), frame_guard, access_type)
    }

    fn check_write_page(&self, file_id: u64, page_id: PageId) -> Option<WriteGuard> {
        self.check_page(file_id, page_id, AccessType::Write)
            .map(|guard| guard.into_write_guard())
            .expect("Guard Error")
    }

    fn check_read_page(&self, file_id: u64, page_id: PageId) -> Option<ReadGuard> {
        self.check_page(file_id, page_id, AccessType::Read)
            .map(|guard| guard.into_read_guard())
            .expect("Guard Error")
    }

    pub fn write_page(&self, file_id: u64, page_id: PageId) -> WriteGuard {
        let guard = self
            .check_write_page(file_id, page_id)
            .expect("Write lock error");

        guard
    }

    pub fn read_page(&self, file_id: u64, page_id: PageId) -> ReadGuard {
        let guard = self
            .check_read_page(file_id, page_id)
            .expect("Read lock error");

        guard
    }

    pub fn get_pin_count(&self, file_id: FileId, page_id: PageId) -> u32 {
        let page_frame_map_guard = self.file_page_map.read().unwrap();
        let page_frame_map = page_frame_map_guard.get(&file_id).unwrap();
        let frame = page_frame_map.get(&page_id).unwrap();

        let frame_option_guard = self.frames.read().unwrap();
        let frame_option = frame_option_guard.get(&frame.unwrap()).unwrap();

        drop(page_frame_map_guard);

        let frame_guard = frame_option.as_ref().expect("Frame").read().unwrap();

        let pin = frame_guard.pin_count.load(Ordering::Relaxed);
        drop(frame_guard);
        pin
    }
}
