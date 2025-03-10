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
        page::page_constants::PAGE_SIZE,
        page_guard::{FrameGuard, PageGuard, ReadGuard, WriteGuard},
    },
    utils::replacer::{LRUKReplacer, Replacer},
};

pub type FrameId = u32;
type PageId = u32;
type FileId = u64;

pub struct FrameHeader {
    pub frame_id: FrameId,
    pub pin_count: AtomicU32,
    pub writing_to: AtomicBool,
    pub is_dirty: AtomicBool,

    pub data: Box<[u8]>,
}

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

    manager: Arc<Mutex<Manager>>,
}
pub struct BufferPoolHandle {
    inner: Arc<BufferPoolManager>,
}
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
        self.next_page_id.fetch_add(1, Ordering::Relaxed);

        dbg!("Dropping manager guard used to create new page");
        drop(manager_guard);

        let mut file_page_map_guard = self.file_page_map.write().unwrap();

        // Inititial frame_id for pages
        let page_frame_map = file_page_map_guard.get_mut(&file_id).unwrap();
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

    pub fn flush_page_unsafe(&self, file_id: u64, page_id: PageId, frame_data: &[u8]) -> bool {
        // Does file and page exist ?

        let exists = self
            .file_page_map
            .try_read()
            .unwrap()
            .get(&file_id) // Error if file has not beeen allocated
            .and_then(|file_map| file_map.get(&page_id)) // Error if page as not allocated
            .is_some();

        if exists {
            return false;
        }

        {
            let mut manager_guard = self.manager.lock().unwrap();

            if manager_guard
                .write_page(file_id, page_id, frame_data)
                .is_ok()
            {
                return true;
            }
        }

        false
    }

    fn check_page(
        &self,
        file_id: u64,
        page_id: PageId,
        access_type: AccessType,
    ) -> Option<PageGuard> {
        dbg!("Checking page");

        match access_type {
            AccessType::Read => {
                dbg!("Reading");
            }
            _ => {}
        }

        let file_map_guard = self.file_page_map.read().unwrap();
        let file_map = file_map_guard.get(&file_id)?; // Error if file has not been allocated
        let frame_id = *file_map.get(&page_id).unwrap();

        // Frame not initialized
        // Page has not been given an allocated frame
        // Page id not in memory
        if frame_id.is_none() {
            drop(file_map_guard);
            dbg!("Frame not initialized Page id not in memory");

            // Free frames are available no eviction needed
            if let Some(frame_id) = self.free_frames.lock().unwrap().pop_front() {
                dbg!("Free frames are available no eviction needed");

                self.init_frame_data(file_id, page_id, frame_id);
                let frame_guard = self.frames.read().unwrap();
                let frame_option = frame_guard.get(&frame_id).unwrap();

                // Construct page guard around the data then return it
                // Edit the frames header's is_dirty flag
                // It indicates whether or not the page's data has been modified

                let frame_guard = frame_option.as_ref().unwrap();
                let read_guard = frame_guard.read().unwrap();
                let id = read_guard.frame_id;
                drop(read_guard);

                return Some(self.create_guard(id, access_type));
            }

            // Free frames are not available eviction needed
            // The buffer pool is tasked with finding memory that it can use to bring in a page of memory,
            // using the replacement algorithm you implemented previously to find candidate frames for eviction.

            dbg!("Free frames not available eviction needed");
            let frame_id = {
                let mut replacer_guard = self.replacer.lock().unwrap();
                replacer_guard.evict()? // Returns None as ther is no page to evict
            };

            // Once the buffer pool has identified a frame for eviction, several I/O operations may be necessary
            // to bring in the page of data we want into the frame. Flush the data of the frame to evicted to disk

            let frame = self
                .frames
                .try_write()
                .unwrap()
                .remove(&frame_id)
                .unwrap()
                .expect("Frame removed");

            // Flush page to disk if dirty
            let page_guard = frame.write().unwrap();
            let page_data = &page_guard.data;
            self.flush_page_unsafe(file_id, page_id, &page_data);
            drop(page_guard);

            // Init new frame to be written to
            let page_guard = frame.read().unwrap();
            self.init_frame_data(file_id, page_id, page_guard.frame_id);
            drop(page_guard);

            // Construct page guard around the data then return it
            // Edit the frames header's is_dirty flag
            // It indicates whether or not the page's data has been modified

            return Some(self.create_guard(frame_id, access_type));
        }
        // Page has been allocated a frame
        // Page in memory
        else if let Some(frame_id) = frame_id {
            drop(file_map_guard);
            if self.frames.read().unwrap().get(&frame_id).is_some() {
                dbg!("Page has been allocated a frame\nPage in memory");
                // Construct page guard around the data then return it
                return Some(self.create_guard(frame_id, access_type));
            }
        }
        None
    }

    pub async fn flush_page(&self, file_id: FileId, page_id: PageId, frame: &[u8]) -> bool {
        // Does file and page exist ?

        let exists = self
            .file_page_map
            .try_read()
            .unwrap()
            .get(&file_id) // Error if file has not beeen allocated
            .and_then(|file_map| file_map.get(&page_id)) // Error if page as not allocated
            .is_some();

        if exists {
            return false;
        }

        let scheduler = self.disk_scheduler.lock().unwrap();
        let future = scheduler.create_future();

        // Write Request
        let page_data = Manager::aligned_buffer(&frame);

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
        let mut manager = self.manager.lock().unwrap();
        let mut page_buffer: Vec<u8> = Vec::with_capacity(PAGE_SIZE);
        manager
            .read_page(file_id, page_id, &mut page_buffer)
            .unwrap();

        drop(manager);

        // initialize frame
        let frame = FrameHeader {
            data: page_buffer.into(),
            frame_id,
            writing_to: AtomicBool::new(false),
            is_dirty: AtomicBool::new(false), // I forgot what dirty means. It means data on disk is not consistent with data in memory >_O
            pin_count: AtomicU32::new(0),     //
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





// pub fn delete_page(&self, file_id: FileId, page_id: PageId) -> bool {
//     // Unwrap and error
//     let mut page_frame_map_guard = self.file_page_map.write().unwrap();
//     let page_frame_map = page_frame_map_guard.get_mut(&file_id).unwrap();

//     let frame_id_option = page_frame_map.get(&page_id).unwrap().clone();

//     if frame_id_option.is_none() {
//         return false;
//     }
//     let frame_id = frame_id_option.unwrap();

//     let mut frame_guard = self.frames.write().unwrap();
//     let frame = frame_guard.get_mut(&frame_id).unwrap();

//     // Is the frame for that page pinned
//     if frame.as_ref().map_or(false, |frame| {
//         frame.read().unwrap().pin_count.load(Ordering::Relaxed) > 0
//     }) {
//         drop(frame_guard);
//         drop(page_frame_map_guard);
//         return false;
//     }

//     if let Some(frame) = frame.take() {
//         let mut manager_guard = self.manager.lock().unwrap();

//         match manager_guard.delete_page(file_id, page_id) {
//             Ok(_) => {
//                 self.frames.try_write().unwrap().replace(frame_id, None);

//                 // Page no longer has an associated Frame
//                 page_frame_map.insert(page_id, None);

//                 let frame_guard = frame.read().unwrap();
//                 self.free_frames
//                     .lock()
//                     .unwrap()
//                     .push_front(frame_guard.frame_id);
//                 drop(frame_guard);

//                 return true;
//             }
//             Err(err) => {
//                 println!("{}", err);
//                 self.frames
//                     .try_write()
//                     .unwrap()
//                     .insert(frame_id, Some(frame));
//                 return false;
//             }
//         }
//     }
//     false
// }



//     pub fn delete_page(&mut self, file_id: FileId, page_id: PageId) -> bool {
//         // Unwrap and error
//         let page_frame_map = self.file_page_map.get_mut(&file_id).unwrap();

//         let frame_id_option = page_frame_map.get(&page_id).unwrap().clone();

//         if frame_id_option.is_none() {
//             return false;
//         }
//         let frame_id = frame_id_option.unwrap();

//         let frame = self.frames.get_mut(&frame_id).unwrap();

//         // Is the frame for that page pinned
//         if frame.as_ref().map_or(false, |frame| {
//             frame.read().unwrap().pin_count.load(Ordering::Relaxed) > 0
//         }) {
//             return false;
//         }

//         if let Some(frame) = frame.take() {
//             let mut manager_guard = self.manager.lock().unwrap();

//             match manager_guard.delete_page(file_id, page_id) {
//                 Ok(_) => {
//                     self.frames.replace(frame_id, None);

//                     // Page no longer has an associated Frame
//                     page_frame_map.insert(page_id, None);

//                     let frame_guard = frame.read().unwrap();
//                     self.free_frames.push_front(frame_guard.frame_id);
//                     drop(frame_guard);

//                     return true;
//                 }
//                 Err(err) => {
//                     println!("{}", err);
//                     self.frames.insert(frame_id, Some(frame));
//                     return false;
//                 }
//             }
//         }
//         false
//     }



