use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicU32, Ordering},
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
        page_guard::{PageGuard, ReadGuard, WriteGuard},
    },
    utils::replacer::{LRUKReplacer, Replacer},
};

pub struct FrameHeader {
    pub frame_id: u32,
    pub pin_count: AtomicU32,
    pub is_dirty: bool,

    pub data: Vec<u8>,
}

pub type SharedFrameHeader = Arc<FrameHeader>;
type FrameId = u32;
type PageId = u32;

type FilePageMap = HashMap<PageId, FrameId>;

pub struct BufferPoolManager {
    num_frames: usize,
    next_page_id: AtomicU32,

    // The frame headers of the frames that this buffer pool manages
    frames: LinkedHashMap<FrameId, Option<RwLock<SharedFrameHeader>>>,

    // File ID to file page map
    file_page_map: HashMap<u64, FilePageMap>,

    free_frames: VecDeque<FrameId>,

    // The replacer to find unpinned / candidate pages for eviction.
    replacer: Arc<Mutex<LRUKReplacer<u32>>>,

    // A pointer to the disk scheduler. Shared with the page guards for flushing.
    disk_scheduler: Arc<Mutex<DiskScheduler>>,

    manager: Arc<Mutex<Manager>>,
}

enum AccessType {
    Read,
    Write,
}

impl<'a> BufferPoolManager {
    fn new(num_frames: usize, manager: Manager) -> Self {
        let manager = Arc::new(Mutex::new(manager));

        let mut frames: LinkedHashMap<u32, Option<RwLock<SharedFrameHeader>>> =
            LinkedHashMap::with_capacity(num_frames);

        let mut free_frames: VecDeque<u32> = VecDeque::with_capacity(num_frames);

        let file_page_map: HashMap<u64, FilePageMap> = HashMap::new();

        for i in 0..num_frames {
            frames.insert(i as u32, None);
            free_frames.push_back(i as u32);
        }

        Self {
            num_frames: num_frames,
            next_page_id: AtomicU32::new(0),
            frames,
            file_page_map,
            free_frames,
            replacer: Arc::new(Mutex::new(LRUKReplacer::new(num_frames as usize, 2))),
            disk_scheduler: Arc::new(Mutex::new(DiskScheduler::new(Arc::clone(&manager)))),
            manager: Arc::clone(&manager),
        }
    }

    fn new_page(&mut self, file_id: u64) -> u32 {
        // Lock Contention
        // Not the most viable algorithm

        let manager = &self.manager;
        let mut manager_guard = manager.lock().unwrap();

        let (page_id, _) = manager_guard.allocate_page(file_id);
        self.next_page_id.fetch_add(1, Ordering::Relaxed);

        drop(manager_guard);

        page_id
    }

    fn allocate_file(&mut self) -> u64 {
        let manager = &self.manager;
        let mut manager_guard = manager.lock().unwrap();

        let (file_id, _) = manager_guard.create_db_file().unwrap();

        drop(manager_guard);

        file_id
    }

    fn delete_page(&mut self, file_id: u64, page_id: u32) -> bool {
        // Unwrap and error
        let page_frame_id_map = self.file_page_map.get_mut(&file_id).unwrap();

        let frame_id = page_frame_id_map.get(&page_id).unwrap().clone();

        let frame = self.frames.get_mut(&frame_id).unwrap();

        // Is the frame for that page pinned
        // Is the ordering optimal?
        if frame.as_ref().map_or(false, |frame| {
            frame
                .try_write()
                .expect("Valid Frame")
                .pin_count
                .load(Ordering::Relaxed)
                > 0
        }) {
            return false;
        }

        if let Some(frame) = frame.take() {
            let mut manager_guard = self.manager.lock().unwrap();

            match manager_guard.delete_page(file_id, page_id) {
                Ok(_) => {
                    self.frames.replace(frame_id, None);

                    // Page no longer has an associated Frame
                    page_frame_id_map.remove(&page_id);

                    self.free_frames
                        .push_front(frame.try_read().expect("Valid Frame").frame_id);
                    return true;
                }
                Err(err) => {
                    println!("{}", err);
                    self.frames.insert(frame_id, Some(frame));
                    return false;
                }
            }
        }
        false
    }

    fn check_page(
        &mut self,
        file_id: u64,
        page_id: u32,
        access_type: AccessType,
    ) -> Option<PageGuard> {
        let frame_id = {
            let file_map = self.file_page_map.get_mut(&file_id)?; // Error if file has not beeen allocated
            *file_map.get(&page_id)? // Error if page as not allocated
        };

        // Page already in memory no addition I/O
        if let Some(frame) = self.frames.get(&frame_id).unwrap() {
            // Construct page guard around the data then return it
            // Edit the frames header's is_dirty flag
            // It indicates whether or not the page's data has been modified

            return Some(self.create_guard(frame_id, page_id, access_type));
            // Data should be fully mutable
        }

        // Page not it memory No eviction needed Free frames available
        if let Some(frame_id) = self.free_frames.pop_front() {
            self.init_frame_data(file_id, page_id, frame_id);
            let frame_option = self.frames.get(&frame_id).unwrap();

            // Construct page guard around the data then return it
            // Edit the frames header's is_dirty flag
            // It indicates whether or not the page's data has been modified

            let frame_guard = frame_option.as_ref().unwrap();
            return Some(self.create_guard(
                frame_guard.try_read().unwrap().frame_id,
                page_id,
                access_type,
            ));

            // Data should be fully mutable
        }

        // Page not in memory eviction is needed
        // The buffer pool is tasked with finding memory that it can use to bring in a page of memory,
        // using the replacement algorithm you implemented previously to find candidate frames for eviction.

        let frame_id = {
            let mut replacer_guard = self.replacer.lock().unwrap();
            replacer_guard.evict()? // Returns None as ther is no page to evict
        };

        // Once the buffer pool has identified a frame for eviction, several I/O operations may be necessary
        // to bring in the page of data we want into the frame. Flush the data of the frame to evicted to disk

        let frame = self
            .frames
            .remove(&frame_id)
            .unwrap()
            .expect("Frame removed");

        // Flush page to disk if dirty
        let page_data = &frame.try_write().unwrap().data;
        self.flush_page(file_id, page_id, &page_data);

        // Init new frame to be written to
        self.init_frame_data(file_id, page_id, frame.try_read().unwrap().frame_id);


        // Construct page guard around the data then return it
        // Edit the frames header's is_dirty flag
        // It indicates whether or not the page's data has been modified

        return Some(self.create_guard(frame_id, page_id, access_type));


    }

    fn check_write_page(&mut self, file_id: u64, page_id: u32) -> Option<WriteGuard> {
        self.check_page(file_id, page_id, AccessType::Write)
            .map(|guard| guard.into_write_guard())
            .expect("Guard Error")
    }

    fn check_read_page(&mut self, file_id: u64, page_id: u32) -> Option<ReadGuard> {
        self.check_page(file_id, page_id, AccessType::Write)
            .map(|guard| guard.into_read_guard())
            .expect("Guard Error")
    }

    fn write_page(&mut self, file_id: u64, page_id: u32) -> WriteGuard {
        let guard = self
            .check_write_page(file_id, page_id)
            .expect("Read lock error");

        guard
    }

    fn read_page(&mut self, file_id: u64, page_id: u32) -> ReadGuard {
        let guard = self
            .check_read_page(file_id, page_id)
            .expect("Write lock error");

        guard
    }

    fn flush_page_unsafe(&self, file_id: u64, page_id: u32, frame: FrameHeader) -> bool {
        // Does file and page exist ?

        let exists = self
            .file_page_map
            .get(&file_id) // Error if file has not beeen allocated
            .and_then(|file_map| file_map.get(&page_id)) // Error if page as not allocated
            .is_some();

        if exists {
            return false;
        }

        let mut manager_guard = self.manager.lock().unwrap();

        if manager_guard
            .write_page(file_id, page_id, &frame.data)
            .is_ok()
        {
            return true;
        }
        false
    }

    fn flush_page(&self, file_id: u64, page_id: u32, frame: &[u8]) -> bool {
        // Does file and page exist ?

        let exists = self
            .file_page_map
            .get(&file_id) // Error if file has not beeen allocated
            .and_then(|file_map| file_map.get(&page_id)) // Error if page as not allocated
            .is_some();

        if exists {
            return false;
        }

        let scheduler = self.disk_scheduler.try_lock().unwrap();
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

        true
    }

    // fn flush_all_pages() {}

    fn size(&self) -> u32 {
        self.frames.len() as u32
    }

    fn get_pin_count(&self, file_id: u64, page_id: u32) -> &u32 {
        let file_map = self.file_page_map.get(&file_id).unwrap();
        &file_map.get(&page_id).unwrap()
    }

    fn init_frame_data(&mut self, file_id: u64, page_id: u32, frame_id: u32) {
        let file_map = self.file_page_map.get_mut(&file_id).unwrap();
        // Read page data into memory
        let mut manager = self.manager.lock().unwrap();
        let mut page_buffer: Vec<u8> = Vec::with_capacity(PAGE_SIZE);
        manager
            .read_page(file_id, page_id, &mut page_buffer)
            .unwrap();

        drop(manager);

        // initialize frame
        let frame = FrameHeader {
            data: page_buffer,
            frame_id: frame_id,
            is_dirty: false, // I forgot what dirty means. It means data on disk is not consistent with data in memory >_O
            pin_count: AtomicU32::new(0), //
        };

        // Maps the Page_ID to a Frame_Id
        file_map.insert(page_id, frame_id);

        // Update a Shared frame to a non None value
        self.frames
            .insert(frame_id, Some(RwLock::new(Arc::new(frame))));

        let frame = self.frames.get(&frame_id).unwrap();
        // Arc::clone(frame.as_ref().expect("frame available"))
    }

    // Selects a frame a returns a guard enclosing it
    fn create_guard(&self, frame_id: u32, page_id: u32, access_type: AccessType) -> PageGuard {
        let frame = self
            .frames
            .get(&frame_id)
            .unwrap()
            .as_ref()
            .expect("Valid frame");

        match access_type {
            AccessType::Read => {
                let guard = frame.try_read().unwrap();

                return PageGuard::ReadGuard(ReadGuard::new(
                    page_id,
                    guard.into(),
                    self.replacer.clone(),
                    self.disk_scheduler.clone(),
                    Arc::new(Mutex::new(self)),
                ));
            }
            AccessType::Write => {
                let guard = frame.try_write().unwrap();

                return PageGuard::WriteGuard(WriteGuard::new(
                    page_id,
                    guard.into(),
                    self.replacer.clone(),
                    self.disk_scheduler.clone(),
                    Arc::new(Mutex::new(self)),
                ));
            }
        }
    }
}
