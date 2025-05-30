#![allow(unused_variables)] 
#![allow(dead_code)] 

use std::{
    collections::HashMap,
    path::Path,
    sync::{ atomic::{ AtomicU32, Ordering }, Arc, Mutex, RwLock },
};

use crossbeam_queue::SegQueue;
use dashmap::DashMap;
use hashlink::LinkedHashMap;

use crate::{
    index::tree::tree_page::tree_page_layout::PAGE_SIZE,
    storage::{
        disk::{ manager::Manager, scheduler::{ DiskData, DiskRequest, DiskScheduler } },
        page::page_guard::{ FrameGuard, PageGuard, ReadGuard, WriteGuard },
    },
    utils::replacer::{ LRUKReplacer, Replacer },
};

#[derive(Clone, Copy)]
pub enum Protocol {
    Exclusive, // Pesimistic crabbing. Exclusive / Write latches upon tree descent
    Shared, // Optimistic crabbing. Shared / Read latches upon tree descent
}

pub type FrameId = u32;
pub type PageId = u32;
pub type FileId = u64;

pub struct FrameHeader {
    pub frame_id: FrameId,
    pub pin_count: AtomicU32,

    pub page_id: PageId,
    pub file_id: FileId,

    pub data: [u8; PAGE_SIZE],
}

// Tracks page allocations in a File
// Maps every page to possible allocated frame
type FilePageMap = HashMap<PageId, Option<FrameId>>;

type RwLinkMap<K, V> = RwLock<LinkedHashMap<K, V>>;
type FrameOption = Option<RwLock<FrameHeader>>;

pub struct BufferPoolManager {
    num_frames: usize,
    next_page_id: AtomicU32,

    // The frame headers of the frames that this buffer pool manages
    frames: Arc<RwLinkMap<FrameId, FrameOption>>,

    // File ID to file page map
    file_page_map: Arc<DashMap<FileId, FilePageMap>>,

    free_frames: Arc<SegQueue<FrameId>>,

    // The replacer to find unpinned / candidate Frames for eviction.
    replacer: Arc<Mutex<LRUKReplacer<FrameId>>>,

    // A pointer to the disk scheduler. Shared with the page guards for flushing.
    disk_scheduler: Arc<Mutex<DiskScheduler>>,

    pub manager: Arc<Mutex<Manager>>,
}

impl BufferPoolManager {
    pub fn new(num_frames: usize, manager: Manager, k_dist: usize) -> Self {
        let manager = Arc::new(Mutex::new(manager));

        let mut frames: LinkedHashMap<
            FrameId,
            Option<RwLock<FrameHeader>>
        > = LinkedHashMap::with_capacity(num_frames);

        let free_frames: SegQueue<FrameId> = SegQueue::new();

        let file_page_map: DashMap<FileId, FilePageMap> = DashMap::new();

        for i in 0..num_frames {
            frames.insert(i as u32, None);

            // The maximum amount of frames are all allocated at once

            free_frames.push(i as u32);
        }

        Self {
            num_frames,
            next_page_id: AtomicU32::new(0),
            frames: Arc::new(RwLock::new(frames)),
            file_page_map: Arc::new(file_page_map),
            free_frames: Arc::new(free_frames),
            replacer: Arc::new(Mutex::new(LRUKReplacer::new(num_frames as usize, k_dist))),
            disk_scheduler: Arc::new(Mutex::new(DiskScheduler::new(Arc::clone(&manager)))),
            manager: Arc::clone(&manager),
        }
    }

    pub fn new_with_arc(num_frames: usize, manager: Arc<Mutex<Manager>>, k_dist: usize) -> Self {
        let mut frames: LinkedHashMap<
            FrameId,
            Option<RwLock<FrameHeader>>
        > = LinkedHashMap::with_capacity(num_frames);

        let free_frames: SegQueue<FrameId> = SegQueue::new();

        let file_page_map: DashMap<FileId, FilePageMap> = DashMap::new();

        for i in 0..num_frames {
            frames.insert(i as u32, None);

            // The maximum amount of frames are all allocated at once

            free_frames.push(i as u32);
        }

        Self {
            num_frames,
            next_page_id: AtomicU32::new(0),
            frames: Arc::new(RwLock::new(frames)),
            file_page_map: Arc::new(file_page_map),
            free_frames: Arc::new(free_frames),
            replacer: Arc::new(Mutex::new(LRUKReplacer::new(num_frames as usize, k_dist))),
            disk_scheduler: Arc::new(Mutex::new(DiskScheduler::new(Arc::clone(&manager)))),
            manager,
        }
    }

    // Allocates a new File on disk

    pub fn allocate_file(&self) -> FileId {
        let manager = &self.manager;
        let mut manager_guard = manager.lock().unwrap();

        let (file_id, _) = manager_guard.create_db_file().unwrap();

        drop(manager_guard);

        {
            self.file_page_map.insert(file_id, HashMap::new());
        }

        file_id
    }

    pub fn open_file(&self, path: impl AsRef<Path> + std::fmt::Debug) -> FileId {
        let manager = &self.manager;
        let mut manager_guard = manager.lock().unwrap();

        let (file_id, _) = manager_guard.open_from_db_file(path).unwrap();

        drop(manager_guard);

        {
            self.file_page_map.insert(file_id, HashMap::new());
            let mut page = self.file_page_map.get_mut(&file_id).unwrap();
            page.insert(0, None);
        }

        file_id
    }

    pub fn new_page(&self, file_id: FileId) -> PageId {
        let manager = &self.manager;
        let mut manager_guard = manager.lock().unwrap();

        let (page_id, _) = manager_guard.allocate_page(file_id);

        self.next_page_id.fetch_add(1, Ordering::Relaxed);

        // let mut file_page_map_guard = self.file_page_map.write().unwrap();

        // Inititial frame_id for page
        let mut page_frame_map = self.file_page_map.get_mut(&file_id).unwrap();

        // Inittialize Default Page
        {
            let buffer: Box<[u8]> = Box::new([0; PAGE_SIZE]);
            let mut page_buffer = Manager::aligned_buffer(&buffer);
            manager_guard.write_page(file_id, page_id, &mut page_buffer).unwrap();
        }

        drop(manager_guard);

        // Page does not an allocated frame
        // Therefore, value is initialized to zero
        page_frame_map.insert(page_id, None);
        page_id
    }

    // Study this
    pub fn delete_page(&self, file_id: FileId, page_id: PageId) -> bool {
        let page_frame_map = self.file_page_map.clone();

        let mut page_frame_map = match page_frame_map.get_mut(&file_id) {
            Some(map) => map,
            None => {
                return false;
            }
        };

        let frame_id = match page_frame_map.get(&page_id).and_then(|id| id.clone()) {
            Some(id) => id,
            None => {
                return false;
            }
        };

        let mut frame_guard = self.frames.write().unwrap();
        let frame = match frame_guard.get_mut(&frame_id).and_then(|f| f.take()) {
            Some(f) => f,
            None => {
                return false;
            }
        };

        if frame.read().unwrap().pin_count.load(Ordering::Relaxed) > 0 {
            frame_guard.insert(frame_id, Some(frame)); // Restore frame if pinned
            return false;
        }

        if self.manager.lock().unwrap().delete_page(file_id, page_id).is_ok() {
            page_frame_map.insert(page_id, None);
            self.free_frames.push(frame.read().unwrap().frame_id);
            return true;
        }

        frame_guard.insert(frame_id, Some(frame)); // Rollback if deletion fails
        false
    }

    pub(self) fn check_page(
        &self,
        file_id: FileId,
        page_id: PageId,
        access_type: Protocol
    ) -> Option<PageGuard> {
        let frame_id: Option<u32>;

        {
            // let file_map_guard = self.file_page_map.read().unwrap();
            let file_map = self.file_page_map.get(&file_id)?; // Error if file has not been allocated
            frame_id = file_map.get(&page_id).and_then(|x| *x);
        }

        if frame_id.is_none() {
            // Cases

            // 1.
            // Free frames are available no eviction needed

            if let Some(frame_id) = self.free_frames.pop() {
                self.init_frame_data(file_id, page_id, frame_id);
                return Some(self.create_guard(frame_id, access_type));
            }

            // 2.
            // Free frames are not available eviction needed
            // The buffer pool is tasked with finding memory that it can use to bring
            // in a page of memory, using the replacement algorithm you implemented
            // previously to find candidate frames for eviction.

            let eviction_id = {
                let mut replacer_guard = self.replacer.lock().unwrap();
                replacer_guard.evict()? // Returns None as ther is no page to evict
            };

            // Data to be evicticted needs to be persisted on disk
            // using the evict() function, obtain the frame_id and by extention the
            // page_id as well as the file_id to find the location of persistence

            // Once the buffer pool has identified a frame for eviction, several I/O operations may be necessary
            // to bring in the page of data we want into the frame. Flush the data of the frame to evicted to disk

            let mut frame_guard = self.frames.write().unwrap();

            let frame = frame_guard.get_mut(&eviction_id).unwrap().as_ref().expect("Valid frame");

            // Flush page to disk if dirty
            let evicted_page_guard = frame.write().unwrap();
            let page_data = &evicted_page_guard.data;

            {
                let mut file_map = self.file_page_map.get_mut(&evicted_page_guard.file_id)?;
                file_map.insert(evicted_page_guard.page_id, None);
            }

            let res = self.flush_page_sync(
                evicted_page_guard.file_id,
                evicted_page_guard.page_id,
                &page_data
            );

            drop(evicted_page_guard);
            drop(frame_guard);

            // Init new frame to be written to
            // It now takes the ID of the evicted frame
            self.init_frame_data(file_id, page_id, eviction_id);

            // Construct page guard around the data then return it
            // Edit the frames header's is_dirty flag
            // It indicates whether or not the page's data has been modified

            return Some(self.create_guard(eviction_id, access_type));
        } else if
            // Page has been allocated a frame
            // Page in memory
            let Some(frame_id) = frame_id
        {
            if self.frames.read().unwrap().get(&frame_id).is_some() {
                return Some(self.create_guard(frame_id, access_type));
            }
        }

        None
    }

    pub(self) fn flush_page_sync(
        &self,
        file_id: u64,
        page_id: PageId,
        frame_data: &[u8; PAGE_SIZE]
    ) -> bool {
        // Does file and page exist ?

        let file_map = self.file_page_map.get(&file_id).unwrap(); // Error if file has not beeen allocated

        let exists = file_map.get(&page_id).is_some();

        if !exists {
            return false;
        }

        {
            let mut manager_guard = self.manager.lock().unwrap();

            let alingend_frame_data = Manager::aligned_buffer(frame_data);

            if manager_guard.write_page(file_id, page_id, &alingend_frame_data).is_ok() {
                return true;
            }
        }

        false
    }
    pub(self) async fn flush_page_async(
        &self,
        file_id: FileId,
        page_id: PageId,
        frame: &[u8]
    ) -> bool {
        // Does file and page exist ?

        let file_map = self.file_page_map.get(&file_id).unwrap(); // Error if file has not beeen allocated

        let exists = file_map.get(&page_id).is_some();

        if !exists {
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

    pub(self) fn init_frame_data(&self, file_id: FileId, page_id: PageId, frame_id: FrameId) {
        // Read page data into memory

        let mut frame_data = [0u8; PAGE_SIZE];

        {
            let mut manager = self.manager.lock().unwrap();

            let buffer: Box<[u8]> = Box::new([0; PAGE_SIZE]);
            let mut page_buffer = Manager::aligned_buffer(&buffer);
            manager.read_page(file_id, page_id, &mut page_buffer).unwrap();

            if page_buffer.len() == PAGE_SIZE {
                frame_data.copy_from_slice(&page_buffer);
            }
        }

        // initialize frame
        let frame = FrameHeader {
            data: frame_data,
            frame_id,

            pin_count: AtomicU32::new(0),
            file_id,
            page_id,
        };

        {
            let mut file_map = self.file_page_map.get_mut(&file_id).unwrap();

            // Maps the Page_ID to a Frame_Id
            file_map.insert(page_id, Some(frame_id));
        }

        let mut frame_guard = self.frames.try_write().unwrap();

        frame_guard.insert(frame_id, Some(RwLock::new(frame)));
        drop(frame_guard);
    }

    pub(self) fn create_guard(&self, frame_id: FrameId, access_type: Protocol) -> PageGuard {
        // Acquire the read lock for the frames map
        let frame_guard = self.frames.read().unwrap();
        FrameGuard::new(frame_id, self.replacer.clone(), frame_guard, access_type)
    }

    pub(self) fn check_write_page(&self, file_id: u64, page_id: PageId) -> Option<WriteGuard> {
        self.check_page(file_id, page_id, Protocol::Exclusive)
            .map(|guard| guard.into_write_guard())
            .expect("Guard Error")
    }

    pub(self) fn check_read_page(&self, file_id: u64, page_id: PageId) -> Option<ReadGuard> {
        self.check_page(file_id, page_id, Protocol::Shared)
            .map(|guard| guard.into_read_guard())
            .expect("Guard Error")
    }

    pub(crate) fn write_page(&self, file_id: u64, page_id: PageId) -> WriteGuard {
        let guard = self.check_write_page(file_id, page_id).expect("Write lock error");

        guard
    }

    pub(crate) fn read_page(&self, file_id: u64, page_id: PageId) -> ReadGuard {
        let guard = self.check_read_page(file_id, page_id).expect("Read lock error");

        guard
    }

    pub(crate) fn get_pin_count(&self, file_id: FileId, page_id: PageId) -> u32 {
        let page_frame_map = self.file_page_map.get(&file_id).unwrap();
        let frame = page_frame_map.get(&page_id).unwrap();

        let frame_option_guard = self.frames.read().unwrap();
        let frame_option = frame_option_guard.get(&frame.unwrap()).unwrap();

        let frame_guard = frame_option.as_ref().expect("Frame").read().unwrap();

        let pin = frame_guard.pin_count.load(Ordering::Relaxed);
        drop(frame_guard);
        pin
    }
}
