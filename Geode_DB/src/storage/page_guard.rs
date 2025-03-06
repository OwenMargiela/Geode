use std::sync::{atomic::Ordering, Arc, Mutex, RwLockReadGuard, RwLockWriteGuard};

use crate::{
    buffer::buffer_pool_manager::{BufferPoolManager, FrameHeader},
    utils::replacer::{LRUKReplacer, Replacer},
};

use super::disk::scheduler::DiskScheduler;

pub enum PageGuard<'a> {
    WriteGuard(WriteGuard<'a>),
    ReadGuard(ReadGuard<'a>),
}

impl<'a> PageGuard<'a> {
    pub fn into_read_guard(self) -> Option<ReadGuard<'a>> {
        if let PageGuard::ReadGuard(guard) = self {
            Some(guard)
        } else {
            None
        }
    }

    pub fn into_write_guard(self) -> Option<WriteGuard<'a>> {
        if let PageGuard::WriteGuard(guard) = self {
            Some(guard)
        } else {
            None
        }
    }
}
pub struct WriteGuard<'a> {
    pub frame: RwLockWriteGuard<'a, FrameHeader>,
    page_id: u32,
    replacer: Arc<Mutex<LRUKReplacer<u32>>>,
    is_valid: bool,
}

impl<'a> WriteGuard<'a> {
    pub fn new(
        page_id: u32,
        frame: RwLockWriteGuard<'a, FrameHeader>,
        replacer: Arc<Mutex<LRUKReplacer<u32>>>,
    ) -> Self {
        frame.pin_count.fetch_add(1, Ordering::Relaxed);

        let mut replacer_guard = replacer.try_lock().unwrap();
        dbg!("Aqquired replacer guard");
        replacer_guard.record_access(frame.frame_id.clone());
        replacer_guard.set_evictable(frame.frame_id, false);
        drop(replacer_guard);
        dbg!("Dropped replacer guard");

        dbg!("Aqquired write guard");
        Self {
            frame,
            is_valid: false,
            page_id,
            replacer,

        }
    }
}

impl<'a> Drop for WriteGuard<'a> {
    fn drop(&mut self) {
        dbg!("Dropping write guard");
        self.frame.pin_count.fetch_sub(1, Ordering::Release);
        let mut replacer_guard = self.replacer.lock().unwrap();
        replacer_guard.set_evictable(self.frame.frame_id, true);

        
    }
}

pub struct ReadGuard<'a> {
    page_id: u32,
    pub frame: RwLockReadGuard<'a, FrameHeader>,
    replacer: Arc<Mutex<LRUKReplacer<u32>>>, // Why do we need you actually?
    scheduler: Arc<Mutex<DiskScheduler>>,
    is_valid: bool,
    bpm_latch: Arc<Mutex<&'a BufferPoolManager>>,
}

impl<'a> ReadGuard<'a> {
    pub fn new(
        page_id: u32,
        frame: RwLockReadGuard<'a, FrameHeader>,
        replacer: Arc<Mutex<LRUKReplacer<u32>>>,
        scheduler: Arc<Mutex<DiskScheduler>>,
        bpm_latch: Arc<Mutex<&'a BufferPoolManager>>,
    ) -> Self {
        frame.pin_count.fetch_add(1, Ordering::Relaxed);

        let mut replacer_guard = replacer.lock().unwrap();
        replacer_guard.record_access(frame.frame_id.clone());
        replacer_guard.set_evictable(frame.frame_id, false);
        drop(replacer_guard);

        Self {
            bpm_latch,
            frame,
            is_valid: false,
            page_id,
            replacer,
            scheduler,
        }
    }

    pub fn get_data(&self) -> &Box<[u8]> {
        &self.frame.data
    }
}

impl<'a> Drop for ReadGuard<'a> {
    fn drop(&mut self) {
        self.frame.pin_count.fetch_sub(1, Ordering::Release);
        print!("Dropping");

        if self.frame.pin_count.load(Ordering::SeqCst) == 0 {
            let mut replacer_guard = self.replacer.lock().unwrap();
            replacer_guard.set_evictable(self.frame.frame_id, true);
        }
    }
}
