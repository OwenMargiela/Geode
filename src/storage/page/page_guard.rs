#![allow(unused_variables)] 
#![allow(dead_code)] 

use std::sync::{atomic::Ordering, Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use hashlink::LinkedHashMap;

use crate::{
    buffer::buffer_pool_manager::{FrameHeader, FrameId, Protocol},
    utils::replacer::{LRUKReplacer, Replacer},
};



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

    pub fn into_read_guard_ref(&self) -> Option<&ReadGuard<'a>> {
        if let PageGuard::ReadGuard(guard) = self {
            Some(&guard)
        } else {
            None
        }
    }

      pub fn into_write_guard_ref(&self) -> Option<&WriteGuard<'a>> {
        if let PageGuard::WriteGuard(guard) = self {
            Some(&guard)
        } else {
            None
        }
    }
}

pub struct FrameGuard<'a> {
    frame_id: u32,
    map_guard: Arc<RwLockReadGuard<'a, LinkedHashMap<u32, Option<RwLock<FrameHeader>>>>>,
    on_drop: Box<dyn Fn(u32, bool) + 'a>,
    is_valid: bool,
}

impl<'a> FrameGuard<'a> {
    pub fn new(
        frame_id: u32,
        replacer: Arc<Mutex<LRUKReplacer<u32>>>,
        map_guard: RwLockReadGuard<'a, LinkedHashMap<u32, Option<RwLock<FrameHeader>>>>,
        access_type: Protocol,
    ) -> PageGuard<'a> {
        {
            let frame_guard = map_guard
                .get(&frame_id)
                .unwrap()
                .as_ref()
                .expect("Valid frame");
            let frame = frame_guard.write().unwrap();
            frame.pin_count.fetch_add(1, Ordering::Relaxed);
        }

        let on_drop = Box::new(move |frame_id: u32, evictabilility: bool| {
            let mut replacer_guard = replacer.lock().unwrap();

            replacer_guard.record_access(frame_id);
            replacer_guard.set_evictable(frame_id, evictabilility);

            drop(replacer_guard);
        });

        on_drop(frame_id, false);

        let frame = FrameGuard {
            frame_id,
            map_guard: Arc::new(map_guard),
            is_valid: false,
            on_drop,
        };

        match access_type {
            Protocol::Exclusive => PageGuard::WriteGuard(WriteGuard::new(frame)),
            Protocol::Shared => PageGuard::ReadGuard(ReadGuard::new(frame)),
        }
    }
}

impl<'a> Drop for FrameGuard<'a> {
    fn drop(&mut self) {
        let frame_guard = self
            .map_guard
            .get(&self.frame_id)
            .unwrap()
            .as_ref()
            .expect("Valid frame");

        let frame = frame_guard.write().unwrap();
        frame.pin_count.fetch_sub(1, Ordering::Release);

        (self.on_drop)(self.frame_id, true);
    }
}

pub struct WriteGuard<'a> {
    _frame_id: FrameId,
    _frame: FrameGuard<'a>,
}

impl<'a> WriteGuard<'a> {
    pub fn new(_frame: FrameGuard<'a>) -> Self {
        let _frame_id = _frame.frame_id;

        Self { _frame, _frame_id }
    }

    pub fn get_frame(&self) -> RwLockWriteGuard<'_, FrameHeader> {
        let frame_guard = self
            ._frame
            .map_guard
            .get(&self._frame_id)
            .unwrap()
            .as_ref()
            .expect("Valid frame");

        let frame = frame_guard.write().unwrap();
        frame
    }
}

pub struct ReadGuard<'a> {
    _frame_id: FrameId,
    _frame: FrameGuard<'a>,
}

impl<'a> ReadGuard<'a> {
    pub fn new(_frame: FrameGuard<'a>) -> Self {
        let _frame_id = _frame.frame_id;

        Self { _frame, _frame_id }
    }

    pub fn get_frame(&self) -> RwLockReadGuard<'_, FrameHeader> {
        let frame_guard = self
            ._frame
            .map_guard
            .get(&self._frame_id)
            .unwrap()
            .as_ref()
            .expect("Valid frame");

        let frame = frame_guard.read().unwrap();
        frame
    }
}
