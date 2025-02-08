use std::{
    collections::HashMap, fs::File, future::Future, sync::{
        atomic:: AtomicU64,
        Arc, Mutex,
    }, task::Waker, thread
};

use io_uring::IoUring;

use super::page::page_constants::PAGE_SIZE;

// These can go in their own files
pub struct TableId {
    value: u16,
}
pub struct PageId {
    value: [u8; 4],
}

pub struct RowID {
    value: [u8; 6],
}

pub struct FdPool {
    file_descriptors: Vec<File>, // Replacer algorithm here
}

pub struct DiskRequest {
    is_write: bool,
    data: [u8; PAGE_SIZE],
    page_id: PageId,
}

// For some reason MMaps are used to implemenet the completion queue and submission queue
// Analyze why.....when? IDK
struct IoState {
    ring: IoUring,
    wakers: Arc<Mutex<HashMap<u64, Waker>>>,
    current_id: AtomicU64,
}

impl IoState {
    fn new(entries: u32) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            ring: IoUring::new(entries).expect("Failed to create IoUring"),
            wakers: Arc::new(Mutex::new(HashMap::new())),
            current_id: AtomicU64::new(0),
        }))
    }

    fn start_completion_handler(state: Arc<Mutex<Self>>) {
        thread::spawn(move || loop {
            {
                let mut state_guard = state.lock().unwrap();
                let ring = &mut state_guard.ring;
                ring.submit_and_wait(1)
                    .expect("Failed to wair for completion");
            }

            while let Some(cqe) = {
                let mut state_guard = state.lock().unwrap();
                let mut completion_queue = state_guard.ring.completion();
                completion_queue.next()
            } {
                let wakers = {
                    let state_guared = state.lock().unwrap();
                    state_guared.wakers.clone()
                };

                let mut wakers_guard = wakers.lock().unwrap();
                if let Some(waker) = wakers_guard.remove(&cqe.user_data()) {
                    waker.wake();
                }
            }
        });
    }
}

pub struct IoFuture {
    state: Arc<Mutex<IoState>>,
    fd: i32,
    buffer: [u8; PAGE_SIZE],
    operation_id: u64,
}

pub trait AsyncDiskOps {
    fn uring_write(request: DiskRequest) -> impl Future;

    fn uring_read(request: DiskRequest) -> impl Future;
}