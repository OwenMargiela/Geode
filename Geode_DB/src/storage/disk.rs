use std::{
    collections::HashMap,
    fs::File,
    future::Future,
    os::fd::{AsFd, AsRawFd},
    sync::{atomic::AtomicU64, Arc, Mutex},
    task::Waker,
    thread,
};

use io_uring::{opcode, types::Fd, IoUring};

use super::page::page_constants::PAGE_SIZE;

// These can go in their own files

// Replaced with a file's unique identifier
#[derive(Eq, Hash, PartialEq)]
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
    file_descriptors: HashMap<TableId, File>, // Replacer algorithm here
    
}

pub struct DiskRequest {
    is_write: bool,
    data: Option<[u8; PAGE_SIZE]>,
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
    fd: Fd,
    buffer: Option<[u8; PAGE_SIZE]>,
    operation_id: u64,
}

// Pages page placed in free slots inherit the ID of the page deleted
pub struct DiskManager {
    fd_pool: FdPool,
    state: Arc<Mutex<IoState>>,
}

// Monday activity

impl DiskManager {
    fn new(entries: u32) -> Self {
        // for now, 8 is the default number of entries
        Self {
            fd_pool: FdPool {
                file_descriptors: HashMap::new(),
            },
            state: IoState::new(entries),
        }
    }
    fn write_page(&self, request: DiskRequest) -> Result<IoFuture, i8> {
        if !request.is_write || request.data.is_none() {
            return Err(-1);
        }

        let mut buffer: [u8; PAGE_SIZE];
        buffer = request.data.unwrap();

        {
            let mut state_guard = self.state.lock().unwrap();
            let op_id = state_guard
                .current_id
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

            let ring = &mut state_guard.ring;
            let buffer_ptr = buffer.as_mut_ptr();

            let table_id = TableId {
                value: u16::from_le_bytes([request.page_id.value[0], request.page_id.value[1]]),
            };

            // Direct I/o by opening the file
            // Add an option for not opening with direct I/o for benchmarks
            if let fd = self.fd_pool.file_descriptors.get(&table_id).is_none() {}
            let fd = Fd(fd);

            let entry = opcode::Write::new(fd, buffer_ptr, buffer.len() as _)
                .build()
                .user_data(op_id);

            unsafe {
                ring.submission().push(&entry);
            }

            Ok(IoFuture {
                state: self.state.clone(),
                fd: fd,
                buffer: None,
                operation_id: op_id,
            })
        }
    }

    // fn read_page(request: DiskRequest) -> IoFuture {};

    // fn delete_page(request: DiskRequest) -> IoFuture{};
}

/*
   API rundown

   let table file = OpenOptions::new()
       .read(true)
       .write(true)
       .custom_flags(O_DIRECT)
       .open(DEFAULT_TABLE_NAME);

   let inode_id = file.metadata().ino()

   OR

   Cross platfor solution using file-metadata

   let file_id = table file.metadata().file_id()

   OR

   Cross platform solution using FileDescriptor::unique_id()
*/

/*
  table_id
  fd_pool.insert(table_id, table)

  let new_request = DiskRequest {
    is_write: bool,
    data: Option<[u8; PAGE_SIZE]>,
    page_id: PageId, // Should contain the table_id

  }

  DiskManager.write( DiskRequest )
        destructures ithe table id out of the page_id
        let page_table_id = page_id.get_table();

        fd  = fd.get(page_table_id)
            if not in the pool, get the path of table using its id
            then open it, evict a viable victim to insert the new fd into the 
            cache, then return the fd for the new file
        
        use the fd for io_uring operations

        let future = write(fd....).unwrap().



*/

#[cfg(test)]
mod test {

    #[test]
    fn io_test() {}
}
