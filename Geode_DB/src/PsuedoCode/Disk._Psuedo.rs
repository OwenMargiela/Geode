/*

    Protoype
    Disk Scheduler
        Bus Tub's  scheduler utalized asynchrous shared queues
        to implement an async i/o mechanism. The usage of io_uring
        mitigates the need for our system to do the same. However,
        they are things to be done to optimize the scheduler in ways
        that suit us.

        A fixed sized pool of open file descriptions will be maintained
        to minimize the overhead of the usage of system calls.Eviction
        strategies must be implemented to prevent fd exhaustion.

        A mechanism needs to be instantiated to ensure a correct mapping
        between page to file

        table_ID - unique table identifier
        page_ID  - unique page identifier
                   comprised of the page offset (monotomically increasing ) and the table_ID

        row_ID   - unique tuple identifier
                   comprised of the page_ID and the slot number
*/

/*
    The General idea is to implement a structure in which multiple request can be produced
    asynchronously. A method that allows for us as the programmer to know when one a given disk
    operation was successful is should also be put in place. Any mechanism used to manage
    amd execute task efficiently, concurrently and thread safe is good enough.


    Non blocking operation

    Blocking when conformation that the task has bin commited sucessfully is needed


*/

/*
use io_uring::{IoUring, opcode, types};
use std::{
    collections::HashMap,
    fs::OpenOptions,
    os::unix::io::AsRawFd,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
    thread,
};

struct IoState {
    ring: IoUring,
    wakers: HashMap<u64, Waker>, // Stores wakers mapped to user_data
    // include a mechanism to reset the ID
    nextID(1)
}

impl IoState {

    fn new(entries: u32) -> Arc<Mutex<Self>> {
        Arc::new(Mutex::new(Self {
            ring: IoUring::new(entries).expect("Failed to create io_uring"),
            wakers: HashMap::new(),
        }))
    }

    fn start_completion_handler(state: Arc<Mutex<Self>>) {
        thread::spawn(move || loop {
            let mut state_guard = state.lock().unwrap();
            let ring = &mut state_guard.ring;

            ring.submit_and_wait(1).expect("Failed to wait for completion");


            while let Some(cqe) = ring.completion().available().next() {
                if let Some(waker) = state_guard.wakers.remove(&cqe.user_data()) {
                    waker.wake(); // Wake up the future! POLLS THE POLLS THATS WHY THE POLLS WORK!
                }
            }
        });
    }
}

struct IoUringFuture {
    state: Arc<Mutex<IoState>>,  // Shared io_uring state
    fd: i32,
    buffer: Arc<Mutex<Vec<u8>>>, // Buffer for read/write
    operation_id: u64, // Unique ID for this operation
}

impl Future for IoUringFuture {
    type Output = Result<Vec<u8>, std::io::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state_guard = self.state.lock().unwrap();

        // Store the waker for wakeup
        state_guard.wakers.insert(self.operation_id, cx.waker().clone());

        // Check if the operation is already complete
        if let Some(cqe) = state_guard.ring.completion().available().next() {
            if cqe.user_data() == self.operation_id {
                let result = cqe.result();
                if result < 0 {
                    return Poll::Ready(Err(std::io::Error::from_raw_os_error(-result)));
                }

                let mut buffer_guard = self.buffer.lock().unwrap();
                buffer_guard.truncate(result as usize);
                return Poll::Ready(Ok(buffer_guard.clone()));
            }
        }

        Poll::Pending
    }
}

impl IoUringFuture {
    fn new(state: Arc<Mutex<IoState>>, fd: i32, buffer_size: usize, operation_id: u64) -> Self {
        let buffer = Arc::new(Mutex::new(vec![0u8; buffer_size]));

        {
            let mut state_guard = state.lock().unwrap();
            let ring = &mut state_guard.ring;
            let buffer_ptr = buffer.lock().unwrap().as_mut_ptr() as *mut _;

            unsafe {
                let sqe = ring.submission().available().next().expect("No SQEs available");
                sqe.prep_read(fd, buffer_ptr, buffer_size as u32, 0);
                sqe.set_user_data(operation_id);
            }

            ring.submit().expect("Failed to submit io_uring request");
        }

        Self { state, fd, buffer, operation_id }
    }
}

#[tokio::main]
async fn main() {
    let state = IoState::new(4);
    IoState::start_completion_handler(state.clone());

    let file = OpenOptions::new().read(true).open("test.txt").unwrap();
    let fd = file.as_raw_fd();

    // Read example
    // replace manually id with a monotomically increasing
    // atomic ID
    // implememnt the pool of file description with lru eviction or since i'm gonna implement lru k ig that one

    let read_future = IoUringFuture::new(state.clone(), fd, 4096, 1);

    match read_future.await {
        Ok(data) => println!("Read: {:?}", String::from_utf8_lossy(&data)),
        Err(e) => eprintln!("Read Error: {:?}", e),
    }
}
*/

/*

    Disk Request
        is_write
        data
        page_id
        table_unique_identifier

    FdPool
        acknowledge that the os can handle only so much open file descriptors at a time
        file_descriptors : Vec< FileDescriptors >


*/

/*

    Where am I putting these files, surely not within the code base?
    In another directory perhaps?
    
    // Attributes
    
    If anything, a shared queue can be implemented if the IO is filled to capacity to aid with retries.
    This is more of an optimization.

    Disk scheduler
        state: IoState // monotomically increasing ID
        fd_pool: FdPool

    
    // Methods

    Disk scheduler
        schedule( DiskRequest ) -> IoFuture


*/
