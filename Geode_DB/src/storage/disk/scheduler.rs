use std::{
    future::Future,
    sync::{
        atomic::{AtomicU8, Ordering},
        mpsc::{self, Receiver, Sender},
        Arc, Mutex,
    },
    task::{Poll, Waker},
};

use super::manager::Manager;

// Enum representing different states of the I/O operation.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IoStatus {
    Pending = 0,    // Operation is still in progress
    Success = 1,    // Operation completed successfully
    WriteError = 2, // Write operation failed
    ReadError = 3,  // Read operation failed
}

impl IoStatus {
    fn from_u8(value: u8) -> Self {
        match value {
            1 => IoStatus::Success,
            2 => IoStatus::WriteError,
            3 => IoStatus::ReadError,
            _ => IoStatus::Pending,
        }
    }
}

pub struct IoFuture {
    pub flag: Arc<AtomicU8>,
    pub waker: Arc<Mutex<Option<Waker>>>,
}

impl Future for IoFuture {
    type Output = ();

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        match IoStatus::from_u8(self.flag.load(Ordering::Acquire)) {
            IoStatus::Success | IoStatus::WriteError | IoStatus::ReadError => {
                Poll::Ready(()) // Indicating completion
            }

            IoStatus::Pending => {
                {
                    let mut waker_guard = self.waker.lock().unwrap();
                    *waker_guard = Some(cx.waker().clone());
                }

                Poll::Pending // Still processing
            }
        }
    }
}

pub enum DiskData {
    Write(Option<Box<[u8]>>),
    Read(Option<Arc<Mutex<Box<[u8]>>>>),
}

// Struct representing a request to perform disk I/O.
pub struct DiskRequest {
    // Flag indicating whether the request is a write or a read.
    pub is_write: bool,

    // Data buffer for writes, or shared reference to buffer for reads.
    pub data: DiskData,

    // ID of the page being read from / written to disk.
    pub page_id: u32,

    // ID of the file being read from / written to disk.
    pub file_id: u64,

    // A future to signal to the request issuer when the request has been completed.
    pub done_flag: Arc<AtomicU8>,
    pub waker: Arc<Mutex<Option<Waker>>>,
}

// Struct for scheduling disk I/O operations asynchronously.

pub struct DiskScheduler {
    manager: Arc<Mutex<Manager>>,
    shared_queue: (Sender<DiskRequest>, Option<Receiver<DiskRequest>>),
}

impl DiskScheduler {
    pub fn new(manager: Arc<Mutex<Manager>>) -> Self {
        // let (log_file, log_file_path) = Manager::open_log();
        let (tx, rx) = mpsc::channel();

        let mut scheduler = Self {
            manager: manager,
            shared_queue: (tx, Some(rx)),
        };

        scheduler.start_worker_queue();
        scheduler
    }

    pub fn start_worker_queue(&mut self) {
        let rx = self.shared_queue.1.take().unwrap();
        let manager = Arc::clone(&self.manager);

        std::thread::spawn(move || loop {
            while let Ok(request) = rx.recv() {
                let mut manager_guard = manager.lock().unwrap();

                // Attempt to perform the I/O operation.
                // Any failure should update the `done_flag` but should not crash the worker thread.

                if request.is_write {
                    if let DiskData::Write(Some(data)) = request.data {
                        match manager_guard.write_page(request.file_id, request.page_id, &data) {
                            Ok(_) => {
                                request
                                    .done_flag
                                    .store(IoStatus::Success as u8, Ordering::Release);
                            }
                            Err(_) => request
                                .done_flag
                                .store(IoStatus::WriteError as u8, Ordering::Release),
                        };
                    }
                } else {
                    if let DiskData::Read(Some(buffer)) = &request.data {
                        let mut buffer_lock = buffer.lock().unwrap();
                        match manager_guard.read_page(
                            request.file_id,
                            request.page_id,
                            &mut buffer_lock,
                        ) {
                            Ok(_) => request
                                .done_flag
                                .store(IoStatus::Success as u8, Ordering::Release),
                            Err(_) => request
                                .done_flag
                                .store(IoStatus::ReadError as u8, Ordering::Release),
                        };
                    }
                }
                if let Some(waker) = request.waker.lock().unwrap().take() {
                    waker.wake();
                }
            }
        });
    }

    // Creates a future to track the status of a disk request.

    pub fn create_future(&self) -> IoFuture {
        IoFuture {
            flag: Arc::new(AtomicU8::new(IoStatus::Pending as u8)),

            waker: Arc::new(Mutex::new(None)),
        }
    }

    // Schedules a disk request for processing.

    pub fn schedule(&self, request: DiskRequest) {
        let tx = &self.shared_queue.0;
        tx.send(request).expect("Failed to send disk request");
    }

    
}

#[cfg(test)]
pub mod test {
    use std::sync::{Arc, Mutex};

    use crate::storage::{disk::manager::Manager, page::page_constants::PAGE_SIZE};

    use super::{DiskData, DiskRequest, DiskScheduler};

    #[tokio::main]
    #[test]
    async fn scheduler_test() {
        let (log_file, log_file_path) = Manager::open_log();
        let manager = Arc::new(Mutex::new(Manager::new(log_file, log_file_path)));
        let scheduler = DiskScheduler::new(Arc::clone(&manager));
        let future_one = scheduler.create_future();

        let (file_id, _) = scheduler
            .manager
            .lock()
            .unwrap()
            .create_db_file()
            .expect("File made");

        // Lock contention allow for functionality to check if a page needs to be allocated thats automatic
        let mut gurad = scheduler.manager.lock().unwrap();
        let (page_id, _) = gurad.allocate_page(file_id);
        drop(gurad);

        // Write Request
        let data = [1; PAGE_SIZE];
        let page_data = Manager::aligned_buffer(&data);

        let request = DiskRequest {
            data: DiskData::Write(Some(page_data)), // Move the buffer
            done_flag: Arc::clone(&future_one.flag),
            file_id,
            is_write: true,
            page_id,
            waker: Arc::clone(&future_one.waker),
        };

        scheduler.schedule(request);

        // Read Request
        let future_two = scheduler.create_future();
        let page_buffer = Arc::new(Mutex::new(Manager::aligned_buffer(&vec![0; PAGE_SIZE])));

        let request = DiskRequest {
            data: DiskData::Read(Some(Arc::clone(&page_buffer))), // Shared buffer reference
            done_flag: Arc::clone(&future_two.flag),
            file_id,
            is_write: false,
            page_id,
            waker: Arc::clone(&future_two.waker),
        };

        scheduler.schedule(request);

        future_one.await;
        future_two.await;

        // Verify Read &Write
        let read_data = page_buffer.lock().unwrap();

        assert_eq!(&**read_data, &data, "Page read mismatch!");
    }
}
