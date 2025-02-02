pub mod catalog;
pub mod db_types;
pub mod storage;

use std::sync::Arc;
use tokio::sync::oneshot;

struct DiskScheduler;

impl DiskScheduler {
    async fn schedule(&self, is_write: bool, data: &str, page_id: usize, tx: oneshot::Sender<bool>) {
        if is_write {
            println!("Writing data to page {} {}", page_id, data);
        } else {
            println!("Reading data from page {}", page_id);
        }
        let _ = tx.send(true);
    }
}

#[tokio::main]
async fn main() {
    let disk_scheduler = Arc::new(DiskScheduler);

    let (tx1, rx1) = oneshot::channel();
    let (tx2, rx2) = oneshot::channel();

    let data = "A test string.";

    let scheduler1 = disk_scheduler.clone();
    let scheduler2 = disk_scheduler.clone();

    tokio::spawn(async move {
        scheduler1.schedule(true, data, 0, tx1).await;
    });

    tokio::spawn(async move {
        scheduler2.schedule(false, &data, 0, tx2).await;
    });

    assert!(rx1.await.unwrap());
    assert!(rx2.await.unwrap());
}

