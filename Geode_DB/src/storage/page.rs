use std::sync::atomic::{AtomicBool, AtomicI32};
pub mod page_constants {
    pub const PAGE_SIZE: usize = 4096;
}

pub struct Page {
    data: Vec<u8>,
    pin_count: AtomicI32,
    isDirty: AtomicBool,
}
