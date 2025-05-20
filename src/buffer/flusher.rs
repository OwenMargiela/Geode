#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{ collections::VecDeque, sync::{ atomic::{ AtomicU32, Ordering }, Arc, RwLock } };

use anyhow::Ok;

use dashmap::DashMap;

use crate::{
    index::tree::tree_node::node_type::PagePointer,
    storage::page::btree_page_layout::PAGE_SIZE,
};

use super::buffer_pool_manager::BufferPoolManager;

#[derive(Debug)]
enum Status {
    Running,
    Completed,
}

#[derive(Debug)]
struct LockData {
    guard: Lock,
    ticker: AtomicU32,
    txn_num: u32,
    txn_status: Status,
    page_id: u32,
}

#[derive(Debug)]
pub enum Lock {
    EXLOCK,
    SHLOCK,
}

pub struct Context {
    stack: Arc<RwLock<VecDeque<PagePointer>>>,
    context_type: Option<Lock>,
}

pub struct Flusher {
    pub(self) inner: Arc<BufferPoolManager>,
    file: u64,

    pub(self) lock_table: DashMap<u32, LockData>,

    pub(self) txn_ticker: AtomicU32,

    pub(self) context: Context,
}

impl Flusher {
    pub fn new(bpm: Arc<BufferPoolManager>, file: u64) -> Self {
        Self {
            inner: bpm,
            file,
            lock_table: DashMap::new(),

            txn_ticker: AtomicU32::new(0),
            context: Context {
                stack: Arc::new(RwLock::new(VecDeque::new())),
                context_type: None,
            },
        }
    }
    pub fn new_page(&self) -> u32 {
        self.inner.new_page(self.file)
    }

    pub fn new_file(&self) -> u64 {
        self.inner.allocate_file()
    }

    pub fn aqquire_ex(&self, page_id: u32) -> anyhow::Result<()> {
        if let Some(lock_data) = self.lock_table.get(&page_id) {
            if let Lock::EXLOCK = lock_data.guard {
                dbg!("");
                self.log_transaction(page_id);
                return Err(anyhow::Error::msg("EX lock has already been granted"));
            }
            self.lock_table.remove(&page_id).unwrap();
        }

        let guard = self.inner.write_page(self.file, page_id);

        self.lock_table.insert(page_id, LockData {
            guard: Lock::EXLOCK,
            txn_num: self.txn_ticker.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
            ticker: AtomicU32::new(1),
            txn_status: Status::Running,
            page_id,
        });

        Ok(())
    }

    pub(self) fn aqquire_sh(&self, page_id: u32) -> anyhow::Result<()> {
        if let Some(lock_data) = self.lock_table.get(&page_id) {
            if let Lock::EXLOCK = lock_data.guard {
                dbg!("");
                self.log_transaction(page_id);
                return Err(anyhow::Error::msg("EX lock has already been granted"));
            }
            self.lock_table.remove(&page_id).unwrap();
        }

        let guard = self.inner.read_page(self.file, page_id);

        if let Some(lock) = self.lock_table.get(&page_id) {
            lock.ticker.fetch_add(1, Ordering::SeqCst);

            match lock.guard {
                Lock::SHLOCK => {
                    lock.ticker.fetch_add(1, Ordering::SeqCst);
                }
                _ => {
                    dbg!("");
                    self.log_transaction(page_id);
                    return Err(anyhow::Error::msg("EX lock has already been granted"));
                }
            }
        } else {
            self.lock_table.insert(page_id, LockData {
                guard: Lock::SHLOCK,
                txn_num: self.txn_ticker.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                ticker: AtomicU32::new(1),
                txn_status: Status::Running,
                page_id,
            });
        }

        Ok(())
    }

    pub fn aqquire_context_ex(&self, mut page_id_stack: VecDeque<u32>) -> anyhow::Result<()> {
        loop {
            if let Some(page_id) = page_id_stack.pop_front() {
                self.aqquire_ex(page_id).unwrap();

                self.context.stack.try_write().unwrap().push_back(page_id);
            } else {
                break;
            }
        }

        Ok(())
    }

    // Lazy gaurd eviction?
    pub fn read_drop(&self, page_id: u32) -> [u8; PAGE_SIZE] {
        self.aqquire_sh(page_id).unwrap();
        let page = self.read(page_id).unwrap();
        self.lock_table.remove(&page_id).unwrap();

        page
    }

    pub(self) fn read(&self, page_id: u32) -> anyhow::Result<[u8; PAGE_SIZE]> {
        if let Some(lock) = self.lock_table.get(&page_id) {
            match lock.guard {
                Lock::EXLOCK => {
                    dbg!("");
                    self.log_transaction(page_id);
                    return Err(
                        anyhow::Error::msg("Incompatible Locks.Expected Shlock found Exlock")
                    );
                }

                _ => {
                    let guard = self.inner.read_page(self.file, page_id);
                    let data = guard.get_frame().data;

                    drop(guard);
                    return Ok(data);
                }
            }
        }

        return Err(anyhow::Error::msg("Guard does not exists"));
    }

    pub(self) fn write_all(&self, page_id: u32, data: [u8; PAGE_SIZE]) -> anyhow::Result<()> {
        if let Some(lock) = self.lock_table.get(&page_id) {
            match lock.guard {
                Lock::SHLOCK => {
                    dbg!("");
                    self.log_transaction(page_id);
                    return Err(
                        anyhow::Error::msg("Incompatible Locks.Expected Exlock found Shlock")
                    );
                }

                _ => {
                    let guard = self.inner.write_page(self.file, page_id);

                    guard.get_frame().data.copy_from_slice(&data);
                    drop(guard);

                    return Ok(());
                }
            }
        }

        return Err(anyhow::Error::msg("Guard does not exists"));
    }

    pub fn pop_flush(&self, data: [u8; PAGE_SIZE], page_id: u32) -> anyhow::Result<()> {
        let context = &self.context;

        if context.stack.try_read().unwrap().len() > 0 {
            let id = context.stack.try_write().unwrap().pop_front().unwrap();

            if id == page_id {
                self.write_all(page_id, data)?;
            } else {
                println!("Not equal");
            }
        }

        self.lock_table.remove(&page_id).unwrap();
        Ok(())
    }

    pub fn pop_flush_test(&self, data: [u8; PAGE_SIZE]) -> anyhow::Result<()> {
        let context = &self.context;

        let id: u32;
        if context.stack.try_read().unwrap().len() > 0 {
            id = context.stack.try_write().unwrap().pop_front().unwrap();

            self.write_all(id, data)?;
            self.lock_table.remove(&id).unwrap();
        }

        Ok(())
    }

    // Transaction ticket to coordinate who can and cant read top?
    pub fn read_top(&self) -> anyhow::Result<[u8; PAGE_SIZE]> {
        let context = &self.context;
        let stack = context.stack.try_read().unwrap();
        if stack.len() > 0 {
            let data = stack.front().unwrap();

            let gaurd = self.inner.read_page(self.file, data.clone());
            let data = gaurd.get_frame().data;
            drop(gaurd);

            return Ok(data);
        }

        return Err(anyhow::Error::msg("Invalid stack len"));
    }

    pub fn write_flush(&self, data: [u8; PAGE_SIZE], page_id: u32) -> anyhow::Result<()> {
        self.aqquire_ex(page_id)?;
        self.write_all(page_id, data)?;

        self.lock_table.remove(&page_id).unwrap();
        Ok(())
    }

    pub fn release_ex(&self) -> anyhow::Result<()> {
        let context = &self.context;

        let id: u32;
        if context.stack.try_read().unwrap().len() > 0 {
            id = context.stack.try_write().unwrap().pop_front().unwrap();

            self.lock_table.remove(&id).unwrap();
        }

        Ok(())
    }

    pub fn log_transaction(&self, page_id: u32) {
        let txn = self.lock_table.get(&page_id).unwrap();

        dbg!(&txn.page_id);
        dbg!(&txn.txn_status);
        dbg!(&txn.txn_num);
        dbg!(&txn.ticker);
        dbg!(&txn.guard);
    }
}
