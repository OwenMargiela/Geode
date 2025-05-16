#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

use anyhow::Ok;
use crossbeam_queue::SegQueue;
use dashmap::DashMap;

use crate::storage::page::{btree_page_layout::PAGE_SIZE, page_guard::PageGuard};

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
}

#[derive(Debug)]
pub enum Lock {
    EXLOCK,
    SHLOCK,
}

pub struct Context<'a> {
    stack: SegQueue<PageGuard<'a>>,
    context_type: Lock,
}

pub struct Flusher<'a> {
    inner: Arc<BufferPoolManager>,
    file: u64,

    lock_table: DashMap<u32, LockData>,
    guard_table: DashMap<u32, PageGuard<'a>>,

    txn_ticker: AtomicU32,

    context: Context<'a>,
}

impl<'a> Flusher<'a> {
    pub fn new_page(&self) -> u32 {
        self.inner.new_page(self.file)
    }

    pub fn new_file(&self) -> u64 {
        self.inner.allocate_file()
    }

    pub fn aqquire_ex(&'a self, page_id: u32) -> anyhow::Result<()> {
        if let Some(lock_data) = self.lock_table.get(&page_id) {
            if let Lock::EXLOCK = lock_data.guard {
                self.log_transaction(page_id);
                return Err(anyhow::Error::msg("EX lock has already been granted"));
            }
            self.lock_table.remove(&page_id).unwrap();
        }

        let guard = self.inner.write_page(self.file, page_id);

        self.lock_table.insert(
            page_id,
            LockData {
                guard: Lock::EXLOCK,
                txn_num: self
                    .txn_ticker
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                ticker: AtomicU32::new(1),
                txn_status: Status::Running,
            },
        );

        self.guard_table
            .insert(page_id, PageGuard::WriteGuard(guard));

        Ok(())
    }

    pub fn aqquire_sh(&self, page_id: u32) -> anyhow::Result<()> {
        if let Some(lock_data) = self.lock_table.get(&page_id) {
            if let Lock::EXLOCK = lock_data.guard {
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
                    self.log_transaction(page_id);
                    return Err(anyhow::Error::msg("EX lock has already been granted"));
                }
            }
        } else {
            self.lock_table.insert(
                page_id,
                LockData {
                    guard: Lock::SHLOCK,
                    txn_num: self
                        .txn_ticker
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                    ticker: AtomicU32::new(1),
                    txn_status: Status::Running,
                },
            );
        }

        Ok(())

      
    }

    pub fn aqquire_context_ex(&'a self, mut page_id_stack: VecDeque<u32>) -> anyhow::Result<()> {
        loop {
            if let Some(page_id) = page_id_stack.pop_front() {
                if let Some(lock_data) = self.lock_table.get(&page_id) {
                    if let Lock::EXLOCK = lock_data.guard {
                        self.log_transaction(page_id);
                        return Err(anyhow::Error::msg("EX lock has already been granted"));
                    }
                    self.lock_table.remove(&page_id).unwrap();
                }

                let guard = self.inner.write_page(self.file, page_id);

                self.context.stack.push(PageGuard::WriteGuard(guard));
            } else {
                break;
            }
        }

        Ok(())
    }

    pub fn read(&self, page_id: u32) -> anyhow::Result<[u8; PAGE_SIZE]> {
        if let Some(lock) = self.lock_table.get(&page_id) {
            match lock.guard {
                Lock::EXLOCK => {
                    self.log_transaction(page_id);
                    return Err(anyhow::Error::msg(
                        "Incompatible Locks.Expected Shlock found Exlock",
                    ));
                }

                _ => {
                    let guard = self.guard_table.get(&page_id);

                    if guard.is_some() {
                        let guard = guard.unwrap();
                        let read_guard = guard.into_read_guard_ref().unwrap();
                        let data = read_guard.get_frame().data;
                        return Ok(data);
                    }
                }
            }
        }

        return Err(anyhow::Error::msg("Guard does not exists"));
    }

    pub fn write_all(&self, page_id: u32, data: [u8; PAGE_SIZE]) -> anyhow::Result<()> {
        if let Some(lock) = self.lock_table.get(&page_id) {
            match lock.guard {
                Lock::SHLOCK => {
                    self.log_transaction(page_id);
                    return Err(anyhow::Error::msg(
                        "Incompatible Locks.Expected Exlock found Shlock",
                    ));
                }

                _ => {
                    let (_, guard) = self.guard_table.remove(&page_id).unwrap();

                    self.lock_table.remove(&page_id).unwrap();

                    let guard = guard.into_write_guard().unwrap();
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

        if context.stack.len() > 0 {
            let guard = context.stack.pop().unwrap();

            self.lock_table.remove(&page_id).unwrap();

            let guard = guard.into_write_guard().unwrap();
            guard.get_frame().data.copy_from_slice(&data);

            drop(guard);
        }
        unimplemented!()
    }

    pub fn log_transaction(&self, page_id: u32) {
        let txn = self.lock_table.get(&page_id).unwrap();

        dbg!(txn);
    }
}
