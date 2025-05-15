#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{
    cell::RefCell,
    sync::{atomic::AtomicU32, Arc},
};

use dashmap::DashMap;

use crate::{
    buffer::buffer_pool_manager::{BufferPoolManager, FileId},
    index::tree::{
        index_types::{KeyValuePair, NodeKey},
        tree_node::{node_type::PagePointer, tree_node_inner::NodeInner},
        tree_page::codec::Codec,
    },
};

pub struct LockData {
    lock_type: String,
    txn_num: AtomicU32,
    ticker: AtomicU32,
}

pub struct Flusher {
    inner: Arc<BufferPoolManager>,
    lock_table: DashMap<PagePointer, LockData>,
    // impl pub fn fetch_page(guard_type)  -> Gaurd
    // impl pub fn fetch_pages([], guard_type) -> GaurdContext
}

pub struct BPTree {
    flusher: Flusher,
    root_page_id: RefCell<PagePointer>,

    pub index_id: FileId,

    pub(crate) b: usize,
    codec: Codec,
}

pub struct BTreeBuilder {
    /// Path to the tree file
    // Parameter d is the order of the tree
    // Each	non-leaf node contains d ≤ m ≤ 2d entries
    // minimum	50%	occupancy at all times
    // The root node can have 1 ≤ m ≤ 2d entries
    b: usize,
    // The number of keys in a btree is represented as
    // num_of_keys = 2d - 1

    // The number of pointers to child nodes in a btree is represented as
    // num_of_pointers = 2d
}

impl BTreeBuilder {
    pub fn new() -> BTreeBuilder {
        unimplemented!()
    }
    pub fn b_parameter(self, d: usize) -> BTreeBuilder {
        unimplemented!()
    }

    pub fn build(&self, index_name: String, bpm: Arc<BufferPoolManager>) -> anyhow::Result<BPTree> {
        unimplemented!()
    }
}

impl BPTree {
    pub(crate) fn new_root(&self, key: NodeKey, left_child: PagePointer, right_child: PagePointer) {
        unimplemented!()
    }

    pub fn search(&self, search: NodeKey) -> anyhow::Result<KeyValuePair> {
        unimplemented!()
    }

    pub fn find_min(&self) -> anyhow::Result<NodeInner> {
        unimplemented!()
    }

    pub fn find_node(&self) -> anyhow::Result<NodeInner> {
        unimplemented!()
    }

    pub(crate) fn print(&self) {
        unimplemented!()
    }

    pub(crate) fn borrow_if_needed(
        &self,
        child_id: PagePointer,
        child_node: NodeInner,
        // mut context: Context,
    ) -> anyhow::Result<()> {
        unimplemented!()
    }

    pub(crate) fn tree_descent(
        &self,
        search_key: NodeKey,
        // protocol: Protocol,
        // operation: WriteOperation,
    ) -> anyhow::Result<Vec<PagePointer>> {
        unimplemented!()
    }

    pub(crate) fn propogate_upwards(
        &self,
        node: NodeInner,
        // mut context: Context,
    ) -> anyhow::Result<()> {
        unimplemented!()
    }
}
