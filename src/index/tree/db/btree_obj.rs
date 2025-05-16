#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{cell::RefCell, collections::VecDeque, sync::Arc};

use crate::{
    buffer::{
        buffer_pool_manager::{BufferPoolManager, FileId},
        flusher::{Flusher, Lock},
    },
    index::tree::{
        index_types::{KeyValuePair, NodeKey},
        tree_node::{
            node_type::{NodeType, PagePointer},
            tree_node_inner::NodeInner,
        },
        tree_page::{codec::Codec, page::TreePage},
    },
};

pub enum WriteOperation {
    Delete,
    Insert,
}

pub struct BPTree<'a> {
    flusher: Flusher<'a>,
    root_page_id: RefCell<PagePointer>,

    pub index_id: FileId,

    pub(crate) b: usize,
    codec: Codec,
}

pub struct BTreeBuilder {
    /// Path to the tree file
    // Parameter b is the orber of the tree
    // Each	non-leaf nobe contains b ≤ m ≤ 2b entries
    // minimum	50%	occupancy at all times
    // The root nobe can have 1 ≤ m ≤ 2b entries
    b: usize,
    // The number of keys in a btree is representeb as
    // num_of_keys = 2b - 1

    // The number of pointers to chilb nodes in a btree is represented as
    // num_of_pointers = 2b
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

impl<'a> BPTree<'a> {
    pub(crate) fn new_root(&self, key: NodeKey, left_child: PagePointer, right_child: PagePointer) {
        let new_root_pointer = self.flusher.new_page();
        let new_root = NodeInner::new(
            NodeType::Internal(vec![left_child, right_child], vec![key], new_root_pointer),
            true,
            new_root_pointer,
            None,
        );

        let mut page_id = self.root_page_id.try_borrow_mut().unwrap();
        *page_id = new_root_pointer;

        let page = Codec::encode(&new_root).unwrap();

        self.flusher
            .write_all(new_root_pointer, page.get_data())
            .unwrap();

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
        lock: Lock,
        operation: WriteOperation,
    ) -> anyhow::Result<VecDeque<PagePointer>> {
        let mut contex: VecDeque<PagePointer> = VecDeque::new();

        let root_id = *self.root_page_id.borrow_mut();
        self.flusher.aqquire_sh(root_id)?;
        let mut page = self.flusher.read(root_id)?;

        // TODO Drop Here

        let mut parent = self.codec.decode(&TreePage::new(page))?;

        loop {
            match &parent.node_type {
                NodeType::Internal(children, keys, _) => {
                    let idx = keys.binary_search(&search_key).unwrap_or_else(|x| x);
                    let child_pointer: PagePointer;

                    if idx >= keys.len() || keys[idx] != search_key {
                        child_pointer = children.get(idx).unwrap().clone();
                    } else {
                        child_pointer = children.get(idx + 1).unwrap().clone();
                    }

                    match lock {
                        Lock::EXLOCK => {
                            self.flusher.aqquire_sh(child_pointer)?;
                            page = self.flusher.read(child_pointer)?;

                            // TODO Drop Here

                            let child_node = self.codec.decode(&TreePage::new(page))?;

                            let key_array_length = child_node.get_key_array_length();
                            let safe: bool;

                            // Check if node is safe
                            // Future implementation will rely on less arbituary params
                            if let WriteOperation::Insert = operation {
                                safe = key_array_length + 1 < self.b * 2
                            } else {
                                safe = key_array_length - 1 > self.b - 1
                            }

                            if safe {
                                contex.pop_front();
                                contex.push_front(child_pointer);
                            } else {
                                contex.push_front(child_pointer);
                            }

                            parent = child_node;
                        }
                        Lock::SHLOCK => {
                            self.flusher.aqquire_sh(child_pointer)?;
                            page = self.flusher.read(child_pointer)?;

                            // TODO Drop Here

                            let child_node = self.codec.decode(&TreePage::new(page))?;

                            parent = child_node;
                        }
                    }
                }

                NodeType::Leaf(_, pointer, _) => {
                    contex.push_front(pointer.clone());
                    return Ok(contex);
                }

                NodeType::Unexpected => return Err(anyhow::Error::msg("Unexpected error")),
            }
        }
    }

    pub(crate) fn propogate_upwards(
        &self,
        node: NodeInner,
        // mut context: Context,
    ) -> anyhow::Result<()> {
        unimplemented!()
    }
}
