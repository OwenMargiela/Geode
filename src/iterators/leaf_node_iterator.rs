#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod
use std::sync::Arc;

use crate::{
    buffer::buffer_pool_manager::BufferPoolManager,
    index::{
        node::Node,
        node_type::{Key, KeyValuePair},
    },
    storage::page::b_plus_tree_page::BTreePage,
};

use super::iterator::StorageIterator;

pub struct LeafNodeIterator {
    bpm_instance: Arc<BufferPoolManager>,
    file_id: u64,

    page: Arc<Node>,
    current_key: Option<KeyValuePair>,
    index: usize,
}

impl LeafNodeIterator {
    pub fn get_page(&self, next_page: u32) -> Node {
        let page_guard = self.bpm_instance.read_page(self.file_id, next_page);
        let page = BTreePage::new(page_guard.get_frame().data);
        let node = Node::try_from(page).unwrap();

        drop(page_guard);

        node
    }

    pub fn create_and_seek_to_key(
        bpm_instance: Arc<BufferPoolManager>,
        file_id: u64,
        page: Arc<Node>,
        search_key: Key,
    ) -> Self {
        let current_key = match page.find_key_value(&search_key) {
            Ok(key) => Some(key),
            Err(e) => None,
        };

        let index = page.find_key_index_in_leaf(&search_key);

        Self {
            file_id,
            bpm_instance,
            page,
            current_key,
            index,
        }
    }

    pub fn create_and_seek_to_first(
        bpm_instance: Arc<BufferPoolManager>,
        file_id: u64,
        page: Arc<Node>,
    ) -> Self {
        let current_key = match page.get_key_value_at_index(0) {
            Ok(key) => Some(key),
            Err(e) => None,
        };

        Self {
            file_id,
            bpm_instance,
            page,
            current_key,
            index: 0,
        }
    }
    
}

impl StorageIterator for LeafNodeIterator {
    type Item = KeyValuePair;
    fn value(&self) -> Self::Item {
        assert!(self.is_valid());
        let key = self.current_key.unwrap().clone();

        key
    }

    fn is_valid(&self) -> bool {
        self.current_key.is_some()
    }

    fn advance(&mut self) -> Result<(), ()> {
        let curr = self.index;

        let current_key = match self.page.get_key_value_at_index(curr + 1) {
            Ok(key) => {
                self.index = curr + 1;
                Some(key)
            }
            Err(e) => None,
        };

        if !current_key.is_none() {
            self.current_key = current_key;
            return Ok(());
        }

        let next_page = self.page.get_next_pointer().unwrap();
        let id = u64::from_le_bytes(next_page) as u32;

        if id > 0 {
            let node = self.get_page(id);

            *self = LeafNodeIterator::create_and_seek_to_first(
                self.bpm_instance.clone(),
                self.file_id,
                Arc::new(node),
            );

            return Ok(());
        }

        Err(())
    }
}

impl Iterator for LeafNodeIterator {
    type Item = KeyValuePair;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.is_valid() {
            return None;
        }

        let current = self.current_key.clone();

        match self.advance() {
            Ok(()) => {}
            Err(()) => {
                self.current_key = None;
            }
        }

        current
    }
}
