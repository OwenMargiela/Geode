#![allow(unused_variables)]
#![allow(dead_code)]

use std::sync::Arc;

use crate::{
    buffer::flusher::Flusher,
    index::tree::{
        index_types::{ KeyValuePair, NodeKey },
        tree_node::tree_node_inner::NodeInner,
        tree_page::{ codec::Codec, page::TreePage },
    },
};

use super::iterator::StorageIterator;

pub struct LeafIterator {
    flusher: Arc<Flusher>,
    page: Arc<NodeInner>,
    codec: Codec,

    current_key: Option<KeyValuePair>,
    file_id: u64,
    index: usize,
}

impl LeafIterator {
    pub fn get_page(&self, next_page: u32) -> NodeInner {
        let page = self.flusher.read_drop(next_page);
        let tree_page = TreePage::new(page);
        let node = self.codec.decode(&tree_page).unwrap();

        node
    }

    pub fn create_and_seek_to_key(
        flusher: Arc<Flusher>,
        page: Arc<NodeInner>,
        codec: Codec,

        search_key: NodeKey,
        file_id: u64
    ) -> Self {
        let current_key = match page.get_key_value(&search_key) {
            Ok(key) => Some(key),
            Err(e) => None,
        };

        let index = match page.get_key_value_index(&search_key) {
            Ok(key) => key,
            Err(e) => 0,
        };

        Self {
            flusher,
            page,
            codec,
            current_key,
            file_id,
            index,
        }
    }

    pub fn create_and_seek_to_first(
        flusher: Arc<Flusher>,
        page: Arc<NodeInner>,
        codec: Codec,
        file_id: u64
    ) -> Self {
        let current_key = match page.get_key_value_at(0) {
            Ok(key) => Some(key),
            Err(e) => None,
        };

        Self {
            flusher,
            page,
            codec,
            file_id,
            current_key,
            index: 0,
        }
    }
}

impl StorageIterator for LeafIterator {
    type Item = KeyValuePair;

    fn value(&self) -> Self::Item {
        let key = self.current_key.as_ref().unwrap().clone();
        key
    }

    fn is_valid(&self) -> bool {
        self.current_key.is_some()
    }

    fn advance(&mut self) -> anyhow::Result<()> {
        let cur = self.index;

        let current_key = match self.page.get_key_value_at(cur + 1) {
            Ok(key) => {
                self.index = cur + 1;
                Some(key)
            }
            Err(e) => None,
        };

        if !current_key.is_none() {
            self.current_key = current_key;
            return Ok(());
        }

        let next_page = self.page.next_pointer.unwrap();

        if next_page > 0 {
            let node = self.get_page(next_page);

            *self = LeafIterator::create_and_seek_to_first(
                self.flusher.clone(),
                Arc::new(node),
                self.codec.clone(),
                self.file_id
            );
        }

        return Err(anyhow::Error::msg("Cannot Advance"));
    }
}

impl Iterator for LeafIterator {
    type Item = KeyValuePair;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.is_valid() {
            return None;
        }

        let current = self.current_key.clone();

        match self.advance() {
            Ok(()) => {}
            Err(e) => {
                self.current_key = None;
            }
        }

        current
    }
}
