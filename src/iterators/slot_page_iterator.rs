#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod
use std::sync::Arc;

use crate::storage::{page::page::Page, tuple::Tuple};

use super::iterator::StorageIterator;

pub struct SlottedPageIterator {
    page: Arc<Page>,
    current_tuple: Option<Tuple>,
    index: usize,
}

impl SlottedPageIterator {
    pub fn create(page: Arc<Page>) -> Self {
        unimplemented!()
    }
}

impl StorageIterator for SlottedPageIterator {

    type Item = Tuple;
    fn value(&self) -> Self::Item {
        unimplemented!()
    }

    fn is_valid(&self) -> bool {
        unimplemented!()
    }

    fn advance(&mut self) -> Result<(), ()> {
        unimplemented!()
    }
}

impl Iterator for SlottedPageIterator {
    type Item = Tuple;

    fn next(&mut self) -> Option<Self::Item> {
        unimplemented!()
    }
}
