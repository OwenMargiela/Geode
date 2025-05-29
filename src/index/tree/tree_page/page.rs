#![allow(unused_variables)] 
#![allow(dead_code)] 

use super::tree_page_layout::{LeafNodeHeader, PAGE_SIZE};

pub struct TreePage {
    data: [u8; PAGE_SIZE],
}

impl TreePage {
    pub fn new(page: [u8; PAGE_SIZE]) -> TreePage {
        Self { data: page }
    }

    /// get_sized_from_offset Fetches a slice of bytes from certain offset and of certain size.
    pub fn get_sized_value_from_offset(&self, offset: usize, size: usize) -> &[u8] {
        &self.data[offset..offset + size]
    }

    pub fn get_next_leaf_pointer(&self) -> &[u8] {
        let start = LeafNodeHeader::NextLeafPointer.offset();
        let end = start + LeafNodeHeader::NextLeafPointer.size();

        let next = &self.data[start..end];
        next
    }

    /// get_data returns the underlying array.
    pub fn get_data(&self) -> [u8; PAGE_SIZE] {
        self.data
    }
}
