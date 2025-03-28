use std::path::Path;

use crate::buffer::buffer_pool_manager::{BufferPoolManager, FileId};

use super::{
    errors::Error,
    node::Node,
    node_type::{NextPointer, NodeType},
};

// Allow for interior mutability within the buffer pool
// Flirt with the idea of using a closure to encapsulate all logic needed
// for reads and writes
pub struct BTree {
    bpm: BufferPoolManager,
    index_name: String,
    index_file_id: FileId,
}

pub struct BTreeBuilder {
    /// Path to the tree file

    /// The BTree parameter, an inner node contains no more than 2*b - 1 kets
    /// and no less than b-1 keys and no more than 2*b children and no less than b children
    b: usize,
}

impl BTreeBuilder {
    pub fn new() -> BTreeBuilder {
        BTreeBuilder {
            b: 0,
        }
    }
    pub fn b_parameter(mut self, b: usize) -> BTreeBuilder {
        self.b = b;
        self
    }

    pub fn build(&self, index_name: String, mut bpm: BufferPoolManager) -> Result<BTree, Error> {
        let index_file_id = bpm.allocate_file();

        if self.b == 0 {
            return Err(Error::UnexpectedError);
        }

        let root = Node::new(NodeType::Leaf(vec![], NextPointer(None)), true);
        let page_id = bpm.new_page(index_file_id);

        // {

        //     let write_page = bpm.write_page(index_file_id, page_id);
        //     write_page.get_frame().data.copy_from_slice(root);
        // }


        let index_name = index_name;

        Ok(BTree {
            bpm,
            index_name,
            index_file_id,
        })
    }
}
