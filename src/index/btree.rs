use std::{collections::VecDeque, path::Path, sync::RwLock};

use io_uring::opcode::Write;

use crate::{
    buffer::buffer_pool_manager::{BufferPoolManager, FileId},
    storage::page::{
        b_plus_tree_page::BTreePage,
        page_guard::{ReadGuard, WriteGuard},
    },
};

use super::{
    errors::Error,
    node::Node,
    node_type::{Key, KeyValuePair, NextPointer, NodeType, PageId},
};

// Allow for interior mutability within the buffer pool
// Flirt with the idea of using a closure to encapsulate all logic needed
// for reads and writes if needed. Or if I remember >_<
pub struct BTree {
    bpm: RwLock<BufferPoolManager>,
    index_name: String,
    index_file_id: FileId,
    b: usize,
    header_page_id: u32,
}

pub struct Context<'a> {
    // When you insert into / remove from the B+ tree, store the write guard of header page here.
    // Remember to drop the header page guard and set it to nullopt when you want to unlock all.
    header_page: WriteGuard<'a>,

    // Save the root page id here so that it's easier to know if the current page is the root page.
    // This probaly should be a u32 instead of a u64
    root_page_id: PageId,

    // Store the write guards of the pages that you're modifying here.
    write_set: VecDeque<WriteGuard<'a>>,

    // you may want to use this when getting a value, but not necessary
    read_set: VecDeque<ReadGuard<'a>>,
}

pub struct BTreeBuilder {
    /// Path to the tree file

    /// The BTree parameter, an inner node contains no more than 2*b - 1 keys
    /// and no less than b-1 keys and no more than 2*b children and no less than b children
    b: usize,
}

impl BTreeBuilder {
    pub fn new() -> BTreeBuilder {
        BTreeBuilder { b: 0 }
    }
    pub fn b_parameter(mut self, b: usize) -> BTreeBuilder {
        self.b = b;
        self
    }

    pub fn build(&self, index_name: String, mut bpm: BufferPoolManager) -> Result<BTree, Error> {
        let index_file_id = bpm.allocate_file();
        let b: usize = self.b;

        if b == 0 {
            return Err(Error::UnexpectedError);
        }

        let page_id = bpm.new_page(index_file_id);

        {
            let root = Node::new(NodeType::Leaf(vec![], NextPointer(None)), true);
            let root_page_data = BTreePage::try_from(&root)?;

            let write_page_guard = bpm.write_page(index_file_id, page_id);
            write_page_guard
                .get_frame()
                .data
                .copy_from_slice(&root_page_data.get_data());

            drop(write_page_guard);
        }

        let index_name = index_name;

        Ok(BTree {
            bpm: RwLock::new(bpm),
            index_name,
            index_file_id,
            b,
            header_page_id: page_id,
        })
    }
}

impl Default for BTreeBuilder {
    // A default BTreeBuilder provides a builder with:
    // - b parameter set to 12
    fn default() -> Self {
        BTreeBuilder::new().b_parameter(12)
    }
}

impl BTree {
    fn is_node_full(&self, node: &Node) -> Result<bool, Error> {
        match &node.node_type {
            NodeType::Leaf(pairs, _) => Ok(pairs.len() == (2 * self.b)),
            NodeType::Internal(_, keys) => Ok(keys.len() == (2 * self.b - 1)),
            NodeType::Unexpected => Err(Error::UnexpectedError),
        }
    }

    fn is_node_underflow(&self, node: &Node) -> Result<bool, Error> {
        match &node.node_type {
            // A root cannot really be "underflowing" as it can contain less than b-1 keys / pointers.
            NodeType::Leaf(pairs, _) => Ok(pairs.len() < (self.b - 1) && !node.is_root),
            NodeType::Internal(_, keys) => Ok(keys.len() < (self.b - 1) && !node.is_root),
            NodeType::Unexpected => Err(Error::UnexpectedError),
        }
    }

    /// Returns true if the tree has a 0 keys and values.
    fn is_empty() -> bool {
        // get header
        // serialize header // May or may not be needed
        // if header page is not found, the tree is false
        // if header is internal return true
        // if header page is a leaf with an empty key-value vec
        // return true
        // else falsez

        false
    }

    /// Inserts a key-value pair into the b+ tree object
    fn insert(&self, kv: KeyValuePair) -> Result<(), Error> {
        // Starts off at the root node

        // Priliminarily identify whether or not the current root node
        // is to capacity

        // If so split the old root into to separate nodes (old root, sibling)
        // and initialize the new root with a vector containing
        // the pointers to the old root and the sibling

        // If its not to capacity, continue to insert recursively

        Ok(())
    }

    // recursion helper for fn insert(&self, kv : KeyValuePair) -> Result<(), Error>
    // choose_sub_tree (&mut self,
    //    node: &mut Node,
    //    node_offset: Offset,
    //    kv: KeyValuePair,
    // ) -> Result<(), Error>  

    /*
        match on node type
            Leaf -> ( parent node's data ) {
                insert the kv pair into the vec
                fail on matching keys ( two keys of the same values are not allowed)
                write page to disk
            
            }

            Internal ( parent node's data ) -> {
                get the childpointer and its corresponding page

                If the nodes needs to split, proceed to do so
                    Write the updated pages to disk

                    Write the updated parent's data to disk
                    Update the parent node's pointer
                    continue recursively based on the whether to key 
                    within the kv pair to be inserted is greater than
                    or less than the median

                    continue recursively until leaf page

                Else
                    continue recursively until leaf page
            }

            Unexpected -> {
                Throw unexpected error
            }
    
    
    
    
    
    
    
     */

    /// delete deletes a given key from the tree.
    pub fn delete(&self, key: Key) -> Result<(), Error> {
        Ok(())
    }

    /// merges two sibling nodes
    // it assumes the following:
    // 1. the two nodes are of the same type.
    // 2. the two nodes do not accumulate to an overflow,
    // i.e. |first.keys| + |second.keys| <= [2*(b-1) for keys or 2*b for offsets].
    fn merge(&self, first: Node, second: Node) -> Result<Node, Error> {
        Ok(())
    }

    /// Return the value associated with a given key
    fn get(&self, key: Key) -> Result<KeyValuePair, Error> {
        Ok(())
    }

    /// Return a key-value pair
    fn get_entry(&self, key: Key) -> Result<KeyValuePair, Error> {
        Ok(())
    }
}
