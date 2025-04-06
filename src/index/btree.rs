use std::{collections::VecDeque, vec};

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

/// Latch Crabing Protocols
pub enum Protocol {
    InsertSplit,    // Pesimistic crabbing. Exclusive / Write latches upon tree descent
    InsertNonSplit, // Optimistic crabbing. Shared / Read latches upon tree descent
}

// Allow for interior mutability within the buffer pool
// Flirt with the idea of using a closure to encapsulate all logic needed
// for reads and writes if needed. Or if I remember >_<
pub struct BTree {
    bpm: BufferPoolManager,

    index_name: String,
    index_file_id: FileId,
    d: usize,
    root_page_id: PageId,
}

pub struct Context<'a> {
    // Save the root page id here so that it's easier to know if the current page is the root page.
    // This probaly should be a u32 instead of a u64
    root_page_id: PageId,

    // Store the ids of the pages that you're modifying here.
    write_set: VecDeque<WriteGuard<'a>>,

    // you may want to use this when getting a value, but not necessary
    read_set: VecDeque<ReadGuard<'a>>,
}

impl<'a> Context<'a> {
    pub fn new(root_page_id: PageId) -> Self {
        Context {
            root_page_id,
            write_set: VecDeque::new(),
            read_set: VecDeque::new(),
        }
    }
}

pub struct BTreeBuilder {
    /// Path to the tree file
    // Parameter d is the order of the tree
    // Each	non-leaf node contains d ≤ m ≤ 2d entries
    // minimum	50%	occupancy at all times
    // The root node can have 1 ≤ m ≤ 2d entries
    d: usize,
    // The number of keys in a btree is represented as
    // num_of_keys = 2d - 1

    // The number of pointers to child nodes in a btree is represented as
    // num_of_pointers = 2d
}

impl BTreeBuilder {
    pub fn new() -> BTreeBuilder {
        BTreeBuilder { d: 0 }
    }
    pub fn b_parameter(mut self, d: usize) -> BTreeBuilder {
        self.d = d;
        self
    }

    pub fn build(&self, index_name: String, mut bpm: BufferPoolManager) -> Result<BTree, Error> {
        let index_file_id = bpm.allocate_file();
        let d: usize = self.d;

        if d == 0 {
            return Err(Error::UnexpectedError);
        }

        let page_id = bpm.new_page(index_file_id);

        {
            let root = Node::new(
                NodeType::Leaf(
                    vec![],
                    NextPointer(None),
                    PageId(page_id.try_into().unwrap()),
                ),
                PageId(page_id.try_into().unwrap()),
                true,
            );
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
            bpm,
            index_name,
            index_file_id,
            d,
            root_page_id: PageId(page_id.into()),
        })
    }
}

impl<'a> Context<'a> {
    // Page id should be a u32
    pub fn build(root_page_id: PageId) -> Result<Self, crate::index::errors::Error> {
        Ok(Self {
            root_page_id,
            write_set: VecDeque::new(),
            read_set: VecDeque::new(),
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
    pub fn is_node_full(&self, node: &Node) -> Result<bool, Error> {
        match &node.node_type {
            NodeType::Leaf(pairs, _, _) => Ok(pairs.len() == (2 * self.d)),
            NodeType::Internal(_, keys, _) => Ok(keys.len() == (2 * self.d - 1)),
            NodeType::Unexpected => {
                println!("Unexpected error in is_node_full");
                Err(Error::UnexpectedError)
            }
        }
    }

    pub fn is_node_underflow(&self, node: &Node) -> Result<bool, Error> {
        match &node.node_type {
            // A root cannot really be "underflowing" as it can contain less than b-1 keys / pointers.
            NodeType::Leaf(pairs, _, _) => Ok(pairs.len() < (self.d - 1) && !node.is_root),
            NodeType::Internal(_, keys, _) => Ok(keys.len() < (self.d - 1) && !node.is_root),
            NodeType::Unexpected => {
                println!("Unexpected error in is_node_underflow");
                Err(Error::UnexpectedError)
            }
        }
    }

    // Have this return successful and unsuccessful return values
    // This does not return a write guard but writes a specific page to disk

    pub fn pg_write_helper(&self, page_id: &PageId, node: &Node) {
        if let Ok(new_root_data) = BTreePage::try_from(node) {
            let write_guard = self
                .bpm
                .write_page(self.index_file_id, page_id.0.try_into().unwrap());

            write_guard
                .get_frame()
                .data
                .copy_from_slice(&new_root_data.get_data());
        }
    }

    pub fn pg_read_helper(&self, page_id: PageId) -> Result<ReadGuard, Error> {
        let read_guard = self
            .bpm
            .read_page(self.index_file_id, page_id.0.try_into().unwrap());

        Ok(read_guard)
    }

    /// Inserts a key-value pair into the b+ tree object
    pub fn insert_split_protocol(&mut self, search: KeyValuePair) -> Result<(), Error> {
        let mut context = Context::new(self.root_page_id);
        let mut node: Node;

        {
            let page_data = self
                .bpm
                .write_page(self.index_file_id, self.root_page_id.0.try_into().unwrap());

            let btree_page = BTreePage::new(page_data.get_frame().data);
            node = Node::try_from(btree_page)?;

            context.write_set.push_front(page_data);
        }

        self.choose_sub_tree_ex(&mut node, search, context)?;

        Ok(())
    }

    // Next pointer is currently 8 bytes when it really should be 8

    /// choose_sub_tree_ex (recursively) finds a node rooted at a given non-full node while obtaining exclusive latches.
    /// to insert a given key-value pair. Here we assume the node is
    /// already a copy of an existing node in a copy-on-write root to node traversal.

    pub fn choose_sub_tree_ex<'a>(
        &'a self,
        node: &mut Node,
        // node_pointer: PageId,
        search: KeyValuePair,
        mut context: Context<'a>,
    ) -> Result<(), Error> {
        match node.node_type {
            NodeType::Internal(ref child_pointers, ref keys, current_page) => {
                // Binary search is great because event if a specifc key isn't in the keys array
                // it returns the idx of of where the key should be inserted to maintain sort order

                let idx = keys.binary_search(&Key(search.key)).unwrap_or_else(|x| x);

                let child_pointer = child_pointers.get(idx).ok_or(Error::UnexpectedError)?;

                // aqquire an exclusive or shared latch depending on the protocol
                let child_page_guard = {
                    self.bpm
                        .write_page(self.index_file_id, child_pointer.0.try_into().unwrap())
                };

                let child_btree_page = BTreePage::new(child_page_guard.get_frame().data);
                let mut child_node = Node::try_from(child_btree_page)?;
                // Pesismistic lock control.
                // We first check if nodes are 'safe'
                // That is will this split on a newly inserted key? i.e Is it full

                if !self.is_node_full(&child_node)? {
                    // If it is not full we drop the parent
                    // once popped, the page guard should fall out of scope
                    let parent = context.write_set.pop_back().unwrap();
                    drop(parent);
                }

                // We still aqquire the write guard child node
                context.write_set.push_front(child_page_guard);

                self.choose_sub_tree_ex(
                    &mut child_node,
                    // child_pointer.clone(),
                    search,
                    context,
                    // Protocol::InsertSplit, // Continue with Pesimistic latching until leaf node
                )?;

                Ok(())
            }
            // The root node might be the leaf
            NodeType::Leaf(ref mut pairs, _, curren_page) => {
                let key = search.key;
                let idx = pairs
                    .binary_search_by_key(&key, |pair| pair.key.clone())
                    .unwrap_or_else(|x| x);

                // If the key already exists, reject the value
                if idx < pairs.len() && pairs[idx].key == key {
                    return Err(Error::DuplicateKey);
                } else {
                    // New key needs to be inserted
                    if pairs.len() == (2 * self.d) {
                        // Node is full, need to split

                        let (median, sibling) = node.split(self.d)?;

                        let sibling_pointer: PageId = sibling.get_pointer()?;
                        node.set_next_pointer(sibling_pointer.0.to_le_bytes())?;

                        let node_pointer: PageId = node.get_pointer()?;

                        let new_root = self.bpm.new_page(self.index_file_id);
                        let new_root_node = Node::new(
                            NodeType::Internal(
                                vec![node_pointer, sibling_pointer],
                                vec![median],
                                PageId(new_root.try_into().unwrap()),
                            ),
                            PageId(new_root.try_into().unwrap()),
                            true,
                        );
                        // Write updated node back to disk
                        self.pg_write_helper(&PageId(new_root.into()), &new_root_node);

                        // Write node data to disk
                        self.pg_write_helper(&node_pointer, &node);

                        // Write node and sibling data to disk
                        self.pg_write_helper(&sibling_pointer, &sibling);

                        // self.root_page_id = PageId(new_root.into());
                    } else {
                        // Node has space, just insert
                        // TODO: Insert logic here
                        pairs.insert(idx, search);
                        let node_pointer: PageId = node.get_pointer()?;
                        // No longer doing copy on write. Therefore the node should have data on its page_id
                        self.pg_write_helper(&node_pointer, &node);
                    }
                }

                // Clear write set after operation is complete
                context.write_set.clear();

                return Ok(());
            }
            _ => return Err(Error::UnexpectedError),
        }
    }

    // /// Finds a specific key in the BTree.
    // pub fn find(&self, key: Key) -> Result<KeyValuePair, Error> {}

    // /// Recursively searches a sub tree rooted at a node for a key.
    // pub fn find_node(&self, node: Node, search: Key) -> Result<KeyValuePair, Error> {}

    /// Returns the value associated with a given key
    // pub fn get(&self, key: Key) -> Result<RowID, Error> {
    //     let entry = self.find(key)?;
    //     Ok(entry.value)
    // }

    // /// Returns a key-value pair
    // fn get_entry(&self, key: Key) -> Result<KeyValuePair, Error> {
    //     let entry = self.find(key)?;
    //     Ok(entry)
    // }

    /// delete deletes a given key from the tree.
    pub fn delete(&self, key: Key) -> Result<(), Error> {
        // Get, serialize then copy the contents of the root page
        // Proceed with the recursive delete_key_from_subtree function

        Ok(())
    }

    // delete key from subtree recursively traverses a tree rooted at a node in certain offset
    // until it finds the given key and delete the key-value pair. Here we assume the node is
    // already a copy of an existing node in a copy-on-write root to node traversal.
    // fn delete_key_from_subtree(
    //    &mut self,
    //    key: Key,
    //    node: &mut Node,
    //    node_offset: &Offset,
    // ) -> Result<(), Error>

    /*
    match on node_type -> {
        Leaf -> {
            Similar to insert logic
            Except we remove the kv pair and
            check if any underflows were created

            If so we merge with a siblings recursively up the tree
            }

            Internal -> {

            Retrieve the child page
            Create a new copy of the child page and write it out to disk

            Assign the new pointer to the newly created child page in the parent
            write that update to disk

            We're doing a lot of disk writes this copy on write structure might not be optimal

            Continue recursively to child node

            }


        }



        */

    // borrow_if_needed checks the node for underflow (following a removal of a key),
    // if it underflows it is merged with a sibling node, and than called recoursively
    // up the tree. Since the downward root-to-leaf traversal was done using the copy-on-write
    // technique we are ensured that any merges will only be reflected in the copied parent in the path.
    // fn borrow_if_needed(&mut self, node: Node, key: &Key) -> Result<(), Error> { }

    /*
    If underflow

    fetch sibling node

    merge current and sibling node


    if the parent is the root, and there is a single child - the merged node -
    we can safely replace the root with the child.

    remove the keys that separated the two nodes from each other

    write the new node in place.

    write the updated parent back to disk and continue up the tree
    by passing in the parent pointer.


    */

    // merges two *sibling* nodes, it assumes the following:
    // 1. the two nodes are of the same type.
    // 2. the two nodes do not accumulate to an overflow,
    // i.e. |first.keys| + |second.keys| <= [2*(b-1) for keys or 2*b for offsets].
    // fn merge(&self, first: Node, second: Node) -> Result<Node, Error>

    /*
    match first.node_type
    Leaf -> {
               Make sure other node is a leaf

               merge the vector of KeyValuePairs together
               construct a new leaf node make sure to give the newly constructed
               node the next pointer of the right sibling

               }

               Internal -> {
                Make sure other node is an internal leaf

               Merge keys and offsets
               }




               */
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

    // print_sub_tree is a helper function for recursively printing the nodes rooted at a node given by its offset.
    // fn print_sub_tree(&mut self, prefix: String, page_id: PageId) -> Result<(), Error> {
    //     println!("Node at pointer: {:?}", page_id);

    //     let curr_prefix = format!("{} | -> ", prefix);
    //     let page_data: BTreePage;

    //     {
    //         let page = self.pg_read_helper(page_id)?;
    //         page_data = BTreePage::new(page.get_frame().data);
    //     }
    //     let node = Node::try_from(page_data)?;

    //     match node.node_type {
    //         NodeType::Internal(children, keys) => {
    //             println!("{}Keys: {:?}", curr_prefix, keys);
    //             println!("{}Children: {:?}", curr_prefix, children);
    //             let child_prefix = format!("{}   |  ", prefix);
    //             for child_pointer in children {
    //                 self.print_sub_tree(child_prefix.clone(), child_pointer)?;
    //             }
    //             Ok(())
    //         }
    //         NodeType::Leaf(pairs, _) => {
    //             println!("{}Key value pairs: {:?}", curr_prefix, pairs);
    //             Ok(())
    //         }
    //         NodeType::Unexpected => Err(Error::UnexpectedError),
    //     }
    // }

    // print is a helper for recursively printing the tree.
    // pub fn print(&mut self) -> Result<(), Error> {
    //     println!();
    //     let root_pointer = self.root_page_id;
    //     self.print_sub_tree("".to_string(), root_pointer)
    // }
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{remove_dir_all, remove_file},
        path::PathBuf,
    };

    use crate::{
        buffer::buffer_pool_manager::BufferPoolManager,
        index::{
            errors::Error,
            node::Node,
            node_type::{Key, KeyValuePair, PageId, RowID},
        },
        storage::{
            disk::manager::Manager,
            page::{b_plus_tree_page::BTreePage, btree_page_layout::PAGE_SIZE},
        },
    };

    use super::BTreeBuilder;

    #[test]
    fn teardown() {
        let log_file_path = PathBuf::from("log_file_path.bin");
        let db_path = PathBuf::from("geodeData");
        remove_file(log_file_path).unwrap();
        remove_dir_all(db_path).unwrap();
    }
}
