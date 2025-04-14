use std::{cell::RefCell, collections::VecDeque, vec};

use crate::{
    buffer::buffer_pool_manager::{AccessType, BufferPoolManager, FileId},
    storage::page::{
        b_plus_tree_page::BTreePage,
        btree_page_layout::PAGE_SIZE,
        page_guard::{PageGuard, ReadGuard, WriteGuard},
    },
};

use super::{
    errors::Error,
    node::Node,
    node_type::{Key, KeyValuePair, NextPointer, NodeType, PageId},
};

pub struct WriteContextStack<'a> {
    child: WriteGuard<'a>,
    is_left_sibling: Option<bool>,
    sibling: Option<WriteGuard<'a>>, // Node to borrow from or merge with
}

pub struct ReadContextStack<'a> {
    child: ReadGuard<'a>,
}

/// Crabbing Protocols

#[derive(Clone, Copy)]
pub enum Protocol {
    Exclusive, // Pesimistic crabbing. Exclusive / Write latches upon tree descent
    Shared,    // Optimistic crabbing. Shared / Read latches upon tree descent
}

pub enum WriteOperation {
    Delete,
    Insert,
}

/// Stack of latched Internal and Leaf nodes
pub enum Set<'a> {
    // Store the guards of the pages that you're modifying here.
    WriteStack(VecDeque<WriteContextStack<'a>>),

    // you may want to use this when getting a value, but not necessary
    ReadStack(VecDeque<ReadContextStack<'a>>),
}

// Allow for interior mutability within the buffer pool
// Flirt with the idea of using a closure to encapsulate all logic needed
// for reads and writes if needed. Or if I remember >_<
pub struct BTree {
    bpm: BufferPoolManager,
    index_name: String,
    index_file_id: FileId,
    d: usize,
    root_page_id: RefCell<PageId>,
}

pub struct Context<'a> {
    // Save the root page id here so that it's easier to know if the current page is the root page.
    // This probaly should be a u32 instead of a u64
    root_page_id: PageId,

    // Store the ids of the pages that you're modifying here.
    set: Set<'a>,
}

impl<'a> Context<'a> {
    pub fn new(root_page_id: PageId, protocol: Protocol) -> Self {
        match protocol {
            Protocol::Exclusive => {
                return Context {
                    root_page_id,
                    set: Set::WriteStack(VecDeque::new()),
                }
            }

            Protocol::Shared => {
                return Context {
                    root_page_id,
                    set: Set::ReadStack(VecDeque::new()),
                }
            }
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
            root_page_id: RefCell::new(PageId(page_id.into())),
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

    pub fn get_gaurd(&self, page_id: PageId, acess_type: AccessType) -> Result<PageGuard, Error> {
        match acess_type {
            AccessType::Read => {
                let read_guard = self
                    .bpm
                    .read_page(self.index_file_id, page_id.0.try_into().unwrap());

                return Ok(PageGuard::ReadGuard(read_guard));
            }

            AccessType::Write => {
                let write_guard = self
                    .bpm
                    .write_page(self.index_file_id, page_id.0.try_into().unwrap());

                return Ok(PageGuard::WriteGuard(write_guard));
            }
        }
    }

    pub fn get_node_obj(
        &self,
        protocol: Protocol,
        page_id: PageId,
    ) -> Result<(Node, PageGuard), Error> {
        match protocol {
            Protocol::Exclusive => {
                let page_guard = self
                    .get_gaurd(page_id, AccessType::Write)
                    .unwrap()
                    .into_write_guard()
                    .expect("Write Guard");

                let page_data = page_guard.get_frame().data;
                let page = BTreePage::new(page_data);
                let node = Node::try_from(page)?;
                return Ok((node, PageGuard::WriteGuard(page_guard)));
            }
            Protocol::Shared => {
                let page_guard = self
                    .get_gaurd(page_id, AccessType::Read)
                    .unwrap()
                    .into_read_guard()
                    .expect("Read Guard");

                let page_data = page_guard.get_frame().data;
                let page = BTreePage::new(page_data);
                let node = Node::try_from(page)?;
                return Ok((node, PageGuard::ReadGuard(page_guard)));
            }
        }
    }

    pub fn match_protocol(&self, protocol: Protocol) -> Result<(Node, Context), Error> {
        let mut _btree_page: BTreePage = BTreePage {
            data: [0u8; PAGE_SIZE],
        };

        let mut context = Context::new(*self.root_page_id.borrow(), protocol);
        let root_guard: PageGuard<'_>;

        {
            match protocol {
                Protocol::Exclusive => {
                    root_guard = PageGuard::WriteGuard(self.bpm.write_page(
                        self.index_file_id,
                        self.root_page_id.borrow().0.try_into().unwrap(),
                    ));
                    let write_guard = root_guard.into_write_guard().unwrap();
                    let page_data = write_guard.get_frame().data;
                    _btree_page = BTreePage::new(page_data);

                    if let Set::WriteStack(ref mut context_stack) = context.set {
                        context_stack.push_front(WriteContextStack {
                            child: write_guard,
                            sibling: None,
                            is_left_sibling: None,
                        });
                    } else {
                        return Err(Error::UnexpectedError);
                    }
                }

                Protocol::Shared => {
                    root_guard = PageGuard::ReadGuard(self.bpm.read_page(
                        self.index_file_id,
                        self.root_page_id.borrow().0.try_into().unwrap(),
                    ));

                    let read_guard = root_guard.into_read_guard().unwrap();
                    let page_data = read_guard.get_frame().data;
                    _btree_page = BTreePage::new(page_data);

                    if let Set::ReadStack(ref mut context_stack) = context.set {
                        context_stack.push_front(ReadContextStack { child: read_guard });
                    } else {
                        return Err(Error::UnexpectedError);
                    }
                }
            }
        }
        let root_node = Node::try_from(_btree_page)?;

        Ok((root_node, context))
    }

    pub fn tree_descent(
        &self,
        search_key: Key,
        protocol: Protocol,
        operation: WriteOperation,
    ) -> Result<(Node, Context), Error> {
        let (root_node, context) = self.match_protocol(protocol)?;

        let (leaf_node, context) =
            self.latch_sub_tree(root_node, search_key, context, operation)?;

        Ok((leaf_node, context))
    }

    /// Descends towards the desired leaf node while aquiring read/write latches
    pub fn latch_sub_tree<'a>(
        &'a self,
        parent: Node,
        search: Key,
        mut context: Context<'a>,
        operation: WriteOperation,
    ) -> Result<(Node, Context<'a>), Error> {
        match &parent.node_type {
            NodeType::Internal(children, keys, _) => {
                let idx = keys.binary_search(&search).unwrap_or_else(|x| x);
                let child_pointer = children.get(idx).unwrap().clone();

                match context.set {
                    Set::WriteStack(ref mut stack) => {
                        let (child_node, child_guard) =
                            self.get_node_obj(Protocol::Exclusive, child_pointer)?;
                        let write_gaurd = child_guard.into_write_guard().expect("Page Gaurd");

                        let key_array_length = child_node.get_key_array_length();
                        let safe: bool;

                        // Check if node is safe
                        if let WriteOperation::Insert = operation {
                            safe = key_array_length + 1 == self.d * 2
                        } else {
                            safe = key_array_length - 1 == self.d - 1
                        }

                        if !safe {
                            let sibling_idx = keys
                                .get(idx + 1)
                                .map(|_| idx + 1)
                                .unwrap_or_else(|| idx.saturating_sub(1));

                            let sibling_pointer = children.get(sibling_idx).unwrap().clone();

                            let (_, sibling_guard) =
                                self.get_node_obj(Protocol::Exclusive, sibling_pointer)?;

                            let sibling_gaurd =
                                sibling_guard.into_write_guard().expect("Page Gaurd");

                            let is_left_sibling = idx > sibling_idx;

                            stack.push_front(WriteContextStack {
                                child: write_gaurd,
                                is_left_sibling: Some(is_left_sibling),
                                sibling: Some(sibling_gaurd),
                            });
                        } else {
                            stack.pop_front();
                            stack.push_front(WriteContextStack {
                                child: write_gaurd,
                                is_left_sibling: None,
                                sibling: None,
                            });
                        }

                        let (node, context) =
                            self.latch_sub_tree(child_node, search, context, operation)?;
                        return Ok((node, context));
                    }

                    Set::ReadStack(ref mut stack) => {
                        let (child_node, child_guard) =
                            self.get_node_obj(Protocol::Shared, child_pointer)?;
                        let read_gaurd = child_guard.into_read_guard().expect("Page Gaurd");

                        stack.push_front(ReadContextStack { child: read_gaurd });

                        let (node, context) =
                            self.latch_sub_tree(child_node, search, context, operation)?;

                        return Ok((node, context));
                    }
                }
            }

            NodeType::Leaf(_, _, _) => {
                return Ok((parent, context));
            }

            NodeType::Unexpected => return Err(Error::UnexpectedError),
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
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{remove_dir_all, remove_file},
        path::PathBuf,
    };

    #[test]
    fn teardown() {
        let log_file_path = PathBuf::from("log_file_path.bin");
        let db_path = PathBuf::from("geodeData");
        remove_file(log_file_path).unwrap();
        remove_dir_all(db_path).unwrap();
    }
}

// Next pointer is currently 8 bytes when it really should be 8

/*

//Inserts a key-value pair into the b+ tree object
 pub fn insert_split_protocol(&mut self, search: KeyValuePair) -> Result<(), Error> {
     let mut context = Context::new(*self.root_page_id.borrow());
     let mut node: Node;

     {
         let page_data = self.bpm.write_page(
             self.index_file_id,
             self.root_page_id.borrow().0.try_into().unwrap(),
         );

         let btree_page = BTreePage::new(page_data.get_frame().data);
         node = Node::try_from(btree_page)?;

         context.write_set.push_front(page_data);
     }

     Ok(())
 }


 choose_sub_tree_ex (recursively) finds a node rooted at a given non-full node while obtaining exclusive latches.
 to insert a given key-value pair. Here we assume the node is
 already a copy of an existing node in a copy-on-write root to node traversal.

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
                     let mut root = self.root_page_id.borrow_mut();
                     *root = PageId(new_root.into());

                     // retro actively split parent nodes
                     // nodes are arranged in the manner

                     //   node -> node' -> node'' -> node ''' -> ... ->    node 'n
                     // | Leaf  |                                       |  root    |

                     while let Some(node) = context.write_set.pop_front() {}
                 } else {
                     // Node has space, just insert
                     // TODO: Insert logic here

                     pairs.insert(idx, search);
                     let node_pointer: PageId = node.get_pointer()?;
                     // No longer doing copy on write. Therefore the node should have data on its page_id
                     self.pg_write_helper(&node_pointer, &node);

                     // Clear write set after operation is complete
                     context.write_set.clear();
                 }
             }

             return Ok(());
         }
         _ => return Err(Error::UnexpectedError),
     }
 }

 */
