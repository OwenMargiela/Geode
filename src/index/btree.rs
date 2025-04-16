use std::{cell::RefCell, collections::VecDeque, fmt, vec};

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

pub struct MergeContextStack {
    child: PageId,
    is_left_sibling: Option<bool>,
    sibling: Option<PageId>, // Node to borrow from or merge with
}

pub struct MergeContextGuardStack<'a> {
    child: WriteGuard<'a>,
    is_left_sibling: Option<bool>,
    sibling: Option<WriteGuard<'a>>, // Node to borrow from or merge with
}

pub struct ReadContextStack {
    child: PageId,
}

pub struct ReadContextGuardStack<'a> {
    child: WriteGuard<'a>,
}
pub struct WriteContextStack {
    child: PageId,
}
pub struct WriteContextGuardStack<'a> {
    child: WriteGuard<'a>,
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
/// Stack of latched Internal and Leaf nodes
pub enum Set {
    // Store the guards of the pages that you're modifying here.
    WriteStack(VecDeque<WriteContextStack>),

    // you may want to use this when getting a value, but not necessary
    ReadStack(VecDeque<ReadContextStack>),

    MergeStack(VecDeque<MergeContextStack>),
}

pub enum GuardSet<'a> {
    // Store the guards of the pages that you're modifying here.
    WriteStack(VecDeque<WriteContextGuardStack<'a>>),

    // you may want to use this when getting a value, but not necessary
    ReadStack(VecDeque<ReadContextGuardStack<'a>>),

    MergeStack(VecDeque<MergeContextGuardStack<'a>>),
}

pub struct GuardContext<'a> {
    // Save the root page id here so that it's easier to know if the current page is the root page.
    // This probaly should be a u32 instead of a u64
    root_page_id: PageId,

    // Store the ids of the pages that you're modifying here.
    set: GuardSet<'a>,
}

pub struct Context {
    // Save the root page id here so that it's easier to know if the current page is the root page.
    // This probaly should be a u32 instead of a u64
    root_page_id: PageId,

    // Store the ids of the pages that you're modifying here.
    set: Set,
}

impl fmt::Debug for Context {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Context {{ root_page_id: {:?}, ", self.root_page_id)?;

        match &self.set {
            Set::WriteStack(stack) => {
                write!(f, "set: WriteStack ({} items)", stack.len())?;
                write!(f, " [")?;
                for item in stack {
                    write!(f, " {:?} ", item.child);
                }
                write!(f, "] ")?;
            }
            Set::ReadStack(stack) => {
                write!(f, "set: ReadStack ({} items)", stack.len())?;
            }
            Set::MergeStack(stack) => {
                write!(f, "set: MergeStack ({} items)", stack.len())?;
            }
        }

        write!(f, " }}")
    }
}

impl<'a> Drop for GuardContext<'a> {
    fn drop(&mut self) {
        match &mut self.set {
            GuardSet::WriteStack(write_stack) => {
                while let Some(_ctx) = write_stack.pop_front() {
                    drop(_ctx)
                }
            }
            GuardSet::ReadStack(read_stack) => {
                while let Some(_ctx) = read_stack.pop_front() {
                    drop(_ctx.child);
                    // ReadGuard will be dropped here
                }
            }
            GuardSet::MergeStack(merge_stack) => {
                while let Some(merge_ctx) = merge_stack.pop_back() {
                    // Drop child guard
                    drop(merge_ctx.child);
                    // Drop sibling guard if exists
                    if let Some(sibling) = merge_ctx.sibling {
                        drop(sibling);
                    }
                }
            }
        }
    }
}

impl Context {
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

impl<'a> GuardContext<'a> {
    pub fn new(root_page_id: PageId, protocol: Protocol) -> Self {
        match protocol {
            Protocol::Exclusive => {
                return GuardContext {
                    root_page_id,
                    set: GuardSet::WriteStack(VecDeque::new()),
                }
            }

            Protocol::Shared => {
                return GuardContext {
                    root_page_id,
                    set: GuardSet::ReadStack(VecDeque::new()),
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
            NodeType::Internal(_, keys, _) => Ok(keys.len() == (2 * self.d)),
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

    // pub fn split_and_flush

    pub fn flush_data(&self, node: &Node, page: u32) {
        println!("\nObtaining write guard in flush data");
        let guard = self.bpm.write_page(self.index_file_id, page);
        let node_page = BTreePage::try_from(node).unwrap();

        guard
            .get_frame()
            .data
            .copy_from_slice(&node_page.get_data());

        drop(guard);
    }

    pub fn new_root(&self, key: Key, left_child: PageId, right_child: PageId) {
        let new_root_pointer = PageId(self.bpm.new_page(self.index_file_id).try_into().unwrap());

        let new_root = Node::new(
            NodeType::Internal(vec![left_child, right_child], vec![key], new_root_pointer),
            new_root_pointer,
            true,
        );
        let mut page = self.root_page_id.try_borrow_mut().unwrap();
        *page = new_root_pointer;

        println!("\n\n\nCheckpoint new rooot {:?}\n\n\n", new_root_pointer);

        self.flush_data(&new_root, new_root_pointer.0.try_into().unwrap());
    }

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

    // obtains a latch on the parent
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

                    drop(write_guard);

                    if let Set::WriteStack(ref mut context_stack) = context.set {
                        context_stack.push_front(WriteContextStack {
                            child: self.root_page_id.borrow().clone(),
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

                    drop(read_guard);

                    if let Set::ReadStack(ref mut context_stack) = context.set {
                        context_stack.push_front(ReadContextStack {
                            child: self.root_page_id.borrow().clone(),
                        });
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
            self.latch_sub_tree(root_node, search_key, context, Some(operation))?;

        Ok((leaf_node, context))
    }

    /// Descends towards the desired leaf node while aquiring read/write latches
    /// Returns the gaurds of all the nodes needed for the operation
    pub fn latch_sub_tree<'a>(
        &'a self,
        parent: Node,
        search: Key,
        mut context: Context,
        operation: Option<WriteOperation>,
    ) -> Result<(Node, Context), Error> {
        match &parent.node_type {
            NodeType::Internal(children, keys, _) => {
                let idx = keys.binary_search(&search).unwrap_or_else(|x| x);
                let child_pointer = children.get(idx).unwrap().clone();

                match context.set {
                    Set::WriteStack(ref mut stack) => {
                        let (child_node, _) =
                            self.get_node_obj(Protocol::Exclusive, child_pointer)?;

                        // let write_gaurd = child_guard.into_write_guard().expect("Page Gaurd");

                        let key_array_length = child_node.get_key_array_length();
                        let safe: bool;

                        // Check if node is safe
                        if let Some(WriteOperation::Insert) = operation {
                            safe = key_array_length + 1 < self.d * 2
                        } else {
                            safe = key_array_length - 1 > self.d - 1
                        }

                        if safe {
                            stack.pop_front();
                            stack.push_front(WriteContextStack {
                                child: child_pointer,
                            });
                        } else {
                            println!("Not safe");
                            stack.push_front(WriteContextStack {
                                child: child_pointer,
                            });
                        }

                        let (node, context) =
                            self.latch_sub_tree(child_node, search, context, operation)?;
                        return Ok((node, context));
                    }

                    Set::MergeStack(ref mut stack) => {
                        let (child_node, _) =
                            self.get_node_obj(Protocol::Exclusive, child_pointer)?;
                        // let write_gaurd = child_guard.into_write_guard().expect("Page Gaurd");

                        let key_array_length = child_node.get_key_array_length();
                        let safe: bool;

                        // Check if node is safe
                        if let Some(WriteOperation::Insert) = operation {
                            safe = key_array_length + 1 < self.d * 2
                        } else {
                            safe = key_array_length - 1 <= self.d - 1
                        }

                        if !safe {
                            let sibling_idx = keys
                                .get(idx + 1)
                                .map(|_| idx + 1)
                                .unwrap_or_else(|| idx.saturating_sub(1));

                            let sibling_pointer = children.get(sibling_idx).unwrap().clone();

                            let (_, sibling_guard) =
                                self.get_node_obj(Protocol::Exclusive, sibling_pointer)?;

                            let _ =
                                sibling_guard.into_write_guard().expect("Page Gaurd");

                            let is_left_sibling = idx > sibling_idx;

                            stack.push_front(MergeContextStack {
                                child: child_pointer,
                                is_left_sibling: Some(is_left_sibling),
                                sibling: Some(sibling_pointer),
                            });
                        } else {
                            stack.pop_front();
                            stack.push_front(MergeContextStack {
                                child: child_pointer,
                                is_left_sibling: None,
                                sibling: None,
                            });
                        }

                        let (node, context) =
                            self.latch_sub_tree(child_node, search, context, operation)?;
                        return Ok((node, context));
                    }

                    Set::ReadStack(ref mut stack) => {
                        let (child_node, _) = self.get_node_obj(Protocol::Shared, child_pointer)?;
                        // let read_gaurd = child_guard.into_read_guard().expect("Page Gaurd");

                        stack.push_front(ReadContextStack {
                            child: child_pointer,
                        });

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

    pub fn propogate_upwards(&self, mut node: Node, mut context: Context) -> Result<(), Error> {
        println!("\n\n\nPropogating upwards\n\n\n");

        println!("Context when node split {:?}", context);

        let mut was_root = node.is_root;

        let page_id = match context.set {
            Set::WriteStack(ref mut stack) => stack.pop_front().unwrap(),
            _ => return Err(Error::UnexpectedError),
        };

        let (mut median, mut sibling) = node.split(self.d).unwrap();

        sibling.set_page_pointer(PageId(
            self.bpm.new_page(self.index_file_id).try_into().unwrap(),
        ))?;

        self.flush_data(&sibling, sibling.pointer.unwrap().0.try_into().unwrap());

        self.flush_data(&node, page_id.child.0.try_into().unwrap());

        if was_root {
            self.new_root(median.clone(), page_id.child, sibling.pointer.unwrap());
        }

        // let mut context_stack =
        //     GuardContext::new(self.root_page_id.borrow().clone(), Protocol::Exclusive);

        // match context.set {
        //     Set::WriteStack(ref mut stack) => {
        //         while let Some(guard) = stack.pop_back() {
        //             let write_page = self
        //                 .bpm
        //                 .write_page(self.index_file_id, guard.child.0.try_into().unwrap());

        //             if let GuardSet::WriteStack(ref mut guard_stack) = context_stack.set {
        //                 guard_stack.push_front(WriteContextGuardStack { child: write_page });
        //             }
        //         }
        //     }
        //     _ => return Err(Error::UnexpectedError),
        // };

        // Crabbing is hard and awkward 
        // I will move on for the sake of my sanity 
        loop {

            let stack = match context.set {
                Set::WriteStack(ref mut stack) => stack,
                _ => return Err(Error::UnexpectedError),
            };

            // let stack = match context_stack.set {
            //     GuardSet::WriteStack(ref mut stack) => stack,
            //     _ => return Err(Error::UnexpectedError),
            // };

            let current_node_id = match stack.pop_front() {
                Some(g) => g.child,
                None => {
                    drop(context);   
                    return Ok(());
                }
            };

            let current_node_guard =  self.bpm.write_page(self.index_file_id, current_node_id.0.try_into().unwrap());
            let current_page = BTreePage::new(current_node_guard.get_frame().data);

            let mut current_node = Node::try_from(current_page)?;

            current_node.insert_sibling_node(median.clone(), sibling.pointer.unwrap())?;
            
            drop(current_node_guard);

            // Insert into current with no split
            if current_node.get_key_array_length() < 2 * self.d {
                self.flush_data(
                    &current_node,
                    current_node.pointer.unwrap().0.try_into().unwrap(),
                );
                drop(context);
                return Ok(());
            }

            // insert into current and split
            was_root = current_node.is_root;

            (median, sibling) = current_node.split(self.d)?;

            sibling.set_page_pointer(PageId(
                self.bpm.new_page(self.index_file_id).try_into().unwrap(),
            ))?;

            // Write current node to disk
            self.flush_data(
                &current_node,
                current_node.pointer.unwrap().0.try_into().unwrap(),
            );


            // Writing leaf to disk
            self.flush_data(&sibling, sibling.pointer.unwrap().0.try_into().unwrap());

            // insert into current and split and current is root

            if was_root {
                
                self.new_root(
                    median.clone(),
                    current_node.pointer.unwrap(),
                    sibling.pointer.unwrap(),
                );
                return Ok(());
            }
            
            // drop(context);
            // drop(context_stack);
        }
    }
    
    pub fn insert(&self, entry: KeyValuePair) -> Result<(), Error> {
        let key = Key(entry.key);

        let (mut leaf_node, mut context) =
            self.tree_descent(key, Protocol::Exclusive, WriteOperation::Insert)?;

        leaf_node.insert_entry(entry)?;

        println!("Key length {}", leaf_node.get_key_array_length());
        if leaf_node.get_key_array_length() < 2 * self.d {
            let stack = match &mut context.set {
                Set::WriteStack(stack) => stack,
                _ => return Err(Error::UnexpectedError),
            };

            // Update the leaf_node data using the WriteGuard<'_>
            let pointer = stack.pop_front().unwrap().child;
            
            self.flush_data(&leaf_node, pointer.0.try_into().unwrap());
            
            drop(context);
            return Ok(());
        } else if leaf_node.get_key_array_length() >= 2 * self.d {
            self.propogate_upwards(leaf_node, context)?;
        }
        
        Ok(())
    }
    
        pub fn search(&self, search: Key) -> Result<KeyValuePair, Error> {
            let (node, context) = self.match_protocol(Protocol::Shared)?;
    
            if let NodeType::Leaf(_, _, _) = node.node_type {
                let kv_pair = node.find_key_value(search)?;
                return Ok(kv_pair);
            }
            println!("Descending sub tree");
            let (leaf, mut context) = self.latch_sub_tree(node, search.clone(), context, None)?;
    
            let stack = match &mut context.set {
                Set::ReadStack(stack) => stack,
                _ => return Err(Error::UnexpectedError),
            };
    
            let _ = stack.pop_front().expect("Read_guard").child;
    
            let kv_pair = leaf.find_key_value(search.clone())?;
    
            drop(context);
            return Ok(kv_pair);
        }

    pub fn get_value(&self, search: Key) -> Result<Key, Error> {
        let key = self.search(search)?.key;
        Ok(Key(key))
    }

    pub fn get_entry(&self, search: Key) -> Result<KeyValuePair, Error> {
        Ok(self.search(search)?)
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

