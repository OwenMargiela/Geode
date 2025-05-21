#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{ cell::RefCell, collections::VecDeque, sync::Arc, vec };

use super::{
    errors::Error,
    node::Node,
    node_type::{ Key, KeyValuePair, NextPointer, NodeType, PageId },
};
use crate::{
    buffer::buffer_pool_manager::{ BufferPoolManager, FileId },
    storage::page::{
        b_plus_tree_page::BTreePage,
        btree_page_layout::PAGE_SIZE,
        page_guard::PageGuard,
    },
};

pub struct ReadContextStack {
    child: PageId,
}

pub struct WriteContextStack {
    pub(crate) child: PageId,
}

#[derive(Clone, Copy)]
pub enum Protocol {
    Exclusive, // Pesimistic crabbing. Exclusive / Write latches upon tree descent
    Shared, // Optimistic crabbing. Shared / Read latches upon tree descent
}

pub enum WriteOperation {
    Delete,
    Insert,
}

// Allow for interior mutability within the buffer pool
// Flirt with the idea of using a closure to encapsulate all logic needed
// for reads and writes if needed. Or if I remember >_<
pub struct BTree {
    pub(crate) bpm: Arc<BufferPoolManager>,
    index_name: String,
    pub index_file_id: FileId,
    pub(crate) d: usize,
    root_page_id: RefCell<PageId>,
}
/// Stack of latched Internal and Leaf nodes
pub enum Set {
    // Store the guards of the pages that you're modifying here.
    WriteStack(VecDeque<WriteContextStack>),

    // you may want to use this when getting a value, but not necessary
    ReadStack(VecDeque<ReadContextStack>),
}

pub struct Context {
    // Save the root page id here so that it's easier to know if the current page is the root page.
    // This probaly should be a u32 instead of a u64
    root_page_id: PageId,

    // Store the ids of the pages that you're modifying here.
    pub(crate) set: Set,
}

impl Context {
    pub fn new(root_page_id: PageId, protocol: Protocol) -> Self {
        match protocol {
            Protocol::Exclusive => {
                return Context {
                    root_page_id,
                    set: Set::WriteStack(VecDeque::new()),
                };
            }

            Protocol::Shared => {
                return Context {
                    root_page_id,
                    set: Set::ReadStack(VecDeque::new()),
                };
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

    pub fn build(&self, index_name: String, bpm: Arc<BufferPoolManager>) -> Result<BTree, Error> {
        let index_file_id = bpm.allocate_file();
        let d: usize = self.d;

        if d == 0 {
            return Err(Error::UnexpectedError);
        }

        let page_id = bpm.new_page(index_file_id);

        {
            let root = Node::new(
                NodeType::Leaf(vec![], NextPointer(None), PageId(page_id.try_into().unwrap())),
                PageId(page_id.try_into().unwrap()),
                true
            );
            let root_page_data = BTreePage::try_from(&root)?;

            let write_page_guard = bpm.write_page(index_file_id, page_id);
            write_page_guard.get_frame().data.copy_from_slice(&root_page_data.get_data());

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
    pub(crate) fn flush_data(&self, node: &Node, page: u32) {
        let guard = self.bpm.write_page(self.index_file_id, page);
        let node_page = BTreePage::try_from(node).unwrap();

        guard.get_frame().data.copy_from_slice(&node_page.get_data());

        drop(guard);
    }

    pub fn flush_nodes(&self, nodes: Vec<&Node>) {
        for node in nodes {
            self.flush_data(node, node.pointer.unwrap().0.try_into().unwrap());
        }
    }

    pub(crate) fn new_root(&self, key: Key, left_child: PageId, right_child: PageId) {
        let new_root_pointer = PageId(self.bpm.new_page(self.index_file_id).try_into().unwrap());

        let new_root = Node::new(
            NodeType::Internal(vec![left_child, right_child], vec![key], new_root_pointer),
            new_root_pointer,
            true
        );
        let mut page = self.root_page_id.try_borrow_mut().unwrap();
        *page = new_root_pointer;

        self.flush_data(&new_root, new_root_pointer.0.try_into().unwrap());
    }

    pub(crate) fn get_gaurd(
        &self,
        page_id: PageId,
        acess_type: Protocol
    ) -> Result<PageGuard, Error> {
        match acess_type {
            Protocol::Shared => {
                let read_guard = self.bpm.read_page(
                    self.index_file_id,
                    page_id.0.try_into().unwrap()
                );

                return Ok(PageGuard::ReadGuard(read_guard));
            }

            Protocol::Exclusive => {
                let write_guard = self.bpm.write_page(
                    self.index_file_id,
                    page_id.0.try_into().unwrap()
                );

                return Ok(PageGuard::WriteGuard(write_guard));
            }
        }
    }

    pub(crate) fn get_node_obj(
        &self,
        protocol: Protocol,
        page_id: PageId
    ) -> Result<(Node, PageGuard), Error> {
        match protocol {
            Protocol::Exclusive => {
                let page_guard = self
                    .get_gaurd(page_id, Protocol::Exclusive)
                    .unwrap()
                    .into_write_guard()
                    .unwrap();

                let page_data = page_guard.get_frame().data;

                let page = BTreePage::new(page_data);
                let node = Node::try_from(page).unwrap();

                return Ok((node, PageGuard::WriteGuard(page_guard)));
            }
            Protocol::Shared => {
                let page_guard = self
                    .get_gaurd(page_id, Protocol::Shared)
                    .unwrap()
                    .into_read_guard()
                    .unwrap();

                let page_data = page_guard.get_frame().data;

                let page = BTreePage::new(page_data);
                let node = Node::try_from(page).unwrap();

                return Ok((node, PageGuard::ReadGuard(page_guard)));
            }
        }
    }

    // Looks like
    // Returns the node, if we can borrow from it and whether or not its the left node
    pub(crate) fn find_rebalance_candidate(
        &self,
        node: &Node,
        page_id: PageId
    ) -> Result<(Node, Key, bool, bool), Error> {
        match &node.node_type {
            NodeType::Internal(children, keys, _) => {
                let node_idx = match children.binary_search(&page_id) {
                    Ok(idx) => idx,
                    Err(_) => {
                        return Err(Error::KeyNotFound);
                    }
                };

                let sparator_key = keys.get(node_idx).unwrap_or_else(|| &keys[node_idx - 1]);

                let mut candidate_index = node_idx + 1;
                let can_borrow: bool;

                if let Some(id) = children.get(candidate_index) {
                    let (candidate_node, _) = self.get_node_obj(Protocol::Shared, id.clone())?;

                    can_borrow = candidate_node.get_key_array_length() >= self.d;

                    return Ok((candidate_node, sparator_key.clone(), false, can_borrow));
                }

                candidate_index = node_idx - 1;
                let (candidate_node, _) = self.get_node_obj(
                    Protocol::Shared,
                    children.get(candidate_index).unwrap().clone()
                )?;

                can_borrow = candidate_node.get_key_array_length() >= self.d;
                println!("\n\n Separator {:?}", sparator_key);
                return Ok((candidate_node, sparator_key.clone(), true, can_borrow));
            }
            _ => {
                return Err(Error::UnexpectedError);
            }
        }
    }

    // obtains a latch on the parent
    pub(crate) fn match_protocol(&self, protocol: Protocol) -> Result<(Node, Context), Error> {
        let mut _btree_page: BTreePage = BTreePage {
            data: [0u8; PAGE_SIZE],
        };

        let mut context = Context::new(*self.root_page_id.borrow(), protocol);
        let root_guard: PageGuard<'_>;

        {
            match protocol {
                Protocol::Exclusive => {
                    root_guard = PageGuard::WriteGuard(
                        self.bpm.write_page(
                            self.index_file_id,
                            self.root_page_id.borrow().0.try_into().unwrap()
                        )
                    );

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
                    root_guard = PageGuard::ReadGuard(
                        self.bpm.read_page(
                            self.index_file_id,
                            self.root_page_id.borrow().0.try_into().unwrap()
                        )
                    );

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

    pub(crate) fn tree_descent(
        &self,
        search_key: Key,
        protocol: Protocol,
        operation: WriteOperation
    ) -> Result<(Node, Context), Error> {
        let (root_node, context) = self.match_protocol(protocol)?;

        let (leaf_node, context) = self.latch_sub_tree(
            root_node,
            search_key,
            context,
            Some(operation)
        )?;

        Ok((leaf_node, context))
    }

    /// Descends towards the desired leaf node while aquiring read/write latches
    /// Returns the gaurds of all the nodes needed for the operation
    pub(crate) fn latch_sub_tree<'a>(
        &'a self,
        parent: Node,
        search: Key,
        mut context: Context,
        operation: Option<WriteOperation>
    ) -> Result<(Node, Context), Error> {
        match &parent.node_type {
            NodeType::Internal(children, keys, _) => {
                let idx = keys.binary_search(&search).unwrap_or_else(|x| x);
                let child_pointer: PageId;

                if idx >= keys.len() || keys[idx] != search {
                    child_pointer = children.get(idx).unwrap().clone();
                } else {
                    child_pointer = children
                        .get(idx + 1)
                        .unwrap()
                        .clone();
                }

                match context.set {
                    Set::WriteStack(ref mut stack) => {
                        let (child_node, _) = self.get_node_obj(
                            Protocol::Exclusive,
                            child_pointer
                        )?;

                        let key_array_length = child_node.get_key_array_length();
                        let safe: bool;

                        // Check if node is safe
                        if let Some(WriteOperation::Insert) = operation {
                            safe = key_array_length + 1 < self.d * 2;
                        } else {
                            safe = key_array_length - 1 > self.d - 1;
                        }

                        if safe {
                            stack.pop_front();
                            stack.push_front(WriteContextStack {
                                child: child_pointer,
                            });
                        } else {
                            stack.push_front(WriteContextStack {
                                child: child_pointer,
                            });
                        }

                        let (node, context) = self.latch_sub_tree(
                            child_node,
                            search,
                            context,
                            operation
                        )?;
                        return Ok((node, context));
                    }

                    Set::ReadStack(ref mut stack) => {
                        let (child_node, _) = self.get_node_obj(Protocol::Shared, child_pointer)?;

                        stack.push_front(ReadContextStack {
                            child: child_pointer,
                        });

                        let (node, context) = self.latch_sub_tree(
                            child_node,
                            search,
                            context,
                            operation
                        )?;

                        return Ok((node, context));
                    }
                }
            }

            NodeType::Leaf(_, _, _) => {
                return Ok((parent, context));
            }

            NodeType::Unexpected => {
                return Err(Error::UnexpectedError);
            }
        }
    }

    pub(crate) fn propogate_upwards(
        &self,
        mut node: Node,
        mut context: Context
    ) -> Result<(), Error> {
        let mut was_root = node.is_root;

        let page_id = match context.set {
            Set::WriteStack(ref mut stack) => stack.pop_front().unwrap(),
            _ => {
                return Err(Error::UnexpectedError);
            }
        };

        let (mut median, mut sibling) = node.split(self.d).unwrap();

        sibling.set_page_pointer(
            PageId(self.bpm.new_page(self.index_file_id).try_into().unwrap())
        )?;

        node.set_next_pointer(sibling.pointer.unwrap().0.to_le_bytes())?;
        sibling.set_next_pointer([0u8; 8])?;

        self.flush_nodes(vec![&sibling, &node]);

        if was_root {
            self.new_root(median.clone(), page_id.child, sibling.pointer.unwrap());
        }

        // Crabbing is hard and awkward
        // I will move on for the sake of my sanity

        loop {
            let stack = match context.set {
                Set::WriteStack(ref mut stack) => stack,
                _ => {
                    return Err(Error::UnexpectedError);
                }
            };

            let current_node_id = match stack.pop_front() {
                Some(g) => g.child,
                None => {
                    drop(context);
                    return Ok(());
                }
            };

            let (mut current_node, _) = self.get_node_obj(Protocol::Shared, current_node_id)?;
            current_node.insert_sibling_node(median.clone(), sibling.pointer.unwrap())?;

            // Insert into current with no split
            if current_node.get_key_array_length() < 2 * self.d {
                self.flush_data(&current_node, current_node.pointer.unwrap().0.try_into().unwrap());
                drop(context);
                return Ok(());
            }

            // insert into current and split
            was_root = current_node.is_root;

            (median, sibling) = current_node.split(self.d)?;

            sibling.set_page_pointer(
                PageId(self.bpm.new_page(self.index_file_id).try_into().unwrap())
            )?;

            // Write current node and its sibling to disk
            self.flush_nodes(vec![&current_node, &sibling]);

            // insert into current and split and current is root

            if was_root {
                self.new_root(
                    median.clone(),
                    current_node.pointer.unwrap(),
                    sibling.pointer.unwrap()
                );
                return Ok(());
            }
        }
    }

    pub(crate) fn borrow_if_needed(
        &self,
        child_id: PageId,
        child_node: Node,
        mut context: Context
    ) -> Result<(), Error> {
        // Aquire parent
        // Aqquire either left sibling  or right

        let parent_id = match context.set {
            Set::WriteStack(ref mut stack) => stack.pop_front().unwrap().child,
            _ => {
                return Err(Error::UnexpectedError);
            }
        };

        let (mut parent_node, _) = self.get_node_obj(Protocol::Exclusive, parent_id)?;

        println!("Finding candidatee");
        let (mut candidate, mut separator_key, mut is_left, mut can_borrow) =
            self.find_rebalance_candidate(&parent_node, child_id)?;

        let mut current_node = child_node;
        let mut current_candidate = candidate;
        let mut current_parent_node = parent_node;

        println!("\n\n\nParent {:? }", current_parent_node);
        println!("\n\n\nCurrent {:? }", current_node);
        println!("\n\n\nCandidate {:? }", current_candidate);

        loop {
            if can_borrow {
                println!("Yes");
                current_node.borrow(
                    &mut current_parent_node,
                    &mut current_candidate,
                    is_left,
                    separator_key.clone()
                )?;

                drop(context);
                // Write to disk
                self.flush_nodes(vec![&current_node, &current_candidate, &current_parent_node]);

                return Ok(());
            }

            match current_node.node_type {
                NodeType::Internal(_, _, _) => {
                    current_node.merge_internal(&current_candidate)?;

                    current_node.insert_key(separator_key.clone())?;
                }
                NodeType::Leaf(_, _, _) => {
                    current_node.merge_leaf(&current_candidate)?;
                }
                NodeType::Unexpected => {
                    return Err(Error::UnexpectedError);
                }
            }

            current_parent_node.remove_sibling_node(
                separator_key.clone(),
                current_candidate.pointer.unwrap()
            )?;

            if let Ok(size) = current_parent_node.get_number_of_children() {
                if size < 2 && current_parent_node.is_root {
                    current_node.is_root = true;
                    current_parent_node.is_root = false;

                    let mut root_page = self.root_page_id.borrow_mut();
                    *root_page = current_node.pointer.unwrap();
                }
            }

            self.flush_nodes(vec![&current_node, &current_parent_node, &current_candidate]);

            if current_parent_node.get_key_array_length() >= self.d - 1 {
                return Ok(());
            } else {
                current_node = current_parent_node;

                let parent_id = match context.set {
                    Set::WriteStack(ref mut stack) =>
                        match stack.pop_front() {
                            Some(entry) => entry.child,
                            None => {
                                return Ok(());
                            }
                        }
                    _ => {
                        return Err(Error::UnexpectedError);
                    }
                };

                (parent_node, _) = self.get_node_obj(Protocol::Exclusive, parent_id)?;

                println!("\n\nParent {:?} ", parent_node);

                println!("\n\nCurrent pointer {:?} ", current_node.pointer.unwrap());

                (candidate, separator_key, is_left, can_borrow) = self.find_rebalance_candidate(
                    &parent_node,
                    current_node.pointer.unwrap()
                )?;

                current_candidate = candidate;
                current_parent_node = parent_node;
            }
        }
    }

    pub fn find_node(&self, search: &Key) -> Result<Node, Error> {
        let (node, _) = self.match_protocol(Protocol::Shared)?;

        if let NodeType::Leaf(_, _, _) = node.node_type {
            if node.find_key_value(search).is_ok() {
                return Ok(node);
            } else {
                return Err(Error::UnexpectedError);
            }
        }

        let mut current_node = node;
        loop {
            match &current_node.node_type {
                NodeType::Internal(ref children, ref keys, _) => {
                    let idx = keys.binary_search(&search).unwrap_or_else(|x| x);

                    if idx >= keys.len() || keys[idx] != *search {
                        let child_pointer = children.get(idx).unwrap().clone();

                        (current_node, _) = self
                            .get_node_obj(Protocol::Shared, child_pointer)
                            .unwrap();
                    } else {
                        (current_node, _) = self.get_node_obj(
                            Protocol::Shared,
                            children
                                .get(idx + 1)
                                .unwrap()
                                .clone()
                        )?;
                    }
                }
                NodeType::Leaf(_, _, _) => {
                    if current_node.find_key_value(search).is_ok() {
                        return Ok(current_node);
                    } else {
                        return Err(Error::UnexpectedError);
                    }
                }
                _ => {
                    return Err(Error::UnexpectedError);
                }
            }
        }
    }

    pub fn find_min(&self) -> Result<Node, Error> {
        let (node, _) = self.match_protocol(Protocol::Shared)?;

        if let NodeType::Leaf(ref entries, _, _) = node.node_type {
            return Ok(node);
        }

        let mut current_node = node;
        loop {
            match &current_node.node_type {
                NodeType::Internal(ref children, ref keys, _) => {
                    let child_pointer = children.get(0).unwrap().clone();

                    (current_node, _) = self.get_node_obj(Protocol::Shared, child_pointer).unwrap();
                }
                NodeType::Leaf(_, _, _) => {
                    return Ok(current_node);
                }
                _ => {
                    return Err(Error::UnexpectedError);
                }
            }
        }
    }

    pub fn search(&self, search: Key) -> Result<KeyValuePair, Error> {
        let (node, context) = self.match_protocol(Protocol::Shared)?;

        if let NodeType::Leaf(_, _, _) = node.node_type {
            let kv_pair = node.find_key_value(&search)?;
            return Ok(kv_pair);
        }

        let kv_pair: Option<KeyValuePair>;
        let mut current_node = node;
        loop {
            match &current_node.node_type {
                NodeType::Internal(ref children, ref keys, _) => {
                    let idx = keys.binary_search(&search).unwrap_or_else(|x| x);

                    if idx >= keys.len() || keys[idx] != search {
                        let child_pointer = children.get(idx).unwrap().clone();
                        (current_node, _) = self.get_node_obj(Protocol::Shared, child_pointer)?;
                    } else {
                        (current_node, _) = self.get_node_obj(
                            Protocol::Shared,
                            children
                                .get(idx + 1)
                                .unwrap()
                                .clone()
                        )?;
                    }
                }
                NodeType::Leaf(_, _, _) => {
                    kv_pair = Some(current_node.find_key_value(&search)?);

                    break;
                }
                _ => {
                    kv_pair = None;
                    break;
                }
            }
        }

        Ok(kv_pair.unwrap())
    }

    pub(crate) fn print(&self) {
        let (root_node, _) = self.match_protocol(Protocol::Shared).unwrap();
        self.print_tree(&root_node, 0);
    }

    fn print_tree(&self, node: &Node, depth: usize) {
        match &node.node_type {
            NodeType::Internal(children, keys, current) => {
                if node.is_root {
                    print!("Is Root ");
                }
                println!("\nChildren {:?} Current Page {:?}", children, current);
                print!("Keys [ ");
                for key in keys {
                    print!("Keys ( {:?} )", u64::from_le_bytes(key.0[0..8].try_into().unwrap()));
                }
                print!(" ]\n");

                for child in children {
                    let (child, _) = self.get_node_obj(Protocol::Shared, *child).unwrap();
                    self.print_tree(&child, depth + 1);
                }
            }
            NodeType::Leaf(entries, next, current) => {
                println!("\nEntries [ ");
                for entry in entries {
                    print!(
                        "        Key {:?} Page {:?} Slot {:?}\n",
                        u64::from_le_bytes(entry.key[0..8].try_into().unwrap()),
                        u32::from_le_bytes(entry.value.page_id),
                        u32::from_le_bytes(entry.value.slot_index)
                    );
                }
                println!("\n], current {:?} next {:?} \n ", current, next);
            }
            NodeType::Unexpected => {
                return;
            }
        }
    }
}
