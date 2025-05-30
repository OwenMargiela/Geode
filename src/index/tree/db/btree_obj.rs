#![allow(unused_variables)] 
#![allow(dead_code)] 

use std::{ cell::RefCell, collections::VecDeque, path::Path, sync::{ Arc, Mutex } };

use anyhow::Ok;

use crate::{
    buffer::{ buffer_pool_manager::{ BufferPoolManager, FileId }, flusher::{ Flusher, Lock } },
    index::tree::{
        byte_box::DataType,
        index_types::{ KeyValuePair, NodeKey },
        tree_node::{ node_type::{ NodeType, PagePointer }, tree_node_inner::NodeInner },
        tree_page::{ codec::Codec, page::TreePage, tree_page_layout::PAGE_SIZE },
    },
    storage::disk::manager::Manager,
};

pub enum WriteOperation {
    Delete,
    Insert,
}

pub struct BPTree {
    pub flusher: Arc<Flusher>,
    pub root_page_id: RefCell<PagePointer>,

    pub index_id: FileId,

    pub(crate) b: usize,
    pub codec: Codec,
}

const NUM_FRAMES: usize = 10;
const K_DIST: usize = 2;

pub struct BTreeBuilder {
    /// Path to the tree file
    // Parameter b is the orber of the tree
    // Each	non-leaf nobe contains b ≤ m ≤ 2b entries
    // minimum	50%	occupancy at all times
    // The root nobe can have 1 ≤ m ≤ 2b entries
    b: usize,
    // The number of keys in a btree is representeb as
    // num_of_keys = 2b - 1

    // The number of pointers to chilb nodes in a btree is represented as
    // num_of_pointers = 2b

    table_schema: Codec,
}

impl BTreeBuilder {
    pub fn new() -> BTreeBuilder {
        BTreeBuilder {
            b: 0,
            table_schema: Codec { key_type: DataType::None, value_type: DataType::None },
        }
    }
    pub fn b_parameter(&mut self, b: usize) -> &mut Self {
        self.b = b;
        self
    }

    pub fn tree_schema(&mut self, codec: Codec) -> &mut Self {
        self.table_schema = codec;
        self
    }

    pub fn build_from_file(
        &self,
        path: impl AsRef<Path> + std::marker::Copy + std::fmt::Debug
    ) -> anyhow::Result<BPTree> {
        let (log_io, log_file_path) = Manager::open_log();
        let manager = Manager::new(log_io, log_file_path);

        let bpm = Arc::new(BufferPoolManager::new(NUM_FRAMES, manager, K_DIST));
        let file_id = bpm.open_file(path);

        let flusher = Flusher::new(bpm, file_id);

        Ok(BPTree {
            flusher: Arc::new(flusher),
            root_page_id: RefCell::new(0),
            index_id: file_id,
            b: self.b.clone(),
            codec: self.table_schema.clone(),
        })
    }

    pub fn build(&self) -> anyhow::Result<BPTree> {
        let (log_io, log_file_path) = Manager::open_log();
        let manager = Manager::new(log_io, log_file_path);

        let bpm = Arc::new(BufferPoolManager::new(NUM_FRAMES, manager, K_DIST));
        let file_id = bpm.allocate_file();

        let flusher = Flusher::new(bpm, file_id);

        let page_pointer = flusher.new_page();
        let root = NodeInner::new(
            NodeType::Leaf(vec![], page_pointer, None),
            true,
            page_pointer,
            None
        );

        let root_page_data = Codec::encode(&root)?;
        flusher.write_flush(root_page_data.get_data(), page_pointer)?;

        Ok(BPTree {
            flusher: Arc::new(flusher),
            root_page_id: RefCell::new(page_pointer),
            index_id: file_id,
            b: self.b.clone(),
            codec: self.table_schema.clone(),
        })
    }

    pub fn build_table(
        &self,
        table_name: String,
        manager: Arc<Mutex<Manager>>,
        flusher: Arc<Flusher>,
        file_id: u64
    ) -> anyhow::Result<BPTree> {

        
        let bpm = Arc::new(BufferPoolManager::new_with_arc(NUM_FRAMES, manager, K_DIST));

        let flusher = Flusher::new(bpm, file_id);

        let page_pointer = flusher.new_page();
        let root = NodeInner::new(
            NodeType::Leaf(vec![], page_pointer, None),
            true,
            page_pointer,
            None
        );

        let root_page_data = Codec::encode(&root)?;
        flusher.write_flush(root_page_data.get_data(), page_pointer)?;

        Ok(BPTree {
            flusher: Arc::new(flusher),
            root_page_id: RefCell::new(page_pointer),
            index_id: file_id,
            b: self.b.clone(),
            codec: self.table_schema.clone(),
        })
    }
}

impl BPTree {
    pub(crate) fn new_root(&self, key: NodeKey, left_child: PagePointer, right_child: PagePointer) {
        let new_root_pointer = self.flusher.new_page();
        let new_root = NodeInner::new(
            NodeType::Internal(vec![left_child, right_child], vec![key], new_root_pointer),
            true,
            new_root_pointer,
            None
        );

        let mut page_id = self.root_page_id.try_borrow_mut().unwrap();
        *page_id = new_root_pointer;

        let page = Codec::encode(&new_root).unwrap();

        self.flusher.write_flush(page.get_data(), new_root_pointer).unwrap();
    }

    pub fn search(&self, search: NodeKey) -> anyhow::Result<KeyValuePair> {
        let page = self.flusher.read_drop(*self.root_page_id.borrow());

        let parent = self.codec.decode(&TreePage::new(page)).unwrap();

        if let NodeType::Leaf(entires, _, _) = parent.node_type {
            let index = NodeInner::find_key(search, &entires)?;

            let node_key = entires.get(index).unwrap();

            let (key_bytes, value) = NodeInner::deconstruct_value(node_key);

            return Ok(KeyValuePair {
                key: key_bytes,
                value: value.unwrap(),
            });
        }

        let kv_pair: Option<KeyValuePair>;
        let mut current_node = parent;
        loop {
            match &current_node.node_type {
                NodeType::Internal(ref children, ref keys, _) => {
                    let idx = keys.binary_search(&search).unwrap_or_else(|x| x);

                    if idx >= keys.len() || keys[idx] != search {
                        let child_pointer = children.get(idx).unwrap().clone();

                        let page = &TreePage::new(self.flusher.read_drop(child_pointer));

                        current_node = self.codec.decode(page).unwrap();
                    } else {
                        let page = &TreePage::new(
                            self.flusher.read_drop(
                                children
                                    .get(idx + 1)
                                    .unwrap()
                                    .clone()
                            )
                        );
                        current_node = self.codec.decode(page).unwrap();
                    }
                }
                NodeType::Leaf(entries, _, _) => {
                    let index = NodeInner::find_key(search, &entries)?;

                    let node_key = entries.get(index).unwrap();

                    let (key_bytes, value) = NodeInner::deconstruct_value(node_key);

                    kv_pair = Some(KeyValuePair {
                        key: key_bytes,
                        value: value.unwrap(),
                    });

                    break;
                }
                _ => {
                    kv_pair = None;
                    break;
                }
            }
        }
        return Ok(kv_pair.unwrap());
    }

    pub fn find_min(&self) -> anyhow::Result<NodeInner> {
        let mut page = self.flusher.read_drop(*self.root_page_id.borrow());

        let parent = self.codec.decode(&TreePage::new(page)).unwrap();

        if let NodeType::Leaf(ref entries, _, _) = parent.node_type {
            return Ok(parent);
        }

        let mut current_node = parent;
        loop {
            match &current_node.node_type {
                NodeType::Internal(ref children, ref keys, _) => {
                    let child_pointer = children.get(0).unwrap().clone();

                    page = self.flusher.read_drop(child_pointer);

                    current_node = self.codec.decode(&TreePage::new(page)).unwrap();
                }
                NodeType::Leaf(_, _, _) => {
                    return Ok(current_node);
                }
                _ => {
                    return Err(anyhow::Error::msg("Unexpected error"));
                }
            }
        }
    }

    pub fn find_node(&self, search: NodeKey) -> anyhow::Result<NodeInner> {
        let page = self.flusher.read_drop(*self.root_page_id.borrow());

        let parent = self.codec.decode(&TreePage::new(page)).unwrap();

        if let NodeType::Leaf(ref entires, _, _) = parent.node_type {
            let index = NodeInner::find_key(search, &entires)?;

            let node_key = entires.get(index).unwrap();

            let (key_bytes, value) = NodeInner::deconstruct_value(node_key);

            return Ok(parent);
        }

        let kv_pair: Option<KeyValuePair>;
        let mut current_node = parent;
        loop {
            match &current_node.node_type {
                NodeType::Internal(ref children, ref keys, _) => {
                    let idx = keys.binary_search(&search).unwrap_or_else(|x| x);

                    if idx >= keys.len() || keys[idx] != search {
                        let child_pointer = children.get(idx).unwrap().clone();

                        let page = &TreePage::new(self.flusher.read_drop(child_pointer));

                        current_node = self.codec.decode(page).unwrap();
                    } else {
                        let page = &TreePage::new(
                            self.flusher.read_drop(
                                children
                                    .get(idx + 1)
                                    .unwrap()
                                    .clone()
                            )
                        );
                        current_node = self.codec.decode(page).unwrap();
                    }
                }
                NodeType::Leaf(entries, _, _) => {
                    let index = NodeInner::find_key(search.clone(), &entries)?;

                    if
                        entries.get(index).unwrap().to_kv_pair().unwrap().key ==
                        search.to_guide_post().unwrap()
                    {
                        return Ok(current_node);
                    } else {
                        return Err(anyhow::Error::msg("Node not present"));
                    }
                }
                _ => {
                    return Err(anyhow::Error::msg("Unexpected error"));
                }
            }
        }
    }

    pub(crate) fn print(&self) {
        println!("Breakpoint 1");
        let page = self.flusher.read_drop(*self.root_page_id.borrow());
        println!("Breakpoint 2");
        let parent = self.codec.decode(&TreePage::new(page)).unwrap();
        self.print_tree(&parent, 0);
    }

    pub(self) fn print_tree(&self, node: &NodeInner, depth: usize) {
        match &node.node_type {
            NodeType::Internal(children, keys, current) => {
                if node.is_root {
                    print!("Is Root ");
                }
                println!("\nChildren {:?} Current Page {:?}", children, node.pointer);
                print!("Keys [ ");
                for key in keys {
                    let (key_bytes, _) = NodeInner::deconstruct_value(key);
                    print!("Keys ( {:?} )", key_bytes);
                }
                print!(" ]\n");

                for child in children {
                    println!("Breakpoint alpha");
                    let page = self.flusher.read_drop(*child);
                    println!("Breakpoint beta");
                    let child = self.codec.decode(&TreePage::new(page)).unwrap();
                    self.print_tree(&child, depth + 1);
                }
            }
            NodeType::Leaf(entries, next, current) => {
                println!("\nEntries [ ");
                for entry in entries {
                    if let NodeKey::KeyValuePair(entry) = entry {
                        print!("         {:?}\n", entry);
                    }
                }
                println!("\n], current {:?} next {:?} \n ", node.pointer, node.next_pointer);
            }
            NodeType::Unexpected => {
                return;
            }
        }
    }

    pub(self) fn get_candidate(
        &self,
        parent: &NodeInner,
        child_id: PagePointer
    ) -> anyhow::Result<(NodeInner, bool, NodeKey, bool)> {
        match &parent.node_type {
            NodeType::Internal(children, keys, _) => {
                let node_idx = match children.binary_search(&child_id) {
                    std::result::Result::Ok(idx) => idx,
                    Err(_) => {
                        return Err(anyhow::Error::msg("Key not found"));
                    }
                };

                let separator_key = keys.get(node_idx).unwrap_or_else(|| &keys[node_idx - 1]);
                let mut is_left = false;
                let candidate_index = node_idx + 1;

                let candidate: NodeInner;
                let can_borrow: bool;

                if let Some(id) = children.get(candidate_index) {
                    let page = self.flusher.read_drop(*id);

                    candidate = self.codec.decode(&TreePage::new(page))?;
                    can_borrow = candidate.can_borrow(self.b);
                } else {
                    let id = children.get(node_idx - 1).unwrap();
                    is_left = true;

                    let page = self.flusher.read_drop(*id);
                    candidate = self.codec.decode(&TreePage::new(page))?;
                    can_borrow = candidate.can_borrow(self.b);
                }

                return Ok((candidate, is_left, separator_key.clone(), can_borrow));
            }

            _ => {
                return Err(anyhow::Error::msg("Unexpected error"));
            }
        };
    }

    pub(crate) fn borrow_if_needed(
        &self,
        parent: NodeInner,
        child_node: NodeInner,
        child_id: PagePointer
        // mut context: Context,
    ) -> anyhow::Result<()> {
        let (mut candidate, mut is_left, mut separator, mut can_borrow) = self.get_candidate(
            &parent,
            child_node.pointer
        )?;

        let mut current_node = child_node;
        let mut current_candidate = candidate;
        let mut current_parent_node = parent;

        loop {
            if can_borrow {
                println!("Yes");
                current_node.borrow(
                    &mut current_parent_node,
                    &mut current_candidate,
                    is_left,
                    separator.clone()
                )?;

                self.flusher.pop_flush(
                    Codec::encode(&current_node).unwrap().get_data(),
                    current_node.pointer
                )?;

                self.flusher.pop_flush(
                    Codec::encode(&current_parent_node).unwrap().get_data(),
                    current_parent_node.pointer
                )?;

                let candidate_id = current_candidate.pointer;

                self.flusher.write_flush(
                    Codec::encode(&current_candidate).unwrap().get_data(),
                    candidate_id
                )?;

                return Ok(());
            }

            current_node.merge(&current_candidate)?;

            match current_node.node_type {
                NodeType::Internal(_, _, _) => {
                    current_node.insert_key(separator.clone())?;
                }
                _ => {}
            }

            current_parent_node.remove_sibling_node(separator.clone(), current_candidate.pointer)?;

            if current_parent_node.is_root && current_parent_node.child_ptr_len() < self.b {
                current_node.is_root = true;
                current_parent_node.is_root = false;

                *self.root_page_id.borrow_mut() = current_node.pointer;

                self.flusher.pop_flush(
                    Codec::encode(&current_node).unwrap().get_data(),
                    current_node.pointer
                )?;

                self.flusher.pop_flush(
                    Codec::encode(&current_parent_node).unwrap().get_data(),
                    current_parent_node.pointer
                )?;
                return Ok(());
            } else {
                self.flusher.pop_flush(
                    Codec::encode(&current_node).unwrap().get_data(),
                    current_node.pointer
                )?;
            }

            if current_parent_node.get_key_array_length() >= self.b - 1 {
                self.flusher.pop_flush(
                    Codec::encode(&current_parent_node).unwrap().get_data(),
                    current_parent_node.pointer
                )?;

                let candidate_id = current_candidate.pointer;

                self.flusher.write_flush(
                    Codec::encode(&current_candidate).unwrap().get_data(),
                    candidate_id
                )?;

                return Ok(());
            } else {
                current_node = current_parent_node.clone();
                self.flusher.get_stack();
                let page = self.flusher.read_parent()?;

                let parent = self.codec.decode(&TreePage::new(page))?;

                (candidate, is_left, separator, can_borrow) = self.get_candidate(
                    &parent,
                    current_node.pointer
                )?;

                current_candidate = candidate;

                current_parent_node = parent;
            }
        }
    }

    pub(crate) fn tree_descent(
        &self,
        search_key: NodeKey,
        lock: Lock,
        operation: WriteOperation
    ) -> anyhow::Result<VecDeque<PagePointer>> {
        let mut contex: VecDeque<PagePointer> = VecDeque::new();

        let root_id = *self.root_page_id.borrow_mut();
        contex.push_front(root_id);

        let mut page = self.flusher.read_drop(root_id);

        let mut parent = self.codec.decode(&TreePage::new(page))?;

        loop {
            match &parent.node_type {
                NodeType::Internal(children, keys, _) => {
                    let idx = keys.binary_search(&search_key).unwrap_or_else(|x| x);
                    let child_pointer: PagePointer;

                    if idx >= keys.len() || keys[idx] != search_key {
                        child_pointer = children.get(idx).unwrap().clone();
                    } else {
                        child_pointer = children
                            .get(idx + 1)
                            .unwrap()
                            .clone();
                    }

                    match lock {
                        Lock::EXLOCK => {
                            page = self.flusher.read_drop(child_pointer);

                            let child_node = self.codec.decode(&TreePage::new(page))?;

                            let key_array_length = child_node.get_key_array_length();
                            let safe: bool;

                            // Check if node is safe
                            // Future implementation will rely on less arbituary params
                            if let WriteOperation::Insert = operation {
                                safe = key_array_length + 1 < self.b * 2;
                            } else {
                                safe = key_array_length - 1 > self.b - 1;
                            }

                            if safe {
                                let page = contex.pop_front();
                                contex.push_front(child_pointer);
                            } else {
                                contex.push_front(child_pointer);
                            }

                            parent = child_node;
                        }
                        Lock::SHLOCK => {
                            page = self.flusher.read_drop(child_pointer);

                            let child_node = self.codec.decode(&TreePage::new(page))?;

                            parent = child_node;
                        }
                    }
                }

                NodeType::Leaf(_, pointer, _) => {
                    // contex.push_front(pointer.clone());
                    return Ok(contex);
                }

                NodeType::Unexpected => {
                    return Err(anyhow::Error::msg("Unexpected error"));
                }
            }
        }
    }

    pub(crate) fn propogate_upwards(
        &self,
        mut node: NodeInner
        // mut context: Context,
    ) -> anyhow::Result<()> {
        let mut was_root = node.is_root;

        let (mut median, mut sibling) = node.split(self.b).unwrap();

        // New func to set pointer
        sibling.next_pointer = None;

        let page_pointer = node.pointer;

        // New func to set pointer
        sibling.pointer = self.flusher.new_page();

        // New func to set pointer
        node.next_pointer = Some(sibling.pointer);

        let mut data: [u8; PAGE_SIZE];

        {
            data = Codec::encode(&sibling).unwrap().get_data();
            self.flusher.write_flush(data, sibling.pointer)?;
        }

        {
            data = Codec::encode(&node).unwrap().get_data();
            self.flusher.pop_flush(data, node.pointer)?;

            let test = self.flusher.read_drop(node.pointer);
            let test_node = self.codec.decode(&TreePage::new(test))?;
        }

        if was_root {
            self.new_root(median.clone(), node.pointer, sibling.pointer);
        }

        loop {
            let data = match self.flusher.read_top() {
                anyhow::Result::Ok(data) => {
                    data
                    // Use `data: [u8; BLOCK_SIZE]`
                }
                Err(e) => {
                    return Ok(());
                }
            };

            let mut current_node = self.codec.decode(&TreePage::new(data)).unwrap();
            current_node.insert_sibling_node(median.clone(), sibling.pointer)?;

            // Insert into current with no split
            if current_node.get_key_array_length() < 2 * self.b {
                self.flusher
                    .pop_flush(
                        Codec::encode(&current_node).unwrap().get_data(),
                        current_node.pointer
                    )
                    .unwrap();
                self.flusher.release_ex()?;

                return Ok(());
            }

            // insert into current and split
            was_root = current_node.is_root;

            (median, sibling) = current_node.split(self.b).unwrap();

            // New func to set pointer
            sibling.pointer = self.flusher.new_page();

            self.flusher
                .pop_flush(Codec::encode(&current_node).unwrap().get_data(), current_node.pointer)
                .unwrap();

            {
                let data = Codec::encode(&sibling).unwrap().get_data();
                self.flusher.write_flush(data, sibling.pointer)?;
            }

            if was_root {
                self.new_root(median.clone(), current_node.pointer, sibling.pointer);
            }
        }
    }
}
