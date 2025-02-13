use std::{
    collections::HashMap,
    sync::atomic::{AtomicIsize, AtomicUsize},
};

pub trait Replacer {
    fn evict(&mut self) -> Option<u32>;
    fn record_access(&mut self, entriy_id: u32);
    fn set_evictable(&mut self, entriy_id: u32, evictability: bool);
    fn remove(&mut self, entriy_id: u32) -> bool;
    fn size(&self) -> usize;
}

pub struct LRUKNode {
    history: Vec<usize>,
    is_evictable: bool,
    k: usize,
    eid: u32,
}

impl LRUKNode {
    pub fn new(k: usize, entry_id: u32) -> Self {
        LRUKNode {
            history: Vec::new(),
            is_evictable: false,
            k: k,
            eid: entry_id,
        }
    }

    fn set_evictable(&mut self, evictability: bool) {
        self.is_evictable = evictability;
    }
    // most recent -> oldest entires
    fn push_timestamp(&mut self, timestamp: usize) {
        self.history.insert(0, timestamp);
    }

    fn get_kth_entry(&self) -> Option<&usize> {
        let length = self.history.len();
        if self.k - 1 > length {
            return None;
        }
        Some(self.history.get(self.k - 1).unwrap())
    }

    fn get_last_entry(&self) -> &usize {
        self.history.last().expect("Empty History")
    }
}

pub struct LRUKReplacer {
    node_store: HashMap<u32, LRUKNode>,
    current_timestamp: AtomicUsize,
    current_size: AtomicUsize,
    replacer_size: usize,
    evictable_size: AtomicUsize,
    k: usize,
}
impl LRUKReplacer {
    fn new(number_of_entries: usize, k: usize) -> Self {
        LRUKReplacer {
            node_store: HashMap::new(),
            current_timestamp: AtomicUsize::new(0),
            current_size: AtomicUsize::new(0),
            replacer_size: number_of_entries,
            evictable_size: AtomicUsize::new(0),
            k: k,
        }
    }
}

impl Replacer for LRUKReplacer {
    fn record_access(&mut self, entriy_id: u32) {
        match self.node_store.get_mut(&entriy_id) {
            None => {
                let mut new_node = LRUKNode::new(self.k, entriy_id);
                new_node.set_evictable(false);

                new_node.push_timestamp(
                    self.current_timestamp
                        .fetch_add(1.try_into().unwrap(), std::sync::atomic::Ordering::SeqCst),
                );

                self.node_store.insert(entriy_id, new_node);
                self.current_size
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }

            Some(node) => {
                node.push_timestamp(
                    self.current_timestamp
                        .fetch_add(1.try_into().unwrap(), std::sync::atomic::Ordering::SeqCst),
                );
            }
        }

        self.current_size
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    fn evict(&mut self) -> Option<u32> {
        let store = &self.node_store;

        // filter out evictable nodes
        let evictable_nodes: Vec<(&u32, &LRUKNode)> =
            store.iter().filter(|node| node.1.is_evictable).collect();

        // param entry_id and k-backwards distance and last time_stamp
        // None being an alias for +inf
        let mut node_data: Vec<(u32, Option<usize>, usize)> =
            Vec::with_capacity(evictable_nodes.len());

        let cur_time = &self
            .current_timestamp
            .fetch_add(0, std::sync::atomic::Ordering::Relaxed);

        for node in evictable_nodes {
            let node_id = node.0;
            let k_distance: Option<usize> = match node.1.get_kth_entry() {
                Some(entry) => Some(cur_time - entry),
                None => None,
            };

            let last_time_stamp = node.1.get_last_entry();

            node_data.push((node_id.clone(), k_distance, last_time_stamp.clone()));
        }

        // Case to handle multiple +inf entries. ie if all k-distances are None
        if node_data.iter().all(|p| p.1.is_none()) {
            // the id of the entry with the oldest timestamp

            let id = node_data.iter().min_by_key(|a| a.2).unwrap().0;
            return Some(id);
        }

        // find the id of the entry with the maximum k-distance
        let entry_id = node_data.iter().max_by_key(|node| node.1).unwrap().0;

        Some(entry_id)
    }

    fn remove(&mut self, entriy_id: u32) -> bool {
        let node = self.node_store.get_mut(&entriy_id).unwrap();

        if node.is_evictable {
            node.history.clear();
            self.node_store.remove(&entriy_id).unwrap();

            self.evictable_size
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);

            self.current_size
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            return true;
        }

        false
    }

    fn set_evictable(&mut self, entriy_id: u32, evictability: bool) {
        let node = self
            .node_store
            .get_mut(&entriy_id)
            .expect("Node does not exist");
        node.set_evictable(evictability);

        match evictability {
            true => {
                self.evictable_size
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            false => {
                self.evictable_size
                    .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            }
        }
    }

    fn size(&self) -> usize {
        self.replacer_size
    }
}
