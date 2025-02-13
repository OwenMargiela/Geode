use std::{
    collections::HashMap,
    sync::atomic::{AtomicIsize, AtomicUsize},
};

use hashlink::LinkedHashMap;

pub trait Replacer {
    fn evict(&mut self) -> u32;
    fn record_access(&mut self, entriy_id: u32) -> Option<i8>;
    fn set_evictable(&mut self, entriy_id: u32, evictability: bool);
    fn remove(&mut self, entriy_id: u32) -> bool;
    fn size(&self) -> usize;
}

struct LRUKNode {
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
        let position = self.k - 1;
        if position >= length {
            return None;
        }

        Some(self.history.get(length - position)).unwrap()
    }

    fn get_last_entry(&self) -> &usize {
        self.history.last().expect("Empty History")
    }
}

struct LRUKReplacer {
    node_store: LinkedHashMap<u32, LRUKNode>,
    current_timestamp: AtomicUsize,
    current_size: AtomicUsize,
    replacer_size: usize,
    evictable_size: AtomicUsize,
    k: usize,
}
impl LRUKReplacer {
    fn new(number_of_entries: usize, k: usize) -> Self {
        LRUKReplacer {
            node_store: LinkedHashMap::new(),
            current_timestamp: AtomicUsize::new(0),
            current_size: AtomicUsize::new(0),
            replacer_size: number_of_entries,
            evictable_size: AtomicUsize::new(0),
            k: k,
        }
    }
}

impl Replacer for LRUKReplacer {
    fn record_access(&mut self, entriy_id: u32) -> Option<i8> {
        match self.node_store.get_mut(&entriy_id) {
            None => {
                if self.node_store.len() == self.replacer_size {
                    return None;
                }
                let mut new_node = LRUKNode::new(self.k, entriy_id);
                new_node.set_evictable(false);

                new_node.push_timestamp(
                    self.current_timestamp
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst),
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

        Some(1)
    }

    // Results are needed because list might be empty
    fn evict(&mut self) -> u32 {
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
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        for node in evictable_nodes {
            let node_id = node.0;
            let k_distance: Option<usize> = match node.1.get_kth_entry() {
                Some(entry) => Some(cur_time - entry),
                None => None,
            };

            let last_time_stamp = node.1.get_last_entry();

            node_data.push((node_id.clone(), k_distance, last_time_stamp.clone()));
        }

        println!("{:?}", node_data);

        // Case to handle multiple +inf entries. ie if all k-distances are None
        if node_data.iter().all(|p| p.1.is_none()) {
            // the id of the entry with the oldest timestamp

            let id = node_data.iter().min_by_key(|a| a.2).unwrap().0;
            return id;
        }

        // find the id of the entry with the maximum k-distance or the first instance of a None element representing
        // +inf

        fn find_max_or(node_iter: &mut Vec<(u32, Option<usize>, usize)>) -> Option<usize> {
            let first_entry = match node_iter.iter().next() {
                Some(node) => {
                    if node.1.is_none() {
                        return None; // first entry is +inf
                    } else {
                        node
                    }
                }
                None => return None, // empty list
            };

            // Beyond this point we use the first entry as the largest value we've seen
            // up to this point

    
            let mut max = first_entry.1;
            for entry in node_iter {
                match entry.1 {
                    Some(_) => {
                        if entry.1 > max {
                            max = entry.1;
                        }
                    }
                    None => return None, // All None values will be returned immediately
                }
            }

            max
        }

        // rewrite this
        let entry_id = node_data
            .iter()
            .max_by_key(|node| {
                match node.1 {
                    Some(k_distance) => Some((false, k_distance)),
                    None => Some((true, 0)), // true makes None the maximum
                }
            })
            .unwrap()
            .0;

        entry_id
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

            false => match self.node_store.get_mut(&entriy_id) {
                None => {}
                Some(node) => {
                    if node.is_evictable {
                        self.evictable_size
                            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                    }
                }
            },
        }
    }

    fn size(&self) -> usize {
        self.evictable_size
            .load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[cfg(test)]
pub mod test {
    use super::{LRUKReplacer, Replacer};

    #[test]
    fn repcaer_test() {
        // Initialize the replacer

        let mut replacer = LRUKReplacer::new(7, 2);
        // Add six frames to the replacer. We now have frames [1, 2, 3, 4, 5]. We set frame 6 as non-evictable.

        replacer.record_access(1);
        replacer.record_access(2);
        replacer.record_access(3);
        replacer.record_access(4);
        replacer.record_access(5);
        replacer.record_access(6);

        replacer.set_evictable(1, true);
        replacer.set_evictable(2, true);
        replacer.set_evictable(3, true);
        replacer.set_evictable(4, true);
        replacer.set_evictable(5, true);
        replacer.set_evictable(6, false);

        assert_eq!(5, replacer.size());

        // Record an access for entry 1. Now entrie 1 has two accesses total.
        replacer.record_access(1);
        let node = replacer.node_store.get(&1).unwrap();
        assert_eq!(2, node.history.len());
        println!("Node 1 history: {:?}", node.history);

        //  All other frames now share the maximum backward k-distance. Since we use timestamps to break ties, where the first
        //  to be evicted is the frame with the oldest timestamp, the order of eviction should be [2, 3, 4, 5, 1].

        //  Evict three pages from the replacer.
        //  To break ties, we use LRU with respect to the oldest timestamp, or the least recently used frame.
        assert_eq!(2, replacer.evict());

        // All other entries now share the maximum backward k-distance. Since we use timestamps to break ties, where the first
        // to be evicted is the frame with the oldest timestamp, the order of eviction should be [2, 3, 4, 5, 1].
    }
}

// Note that comparison with `std::nullopt` always results in `false`, and if the optional type actually does contain
// a value, the comparison will compare the inner value.
// See: https://devblogs.microsoft.com/oldnewthing/20211004-00/?p=105754
//   std::optional<frame_id_t> frame;

//   LRUKReplacer lru_replacer(7, 2);

//   lru_replacer.RecordAccess(1);
//   lru_replacer.RecordAccess(2);
//   lru_replacer.RecordAccess(3);
//   lru_replacer.RecordAccess(4);
//   lru_replacer.RecordAccess(5);
//   lru_replacer.RecordAccess(6);
//   lru_replacer.SetEvictable(1, true);
//   lru_replacer.SetEvictable(2, true);
//   lru_replacer.SetEvictable(3, true);
//   lru_replacer.SetEvictable(4, true);
//   lru_replacer.SetEvictable(5, true);
//   lru_replacer.SetEvictable(6, false);

//   // The size of the replacer is the number of frames that can be evicted, _not_ the total number of frames entered.
//   ASSERT_EQ(5, lru_replacer.Size());

//   // Record an access for frame 1. Now frame 1 has two accesses total.
//   lru_replacer.RecordAccess(1);
//   // All other frames now share the maximum backward k-distance. Since we use timestamps to break ties, where the first
//   // to be evicted is the frame with the oldest timestamp, the order of eviction should be [2, 3, 4, 5, 1].

//   // Evict three pages from the replacer.
//   // To break ties, we use LRU with respect to the oldest timestamp, or the least recently used frame.
//   ASSERT_EQ(2, lru_replacer.Evict());
//   ASSERT_EQ(3, lru_replacer.Evict());
//   ASSERT_EQ(4, lru_replacer.Evict());
//   ASSERT_EQ(2, lru_replacer.Size());
//   // Now the replacer has the frames [5, 1].

//   // Insert new frames [3, 4], and update the access history for 5. Now, the ordering is [3, 1, 5, 4].
//   lru_replacer.RecordAccess(3);
//   lru_replacer.RecordAccess(4);
//   lru_replacer.RecordAccess(5);
//   lru_replacer.RecordAccess(4);
//   lru_replacer.SetEvictable(3, true);
//   lru_replacer.SetEvictable(4, true);
//   ASSERT_EQ(4, lru_replacer.Size());

//   // Look for a frame to evict. We expect frame 3 to be evicted next.
//   ASSERT_EQ(3, lru_replacer.Evict());
//   ASSERT_EQ(3, lru_replacer.Size());

//   // Set 6 to be evictable. 6 Should be evicted next since it has the maximum backward k-distance.
//   lru_replacer.SetEvictable(6, true);
//   ASSERT_EQ(4, lru_replacer.Size());
//   ASSERT_EQ(6, lru_replacer.Evict());
//   ASSERT_EQ(3, lru_replacer.Size());

//   // Mark frame 1 as non-evictable. We now have [5, 4].
//   lru_replacer.SetEvictable(1, false);

//   // We expect frame 5 to be evicted next.
//   ASSERT_EQ(2, lru_replacer.Size());
//   ASSERT_EQ(5, lru_replacer.Evict());
//   ASSERT_EQ(1, lru_replacer.Size());

//   // Update the access history for frame 1 and make it evictable. Now we have [4, 1].
//   lru_replacer.RecordAccess(1);
//   lru_replacer.RecordAccess(1);
//   lru_replacer.SetEvictable(1, true);
//   ASSERT_EQ(2, lru_replacer.Size());

//   // Evict the last two frames.
//   ASSERT_EQ(4, lru_replacer.Evict());
//   ASSERT_EQ(1, lru_replacer.Size());
//   ASSERT_EQ(1, lru_replacer.Evict());
//   ASSERT_EQ(0, lru_replacer.Size());

//   // Insert frame 1 again and mark it as non-evictable.
//   lru_replacer.RecordAccess(1);
//   lru_replacer.SetEvictable(1, false);
//   ASSERT_EQ(0, lru_replacer.Size());

//   // A failed eviction should not change the size of the replacer.
//   frame = lru_replacer.Evict();
//   ASSERT_EQ(false, frame.has_value());

//   // Mark frame 1 as evictable again and evict it.
//   lru_replacer.SetEvictable(1, true);
//   ASSERT_EQ(1, lru_replacer.Size());
//   ASSERT_EQ(1, lru_replacer.Evict());
//   ASSERT_EQ(0, lru_replacer.Size());

//   // There is nothing left in the replacer, so make sure this doesn't do something strange.
//   frame = lru_replacer.Evict();
//   ASSERT_EQ(false, frame.has_value());
//   ASSERT_EQ(0, lru_replacer.Size());

//   // Make sure that setting a non-existent frame as evictable or non-evictable doesn't do something strange.
//   lru_replacer.SetEvictable(6, false);
//   lru_replacer.SetEvictable(6, true);
