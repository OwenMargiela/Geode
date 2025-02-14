use std::sync::atomic::AtomicUsize;

use hashlink::LinkedHashMap;

pub trait Replacer {
    fn evict(&mut self) -> Option<u32>;
    fn record_access(&mut self, entriy_id: u32) -> Option<i8>;
    fn set_evictable(&mut self, entriy_id: u32, evictability: bool);
    fn remove(&mut self, entriy_id: u32) -> bool;
    fn size(&self) -> usize;
}

#[derive(Debug)]
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

        Some(self.history.get(position)).unwrap()
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
    fn evict(&mut self) -> Option<u32> {
        if self.size() == 0 {
            println!("Not enough evictable nodes");
            return None;
        }

        let store = &self.node_store;

        // println!("Store {:?}", store);

        // filter out evictable nodes
        let evictable_nodes: Vec<(&u32, &LRUKNode)> =
            store.iter().filter(|node| node.1.is_evictable).collect();

        if evictable_nodes.len() <= 0 {
            println!("empty list");
            return None; // no evictable nodes
        }

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

        fn find_max_or_oldest(node_data: &Vec<(u32, Option<usize>, usize)>) -> Option<u32> {
            if node_data.is_empty() {
                return None;
            }

            let mut max_k_distance_id = None;
            let mut max_k_distance = None;
            let mut first_none_id = None;
            let mut oldest_timestamp_id = node_data[0].0;
            let mut oldest_timestamp = node_data[0].2;

            for &(id, k_distance, timestamp) in node_data {
                // Track oldest timestamp
                if timestamp < oldest_timestamp {
                    oldest_timestamp = timestamp;
                    oldest_timestamp_id = id;
                }

                // Track first None (infinity) encountered
                if k_distance.is_none() && first_none_id.is_none() {
                    first_none_id = Some(id);
                    continue;
                }

                // Track maximum k-distance
                if let Some(curr_distance) = k_distance {
                    match max_k_distance {
                        None => {
                            max_k_distance = Some(curr_distance);
                            max_k_distance_id = Some(id);
                        }
                        Some(max_dist) if curr_distance > max_dist => {
                            max_k_distance = Some(curr_distance);
                            max_k_distance_id = Some(id);
                        }
                        _ => {}
                    }
                }
            }

            // If all values are None, return the oldest timestamp
            if max_k_distance.is_none() && first_none_id.is_some() {
                return Some(oldest_timestamp_id);
            }

            // If we found a None before finding max k-distance, return the None's id
            if let Some(none_id) = first_none_id {
                return Some(none_id);
            }

            // Otherwise return the maximum k-distance id
            max_k_distance_id
        }

        let evicted_node_id = find_max_or_oldest(&node_data);

        self.remove(evicted_node_id.unwrap());

        evicted_node_id
    }

    fn remove(&mut self, entriy_id: u32) -> bool {
        let node = self.node_store.get(&entriy_id).unwrap();

        if node.is_evictable == true {
            self.node_store
                .remove(&entriy_id)
                .expect("Failed to remove entry");

            self.evictable_size
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);

            self.current_size
                .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            return true;
        }

        false
    }

    fn set_evictable(&mut self, entry_id: u32, evictability: bool) {
        match self.node_store.get_mut(&entry_id) {
            Some(node) => {
                // Only update counter if evictability actually changes
                if node.is_evictable != evictability {
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

                node.set_evictable(evictability);
            }
            None => return,
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
    fn replacer_test() {
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

        //  All other entry now share the maximum backward k-distance. Since we use timestamps to break ties, where the first
        //  to be evicted is the frame with the oldest timestamp, the order of eviction should be [2, 3, 4, 5, 1].

        //  Evict three pages from the replacer.
        //  To break ties, we use LRU with respect to the oldest timestamp, or the least recently used frame.

        // All other entries now share the maximum backward k-distance. Since we use timestamps to break ties, where the first
        // to be evicted is the frame with the oldest timestamp, the order of eviction should be [2, 3, 4, 5, 1].
        assert_eq!(2, replacer.evict().unwrap());
        assert_eq!(3, replacer.evict().unwrap());
        assert_eq!(4, replacer.evict().unwrap());
        assert_eq!(2, replacer.size());

        // Now the replacer has the entry [5, 1].
        // Insert new entry [3, 4], and update the access history for 5. Now, the ordering is [3, 1, 5, 4].

        replacer.record_access(3);
        replacer.record_access(4);
        replacer.record_access(5);
        replacer.record_access(4);
        replacer.set_evictable(3, true);
        replacer.set_evictable(4, true);
        assert_eq!(4, replacer.size());

        // Look for a frame to evict. We expect frame 3 to be evicted next.
        assert_eq!(3, replacer.evict().unwrap());
        assert_eq!(3, replacer.size());

        // Mark frame 1 as non-evictable. We now have [5, 4].
        replacer.set_evictable(1, false);

        // We expect frame 5 to be evicted next.
        assert_eq!(2, replacer.size());
        assert_eq!(5, replacer.evict().unwrap());
        assert_eq!(1, replacer.size());

        // Update the access history for frame 1 and make it evictable. Now we have [4, 1].
        replacer.record_access(1);
        replacer.record_access(1);
        replacer.set_evictable(1, true);
        assert_eq!(2, replacer.size());

        // Evict the last two entry.

        // let node_1 = replacer.node_store.get(&4);
        assert_eq!(4, replacer.evict().unwrap());
        assert_eq!(1, replacer.size());

        // let node_2 = replacer.node_store.get(&1);
        assert_eq!(1, replacer.evict().unwrap());
        assert_eq!(0, replacer.size());

        //   Insert frame 1 again and mark it as non-evictable.
        replacer.record_access(1);
        replacer.set_evictable(1, false);
        println!("{:?}", replacer.node_store.get(&1));

        assert_eq!(0, replacer.size());

        // A failed eviction should not change the size of the replacer.
        let entry = replacer.evict();
        assert_eq!(false, entry.is_some());

        assert_eq!(0, replacer.size());

        // Mark frame 1 as evictable again and evict it.
        replacer.set_evictable(1, true);

        assert_eq!(1, replacer.size());
        assert_eq!(1, replacer.evict().unwrap());
        assert_eq!(0, replacer.size());

        // There is nothing left in the replacer, so make sure this doesn't do something strange.
        let entry = replacer.evict();
        assert_eq!(false, entry.is_some());
        assert_eq!(0, replacer.size());

        // Make sure that setting a non-existent frame as evictable or non-evictable doesn't do something strange.
        replacer.set_evictable(6, false);
        replacer.set_evictable(6, true);
    }
}

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

//   // Evict the last two entry.
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
