use std::sync::atomic::AtomicUsize;
use std::hash::Hash;
use hashlink::LinkedHashMap;

const POS_INFINITY: Option<usize> = None;

pub trait Replacer<ID: Eq + Hash + Copy> {
    fn evict(&mut self) -> Option<ID>;
    fn record_access(&mut self, entry_id: ID) -> Option<i8>;
    fn set_evictable(&mut self, entry_id: ID, evictability: bool);
    fn remove(&mut self, entry_id: ID) -> bool;
    fn size(&self) -> usize;
}

#[derive(Debug)]
struct LRUKNode {
    history: Vec<usize>,
    is_evictable: bool,
    k: usize,
}

impl LRUKNode {
    pub fn new(k: usize) -> Self {
        LRUKNode {
            history: Vec::new(),
            is_evictable: false,
            k,
        }
    }

    fn set_evictable(&mut self, evictability: bool) {
        self.is_evictable = evictability;
    }

    fn push_timestamp(&mut self, timestamp: usize) {
        self.history.insert(0, timestamp);
    }

    fn get_kth_entry(&self) -> Option<&usize> {
        self.history.get(self.k - 1)
    }

    fn get_last_entry(&self) -> &usize {
        self.history.last().expect("Empty History")
    }
}

pub struct LRUKReplacer<ID: Eq + Hash + Copy> {
    node_store: LinkedHashMap<ID, LRUKNode>,
    current_timestamp: AtomicUsize,
    current_size: AtomicUsize,
    replacer_size: usize,
    evictable_size: AtomicUsize,
    k: usize,
}

impl<ID: Eq + Hash + Copy> LRUKReplacer<ID> {
    pub fn new(number_of_entries: usize, k: usize) -> Self {
        LRUKReplacer {
            node_store: LinkedHashMap::new(),
            current_timestamp: AtomicUsize::new(0),
            current_size: AtomicUsize::new(0),
            replacer_size: number_of_entries,
            evictable_size: AtomicUsize::new(0),
            k,
        }
    }
}

impl<ID: Eq + Hash + Copy> Replacer<ID> for LRUKReplacer<ID> {
    fn record_access(&mut self, entry_id: ID) -> Option<i8> {

        match self.node_store.get_mut(&entry_id) {
            None => {
                if self.node_store.len() == self.replacer_size {
                    return None;
                }
                let mut new_node = LRUKNode::new(self.k);
                new_node.set_evictable(false);
                new_node.push_timestamp(self.current_timestamp.fetch_add(1, std::sync::atomic::Ordering::SeqCst));
                self.node_store.insert(entry_id, new_node);
                self.current_size.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            Some(node) => {
                node.push_timestamp(self.current_timestamp.fetch_add(1, std::sync::atomic::Ordering::SeqCst));
            }
        }
        Some(1)
    }

    fn evict(&mut self) -> Option<ID> {
        if self.size() == 0 {
            return None;
        }

        let cur_time = self.current_timestamp.load(std::sync::atomic::Ordering::Relaxed);

        let node_data: Vec<(ID, Option<usize>, usize)> = self.node_store.iter()
            .filter(|(_, node)| node.is_evictable)
            .map(|(id, node)| {
                let k_distance = node.get_kth_entry().map(|&entry| cur_time - entry).or(POS_INFINITY);
                (*id, k_distance, *node.get_last_entry())
            })
            .collect();

        fn find_max_or_oldest<ID: Copy>(node_data: &[(ID, Option<usize>, usize)]) -> Option<ID> {
            if node_data.is_empty() {
                return None;
            }

            let mut max_k_distance_id = None;
            let mut max_k_distance = None;
            let mut first_none_id = None;
            let mut oldest_timestamp_id = node_data[0].0;
            let mut oldest_timestamp = node_data[0].2;

            for &(id, k_distance, timestamp) in node_data {
                if timestamp < oldest_timestamp {
                    oldest_timestamp = timestamp;
                    oldest_timestamp_id = id;
                }

                if k_distance.is_none() && first_none_id.is_none() {
                    first_none_id = Some(id);
                    continue;
                }

                if let Some(curr_distance) = k_distance {
                    if max_k_distance.is_none() || curr_distance > max_k_distance.unwrap() {
                        max_k_distance = Some(curr_distance);
                        max_k_distance_id = Some(id);
                    }
                }
            }

            first_none_id.or(max_k_distance_id).or(Some(oldest_timestamp_id))
        }

        let evicted_node_id = find_max_or_oldest(&node_data)?;
        self.remove(evicted_node_id);
        Some(evicted_node_id)
    }

    fn remove(&mut self, entry_id: ID) -> bool {
        if let Some(node) = self.node_store.get(&entry_id) {
            if node.is_evictable {
                self.node_store.remove(&entry_id);
                self.evictable_size.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                self.current_size.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                return true;
            }
        }
        false
    }

    fn set_evictable(&mut self, entry_id: ID, evictability: bool) {
        if let Some(node) = self.node_store.get_mut(&entry_id) {
            if node.is_evictable != evictability {
                match evictability {
                    true => self.evictable_size.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                    false => self.evictable_size.fetch_sub(1, std::sync::atomic::Ordering::SeqCst),
                };
            }
            node.set_evictable(evictability);
        }
    }

    fn size(&self) -> usize {
        self.evictable_size.load(std::sync::atomic::Ordering::SeqCst)
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

        //  All other entry now share the maximum backward k-distance. Since we use timestamps to break ties, where the first
        //  to be evicted is the frame with the oldest timestamp, the order of eviction should be [2, 3, 4, 5, 1].

        //  Evict three entries from the replacer.
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
