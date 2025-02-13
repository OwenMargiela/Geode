
mod utils;

// use std::collections::{HashMap, VecDeque};
// use std::hash::Hash;
// use std::time::SystemTime;

// pub trait Cache<K, V> {
//     fn get(&mut self, key: &K) -> Option<&V>;
//     fn insert(&mut self, key: K, value: V);
//     fn evict(&mut self) -> Option<K>;
//     fn size(&self) -> usize;
//     fn pin(&mut self, key: &K);
//     fn unpin(&mut self, key: &K);
// }

// pub trait EvictionPolicy<K> {
//     fn record_access(&mut self, key: &K);
//     fn evict(&mut self, candidates: &HashMap<K, bool>) -> Option<K>;
//     fn set_evictable(&mut self, key: &K, is_evictable: bool);
//     fn remove(&mut self, key: &K);
// }

// pub struct LRUKNode {
//     history: VecDeque<SystemTime>,
//     k: usize,
//     is_evictable: bool,
// }

// impl LRUKNode {
//     pub fn new(k: usize) -> Self {
//         Self {
//             history: VecDeque::with_capacity(k),
//             k,
//             is_evictable: false,
//         }
//     }

//     pub fn record_access(&mut self) {
//         self.history.push_back(SystemTime::now());
//         if self.history.len() > self.k {
//             self.history.pop_front();
//         }
//     }

//     pub fn get_kth_access(&self) -> Option<&SystemTime> {
//         self.history.front()
//     }
// }

// pub struct LRUKReplacer<K> {
//     node_store: HashMap<K, LRUKNode>,
//     k: usize,
// }

// impl<K: Eq + Hash + Clone> LRUKReplacer<K> {
//     pub fn new(k: usize) -> Self {
//         Self {
//             node_store: HashMap::new(),
//             k,
//         }
//     }
// }

// impl<K: Eq + Hash + Clone> EvictionPolicy<K> for LRUKReplacer<K> {
//     fn record_access(&mut self, key: &K) {
//         let node = self.node_store.entry(key.clone()).or_insert_with(|| LRUKNode::new(self.k));
//         node.record_access();   
//         node.is_evictable = true;
//     }

//     fn evict(&mut self, candidates: &HashMap<K, bool>) -> Option<K> {
//         self.node_store.iter()
//             .filter(|(key, node)| node.is_evictable && !candidates.get(key).unwrap_or(&false))
//             .min_by_key(|(_, node)| node.get_kth_access().unwrap_or(&SystemTime::UNIX_EPOCH))
//             .map(|(key, _)| key.clone())
//     }

//     fn set_evictable(&mut self, key: &K, is_evictable: bool) {
//         if let Some(node) = self.node_store.get_mut(key) {
//             node.is_evictable = is_evictable;
//         }
//     }

//     fn remove(&mut self, key: &K) {
//         self.node_store.remove(key);
//     }
// }

// pub struct CacheWithPolicy<K, V, P> {
//     items: HashMap<K, V>,
//     eviction_policy: P,
//     pinned: HashMap<K, bool>,
//     capacity: usize,
// }

// impl<K: Eq + Hash + Clone, V, P: EvictionPolicy<K>> CacheWithPolicy<K, V, P> {
//     pub fn new(capacity: usize, eviction_policy: P) -> Self {
//         Self {
//             items: HashMap::new(),
//             eviction_policy,
//             pinned: HashMap::new(),
//             capacity,
//         }
//     }

//     pub fn pin(&mut self, key: &K) {
//         self.pinned.insert(key.clone(), true);
//     }

//     pub fn unpin(&mut self, key: &K) {
//         self.pinned.insert(key.clone(), false);
//     }
// }

// impl<K: Eq + Hash + Clone, V, P: EvictionPolicy<K>> Cache<K, V> for CacheWithPolicy<K, V, P> {
//     fn get(&mut self, key: &K) -> Option<&V> {
//         if let Some(value) = self.items.get(key) {
//             self.eviction_policy.record_access(key);
//             Some(value)
//         } else {
//             None
//         }
//     }

//     fn insert(&mut self, key: K, value: V) {
//         if self.items.len() >= self.capacity {
//             if let Some(evicted_key) = self.eviction_policy.evict(&self.pinned) {
//                 self.items.remove(&evicted_key);
//                 self.pinned.remove(&evicted_key);
//             }
//         }
//         self.items.insert(key.clone(), value);
//         self.eviction_policy.record_access(&key);
//     }

//     fn evict(&mut self) -> Option<K> {
//         self.eviction_policy.evict(&self.pinned)
//     }

//     fn size(&self) -> usize {
//         self.items.len()
//     }

//     fn pin(&mut self, key: &K) {
        
//     }

//     fn unpin(&mut self, key: &K) {
        
//     }
// }

fn main() {

}
