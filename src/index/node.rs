#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use crate::storage::page::b_plus_tree_page::BTreePage;
use crate::storage::page::btree_page_layout::{
    FromByte, INTERNAL_NODE_HEADER_SIZE, INTERNAL_NODE_NUM_CHILDREN_OFFSET, IS_ROOT_OFFSET,
    KEY_SIZE, LEAF_NODE_HEADER_SIZE, LEAF_NODE_NUM_PAIRS_OFFSET, NODE_TYPE_OFFSET, POINTER_OFFSET,
    POINTER_SIZE, PTR_SIZE, ROW_ID_SEGMENT_SIZE, VALUE_SIZE,
};

use super::errors::Error;
use super::node_type::{Key, KeyValuePair, NextPointer, NodeType, PageId, RowID};

/// NodeKeys are either guideposts in an internal node, or the actual kv pair in the leaf
pub enum NodeKey {
    GuidePostKey(Key),
    KeyValuePair(KeyValuePair),
}
/// Node represents a node in the BTree occupied by a single page in memory.
#[derive(Clone, Debug)]
pub struct Node {
    pub node_type: NodeType,
    pub is_root: bool,
    pub pointer: Option<PageId>,
}

pub enum Policy {
    Merge,
    Borrow,
}

impl Node {
    pub fn new(node_type: NodeType, page_pointer: PageId, is_root: bool) -> Node {
        Node {
            node_type,
            pointer: Some(page_pointer),
            is_root,
        }
    }

    /// gets the page pointer for the current Node
    pub fn get_pointer(&self) -> Result<PageId, Error> {
        let page = BTreePage::try_from(&self.clone())?;

        let ptr: [u8; POINTER_SIZE] = page
            .get_ptr_from_offset(POINTER_OFFSET, POINTER_SIZE)
            .try_into()
            .unwrap();

        Ok(PageId(u64::from_le_bytes(ptr).try_into().unwrap()))
    }
    /// returns the pointer to the sibling node
    pub fn get_next_pointer(&self) -> Result<[u8; 8], Error> {
        let node_type = self.node_type.clone();

        match node_type {
            NodeType::Leaf(_, next, _) => match next.0 {
                Some(raw) => {
                    let mut dst = [0u8; 8];
                    dst.copy_from_slice(&raw);

                    Ok(dst)
                }
                None => Err(Error::UnexpectedError),
            },
            _ => Err(Error::UnexpectedError),
        }
    }

    /// sets the pointer to the sibling node
    pub fn set_next_pointer(&self, next_page_pointer: [u8; 8]) -> Result<(), Error> {
        let mut node_type = self.node_type.clone();

        match node_type {
            NodeType::Leaf(_, ref mut next, _) => {
                *next = NextPointer(Some(next_page_pointer));

                // update_next_pointer(&mut node_type, next_page_pointer).unwrap();
                Ok(())
            }
            _ => Err(Error::UnexpectedError),
        }
    }

    pub fn set_page_pointer(&mut self, new_page_pointer: PageId) -> Result<(), Error> {
        self.pointer = Some(new_page_pointer);

        match &mut self.node_type {
            // Use &mut self.node_type to mutate the value directly
            NodeType::Internal(_, _, ref mut _pointer) => {
                *_pointer = new_page_pointer; // Dereference the pointer to mutate it
                Ok(())
            }

            NodeType::Leaf(_, _, ref mut _pointer) => {
                *_pointer = new_page_pointer; // Dereference the pointer to mutate it
                Ok(())
            }

            _ => Err(Error::UnexpectedError),
        }
    }

    // Gets the length of the entry array in internal nodes and key-value arrays in the leaves
    pub fn get_key_array_length(&self) -> usize {
        match &self.node_type {
            NodeType::Internal(_, keys, _) => keys.len(),
            NodeType::Leaf(entries, _, _) => entries.len(),

            NodeType::Unexpected => 0,
        }
    }

    pub fn get_number_of_children(&self) -> Result<usize, Error> {
        match &self.node_type {
            NodeType::Internal(children, _, _) => {
                return Ok(children.len());
            }
            _ => {
                return Err(Error::UnexpectedError);
            }
        }
    }

    /// split creates a sibling node from a given node by splitting the node in two around a median.
    /// split will split the child at b leaving the [0, b-1] keys
    /// while moving the set of [b, 2b-1] keys to the sibling.

    pub fn split(&mut self, b: usize) -> Result<(Key, Node), Error> {
        match self.node_type {
            NodeType::Internal(ref mut children, ref mut keys, _) => {
                // Populate siblings keys.
                let mut sibling_keys = keys.split_off(b);

                // Pop median key - to be added to the parent..
                let median_key = sibling_keys.remove(0);

                // Populate siblings children.

                let sibling_children = children.split_off(b + 1);

                self.is_root = false;
                Ok((
                    median_key,
                    Node::new(
                        NodeType::Internal(sibling_children, sibling_keys, PageId(u64::default())),
                        PageId(u64::default()),
                        false,
                    ),
                ))
            }

            NodeType::Leaf(ref mut pairs, _, current_page) => {
                // Populate siblings pairs
                let sibling_pair = pairs.split_off(b);

                // copy median key.
                let median_pair = sibling_pair.get(0).ok_or(Error::UnexpectedError)?.clone();
                self.is_root = false;

                Ok((
                    Key(median_pair.key),
                    Node::new(
                        NodeType::Leaf(sibling_pair, NextPointer(None), PageId(u64::default())),
                        PageId(u64::default()),
                        false,
                    ),
                ))
            }

            NodeType::Unexpected => {
                println!("Unexpected error in split");
                Err(Error::UnexpectedError)
            }
        }
    }

    /// Inserts a key and pointer into an internal node
    pub fn insert_sibling_node(&mut self, key: Key, child: PageId) -> Result<(), Error> {
        match self.node_type {
            NodeType::Internal(_, _, _) => {
                self.insert_key(key)?;
                self.insert_child_pointer(child)?;

                Ok(())
            }
            _ => Err(Error::UnexpectedError),
        }
    }

    /// Inserts a key into an internal node
    pub fn insert_key(&mut self, key: Key) -> Result<(), Error> {
        match self.node_type {
            NodeType::Internal(_, ref mut keys, _) => {
                let idx = keys.binary_search(&key).unwrap_or_else(|x| x);

                keys.insert(idx, key);
                Ok(())
            }

            _ => Err(Error::UnexpectedError),
        }
    }

    /// Inserts a child pointer into an internal node
    pub fn insert_child_pointer(&mut self, child: PageId) -> Result<(), Error> {
        match self.node_type {
            NodeType::Internal(ref mut children, _, _) => {
                let idx = children.binary_search(&child).unwrap_or_else(|x| x);

                children.insert(idx, child);
                Ok(())
            }

            _ => Err(Error::UnexpectedError),
        }
    }

    /// Gets a key at a certain index within an internal node
    pub fn get_key_at_index(&self, idx: usize) -> Result<Key, Error> {
        match &self.node_type {
            NodeType::Internal(_, keys, _) => {
                let key = keys.get(idx).unwrap().clone();
                Ok(key)
            }
            _ => Err(Error::UnexpectedError),
        }
    }

    // finds the key in an internal node
    pub fn find_key(&self, key: Key) -> Result<Key, Error> {
        match &self.node_type {
            NodeType::Internal(_, keys, _) => {
                let idx = keys.binary_search(&key).unwrap().clone();

                Ok(keys.get(idx).unwrap().clone())
            }
            _ => Err(Error::UnexpectedError),
        }
    }

    /// Gets a key-value pair at a certain index within a Leaf node
    pub fn get_key_value_at_index(&self, idx: usize) -> Result<KeyValuePair, Error> {
        match &self.node_type {
            NodeType::Leaf(entries, _, _) => {
                let entry = entries.get(idx).unwrap().clone();
                Ok(entry)
            }
            _ => Err(Error::UnexpectedError),
        }
    }

    /// finds a key-value pair within a Leaf node
    pub fn find_key_value(&self, key: &Key) -> Result<KeyValuePair, Error> {
        match &self.node_type {
            NodeType::Leaf(entries, _, _) => {
                println!("Checkpoint 1");
                let was_found = entries.binary_search_by_key(key, |p| Key(p.key));

                println!("Checkpoint 2");
                let idx = match was_found {
                    Ok(idx) => {
                        println!("\n\n\nWas found {}", was_found.unwrap());
                        idx
                    }
                    Err(_) => return Err(Error::KeyNotFound),
                };

                println!("Checkpoint 3");

                match entries.get(idx) {
                    None => {
                        return {
                            println!("Printing error");
                            Err(Error::KeyNotFound)
                        }
                    }
                    Some(entry) => {
                        println!("Checkpoint 4 Entry {:?}", entry);
                        return Ok(entry.clone());
                    }
                }
            }
            _ => Err(Error::UnexpectedError),
        }
    }

    /// Inserts a key value pair entry into a leaf node
    pub fn insert_entry(&mut self, entry: KeyValuePair) -> Result<(), Error> {
        match self.node_type {
            NodeType::Leaf(ref mut entries, _, _) => {
                let key = entry.key;
                let idx = entries
                    .binary_search_by_key(&key, |p| p.key)
                    .unwrap_or_else(|x| x);

                if Some(&entry) == entries.get(idx) {
                    return Err(Error::KeyAlreadyExists);
                }

                entries.insert(idx, entry);

                Ok(())
            }

            _ => Err(Error::UnexpectedError),
        }
    }

    /// Removes a key value pair entry from a leaf node
    pub fn remove_entry(&mut self, key: &Key) -> Result<(), Error> {
        match self.node_type {
            NodeType::Leaf(ref mut entries, _, _) => {
                let idx = entries
                    .binary_search_by_key(key, |p| Key(p.key))
                    .unwrap_or_else(|x| x);

                entries.remove(idx);

                Ok(())
            }

            _ => Err(Error::UnexpectedError),
        }
    }

    /// Removes a key from an internal node
    pub fn remove_key(&mut self, key: Key) -> Result<(), Error> {
        match self.node_type {
            NodeType::Internal(ref mut children, ref mut keys, _) => {
                let idx = keys.binary_search(&key).unwrap_or_else(|x| x);

                keys.remove(idx);

                Ok(())
            }

            _ => Err(Error::UnexpectedError),
        }
    }

    /// Removes a child pointer from an internal node
    pub fn remove_child_pointer(&mut self, child: PageId) -> Result<(), Error> {
        match self.node_type {
            NodeType::Internal(ref mut children, _, _) => {
                let idx = children.binary_search(&child).unwrap_or_else(|x| x);

                children.remove(idx);
                Ok(())
            }

            _ => Err(Error::UnexpectedError),
        }
    }

    /// Removes a key and child pointer from an internal node
    pub fn remove_sibling_node(&mut self, key: Key, child: PageId) -> Result<(), Error> {
        match self.node_type {
            NodeType::Internal(_, _, _) => {
                self.remove_key(key)?;
                self.remove_child_pointer(child)?;

                Ok(())
            }
            _ => Err(Error::UnexpectedError),
        }
    }

    /// Removes key and child pointers from internal nodes
    ///
    /// The take right bool parameter controls wether to take the child pointer to the keys left or right
    pub fn remove_key_at_index(
        &mut self,
        idx: usize,
        take_right: bool,
    ) -> Result<(Key, PageId), Error> {
        match self.node_type {
            NodeType::Internal(ref mut children, ref mut keys, _) => {
                let key = keys.remove(idx);
                let child_pointer: PageId;
                if take_right {
                    child_pointer = children.remove(idx + 1);
                } else {
                    child_pointer = children.remove(idx);
                }

                Ok((key, child_pointer))
            }

            _ => Err(Error::UnexpectedError),
        }
    }

    /// Removes key-value pairs from leaf nodes
    pub fn remove_key_value_at_index(&mut self, idx: usize) -> Result<KeyValuePair, Error> {
        match self.node_type {
            NodeType::Leaf(ref mut entries, _, _) => {
                let entry = entries.remove(idx);

                Ok(entry)
            }

            _ => Err(Error::UnexpectedError),
        }
    }

    pub fn pop_front(&mut self) -> Result<(NodeKey, NodeKey, Option<PageId>), Error> {
        match &self.node_type {
            NodeType::Internal(_, keys, _) => {
                let len = keys.len();
                let promotion_key = keys.get(len - 1).unwrap().clone();
                let (key, page_id) = self.remove_key_at_index(0, false)?;

                return Ok((
                    NodeKey::GuidePostKey(key),
                    NodeKey::GuidePostKey(promotion_key),
                    Some(page_id),
                ));
            }
            NodeType::Leaf(entries, _, _) => {
                let len = entries.len();
                let promotion_key = entries.get(len - 1).unwrap().clone();
                let entry = self.remove_key_value_at_index(0)?;

                return Ok((
                    NodeKey::KeyValuePair(entry),
                    NodeKey::KeyValuePair(promotion_key),
                    None,
                ));
            }
            NodeType::Unexpected => {
                return Err(Error::UnexpectedError);
            }
        }
    }

    pub fn pop_back(&mut self) -> Result<(NodeKey, NodeKey, Option<PageId>), Error> {
        match self.node_type {
            NodeType::Internal(_, ref mut keys, _) => {
                println!("\n\nKeys in pop back{:?}", keys);
                let len = keys.len();
                let promotion_key = keys.get(0).unwrap().clone();
                let (key, page_id) = self.remove_key_at_index(len - 1, true)?;
                return Ok((
                    NodeKey::GuidePostKey(key),
                    NodeKey::GuidePostKey(promotion_key),
                    Some(page_id),
                ));
            }
            NodeType::Leaf(ref mut entries, _, _) => {
                println!("\n\nEntries in pop back{:?}", entries);
                let len = entries.len();
                let promotion_key = entries.get(0).unwrap().clone();
                let entry = self.remove_key_value_at_index(len)?;

                return Ok((
                    NodeKey::KeyValuePair(entry),
                    NodeKey::KeyValuePair(promotion_key),
                    None,
                ));
            }
            NodeType::Unexpected => {
                return Err(Error::UnexpectedError);
            }
        }
    }

    pub fn find_and_update_key(&mut self, search_key: Key, update_key: Key) -> Result<(), Error> {
        match self.node_type {
            NodeType::Internal(_, ref mut keys, _) => {
                let idx = keys.binary_search(&search_key).unwrap_or_else(|x| x);
                keys[idx] = update_key;

                return Ok(());
            }
            _ => {
                return Err(Error::UnexpectedError);
            }
        }
    }

    pub fn borrow(
        &mut self,
        current_parent_node: &mut Node,
        current_candidate: &mut Node,
        is_left: bool,
        separator_key: Key,
    ) -> Result<(), Error> {
        match self.node_type {
            NodeType::Leaf(_, _, _) => {
                let promotion_key: Key;

                if is_left {
                    let popped = current_candidate.pop_back()?;

                    // Remove the right most value
                    match popped {
                        (
                            NodeKey::KeyValuePair(right_most_entry),
                            NodeKey::KeyValuePair(promotion),
                            _,
                        ) => {
                            // Set promotion key
                            promotion_key = Key(promotion.key.clone());
                            self.insert_entry(right_most_entry)?;
                        }
                        _ => {
                            // This should never happen if the tree is consistent
                            return Err(Error::UnexpectedError);
                        }
                    }
                } else {
                    let popped = current_candidate.pop_front()?;

                    // Remove the left most value
                    match popped {
                        (
                            NodeKey::KeyValuePair(left_most_entry),
                            NodeKey::KeyValuePair(promotion),
                            _,
                        ) => {
                            // Set promotion key
                            promotion_key = Key(promotion.key.clone());
                            self.insert_entry(left_most_entry)?;
                        }
                        _ => {
                            // This should never happen if the tree is consistent
                            return Err(Error::UnexpectedError);
                        }
                    }
                }

                // Update the separator key
                current_parent_node.find_and_update_key(separator_key, promotion_key)?;

                return Ok(());
            }
            NodeType::Internal(_, _, _) => {
                let promotion_key: Key;
                if is_left {
                    let popped = current_candidate.pop_back()?;

                    // Remove the right most value
                    match popped {
                        (
                            NodeKey::GuidePostKey(right_most_key),
                            NodeKey::GuidePostKey(_),
                            Some(child_pointer),
                        ) => {
                            // Set promotion key
                            promotion_key = right_most_key;
                            self.insert_child_pointer(child_pointer)?;
                            self.insert_key(separator_key.clone())?;
                        }
                        _ => {
                            // This should never happen if the tree is consistent
                            return Err(Error::UnexpectedError);
                        }
                    }
                } else {
                    let popped = current_candidate.pop_front()?;

                    // Remove the left most value
                    match popped {
                        (
                            NodeKey::GuidePostKey(left_most_key),
                            NodeKey::GuidePostKey(_),
                            Some(child_pointer),
                        ) => {
                            // Set promotion key
                            promotion_key = left_most_key;
                            self.insert_child_pointer(child_pointer)?;
                            self.insert_key(separator_key.clone())?;
                        }
                        _ => {
                            // This should never happen if the tree is consistent
                            return Err(Error::UnexpectedError);
                        }
                    }
                }

                // Update the separator key
                current_parent_node.find_and_update_key(separator_key, promotion_key)?;

                // Write updates to disk
                return Ok(());
            }
            _ => return Err(Error::UnexpectedError),
        }
    }
    /// merges the sibling node into self, it assumes the following:
    /// 1. The two nodes are of the same parent.
    /// 2. The two nodes do not accumulate to an overflow.
    ///
    ///
    pub fn merge_internal(&mut self, sibling: &Node) -> Result<(), Error> {
        match self.node_type {
            NodeType::Internal(ref mut children, ref mut keys, _) => {
                if let NodeType::Internal(sibling_pointers, sibling_keys, _) = &sibling.node_type {
                    let mut merged_pointers: Vec<PageId> = children
                        .iter()
                        .chain(sibling_pointers.iter())
                        .cloned()
                        .collect();

                    let mut merged_keys: Vec<Key> =
                        keys.iter().chain(sibling_keys.iter()).cloned().collect();

                    // Is not viable for large sets
                    merged_pointers.sort();
                    merged_keys.sort();

                    *children = merged_pointers;
                    *keys = merged_keys;

                    Ok(())
                } else {
                    Err(Error::UnexpectedError)
                }
            }

            _ => Err(Error::UnexpectedError),
        }
    }

    /// merges two leaf nodes, it assumes the following:
    /// 1. The two nodes are of the same parent.
    /// 2. The two nodes do not accumulate to an overflow.
    ///
    /// Returns the a new node
    ///
    pub fn merge_leaf(&mut self, sibling: &Node) -> Result<(), Error> {
        match self.node_type {
            NodeType::Leaf(ref mut entries, ref mut current_next, _) => {
                if let NodeType::Leaf(sibling_entries, sibling_next, _) = &sibling.node_type {
                    let mut merged_entries: Vec<KeyValuePair> = entries
                        .iter()
                        .chain(sibling_entries.iter())
                        .cloned()
                        .collect();

                    // Is not viable for large sets
                    merged_entries.sort();

                    *entries = merged_entries;
                    *current_next = sibling_next.clone();

                    Ok(())
                } else {
                    Err(Error::UnexpectedError)
                }
            }
            _ => Err(Error::UnexpectedError),
        }
    }
}

impl TryFrom<BTreePage> for Node {
    type Error = Error;

    fn try_from(page: BTreePage) -> Result<Node, Self::Error> {
        let raw = page.get_data();
        let node_type = NodeType::from(raw[NODE_TYPE_OFFSET]);

        let is_root = raw[IS_ROOT_OFFSET].from_byte();
        let pointer: Option<PageId>;

        pointer = Some(PageId(u64::from_le_bytes(
            page.get_ptr_from_offset(POINTER_OFFSET, POINTER_SIZE)
                .try_into()
                .unwrap(),
        )));

        match node_type {
            NodeType::Internal(mut children, mut keys, _) => {
                let num_children = page.get_value_from_offset(INTERNAL_NODE_NUM_CHILDREN_OFFSET)?;

                let mut offset = INTERNAL_NODE_HEADER_SIZE;

                for _ in 1..=num_children {
                    let child_offset = page.get_value_from_offset(offset)?;
                    children.push(PageId(child_offset));

                    offset += PTR_SIZE;
                }

                // Number of keys is always one less than the number of children (i.e. branching factor)
                for _ in 1..num_children {
                    let key_raw = page.get_ptr_from_offset(offset, KEY_SIZE);
                    offset += KEY_SIZE;

                    keys.push(Key(key_raw.try_into().unwrap()));
                }

                Ok(Node::new(
                    NodeType::Internal(children, keys, pointer.unwrap()),
                    pointer.unwrap(),
                    is_root,
                ))
            }

            NodeType::Leaf(mut pairs, _, _) => {
                let mut offset = LEAF_NODE_NUM_PAIRS_OFFSET;
                let num_keys_val_pairs = page.get_value_from_offset(offset)?;
                offset = LEAF_NODE_HEADER_SIZE;
                let next_pointer: [u8; 8] = page.get_next_leaf_pointer().try_into().unwrap();

                for _i in 0..num_keys_val_pairs {
                    let key_raw = page.get_ptr_from_offset(offset, KEY_SIZE);

                    offset += KEY_SIZE;

                    let value_raw = page.get_ptr_from_offset(offset, VALUE_SIZE);

                    let value: [u8; 10] = value_raw.try_into().unwrap();

                    // default values
                    // modify page layout scheme
                    let mut page_id: [u8; 4] = [0u8; 4];
                    let mut slot_index: [u8; 4] = [0u8; 4];

                    page_id.copy_from_slice(&value[0..ROW_ID_SEGMENT_SIZE]);

                    slot_index
                        .copy_from_slice(&value[ROW_ID_SEGMENT_SIZE..ROW_ID_SEGMENT_SIZE + 4]);

                    let row_id =
                        RowID::new(u32::from_le_bytes(page_id), u32::from_le_bytes(slot_index));
                    offset += VALUE_SIZE;

                    // Trim leading or trailing zeros.
                    let key = u64::from_le_bytes(key_raw[0..PTR_SIZE].try_into().unwrap());
                    pairs.push(KeyValuePair::new(key, row_id))
                }

                Ok(Node::new(
                    NodeType::Leaf(pairs, NextPointer(Some(next_pointer)), pointer.unwrap()),
                    pointer.unwrap(),
                    is_root,
                ))
            }

            NodeType::Unexpected => {
                println!("Unexpected error in try from");
                Err(Error::UnexpectedError)
            }
        }
    }
}

#[cfg(test)]

mod tests {
    use crate::{
        index::{
            errors::Error,
            node::Node,
            node_type::{Key, KeyValuePair, NextPointer, NodeType, PageId, RowID},
        },
        storage::page::{
            b_plus_tree_page::BTreePage,
            btree_page_layout::{
                INTERNAL_NODE_HEADER_SIZE, KEY_SIZE, LEAF_NODE_HEADER_SIZE, PAGE_SIZE, PTR_SIZE,
                VALUE_SIZE,
            },
        },
    };

    #[test]
    fn page_to_node_works_for_leaf_node() -> Result<(), Error> {
        const DATA_LEN: usize = LEAF_NODE_HEADER_SIZE + KEY_SIZE + VALUE_SIZE;
        let page_data: [u8; DATA_LEN] = [
            0x01, // Is-Root byte.
            0x02, // Leaf Node type byte.
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // id.
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // Number of Key-Value pairs.
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // Next leaf pointer size
            0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x00, 0x00, 0x00, 0x00, 0x00, // "hello"
            0x77, 0x6f, 0x72, 0x6c, 0x64, 0x00, 0x00, 0x00, 0x00, 0x00, // "world"
        ];

        let junk: [u8; PAGE_SIZE - DATA_LEN] = [0x00; PAGE_SIZE - DATA_LEN];
        let mut page = [0x00; PAGE_SIZE];
        for (to, from) in page.iter_mut().zip(page_data.iter().chain(junk.iter())) {
            *to = *from
        }

        let btree = BTreePage::new(page);

        let node = Node::try_from(btree).unwrap();

        let next_pointer = node.get_next_pointer().unwrap();
        let cur_page_pointer = node.get_pointer()?;

        assert_eq!(cur_page_pointer, PageId(0));
        assert_eq!(next_pointer, [0, 0, 0, 0, 0, 0, 0, 1]);
        assert_eq!(node.is_root, true);

        Ok(())
    }

    #[test]
    fn page_to_node_works_for_internal_node() -> Result<(), Error> {
        const DATA_LEN: usize = INTERNAL_NODE_HEADER_SIZE + 3 * PTR_SIZE + 2 * KEY_SIZE;
        let page_data: [u8; DATA_LEN] = [
            0x01, // Is-Root byte.
            0x01, // Internal Node type byte.
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // id.
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, // Number of children.
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, // 4096  (2nd Page id )
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20, 0x00, // 8192  (3rd Page id )
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x30, 0x00, // 12288 (4th Page id)
            0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x00, 0x00, 0x00, 0x00, 0x00, // "hello"
            0x77, 0x6f, 0x72, 0x6c, 0x64, 0x00, 0x00, 0x00, 0x00, 0x00, // "world"
        ];
        let junk: [u8; PAGE_SIZE - DATA_LEN] = [0x00; PAGE_SIZE - DATA_LEN];

        // Concatenate the two arrays; page_data and junk.
        let mut page = [0x00; PAGE_SIZE];
        for (to, from) in page.iter_mut().zip(page_data.iter().chain(junk.iter())) {
            *to = *from
        }

        let node = Node::try_from(BTreePage::new(page))?;

        if let NodeType::Internal(_, keys, current_page) = node.node_type {
            assert_eq!(keys.len(), 2);

            let Key(first_key) = match keys.get(0) {
                Some(key) => key,
                None => return Err(Error::UnexpectedError),
            };

            let key_string = String::from_utf8_lossy(first_key).into_owned();
            let value = key_string.trim_matches('\0');

            assert_eq!(value, "hello");

            let Key(second_key) = match keys.get(1) {
                Some(key) => key,
                None => return Err(Error::UnexpectedError),
            };

            let key_string = String::from_utf8_lossy(second_key).into_owned();
            let value = key_string.trim_matches('\0');

            assert_eq!(value, "world");

            assert_eq!(current_page, PageId(0));

            return Ok(());
        }

        Err(Error::UnexpectedError)
    }

    #[test]
    fn split_leaf_works() -> Result<(), Error> {
        let kv_pair_one = KeyValuePair::new(42, RowID::new(0, 0));
        let kv_pair_two = KeyValuePair::new(3, RowID::new(0, 1));
        let kv_pair_three = KeyValuePair::new(8, RowID::new(0, 2));

        let mut node = Node::new(
            NodeType::Leaf(
                vec![kv_pair_one, kv_pair_two, kv_pair_three],
                NextPointer(Some([0, 0, 0, 0, 0, 0, 0, 1])),
                PageId(1_u32.try_into().unwrap()),
            ),
            PageId(1_u32.try_into().unwrap()),
            true,
        );

        let (median, sibling) = node.split(2)?;

        assert_eq!(median, Key([3, 0, 0, 0, 0, 0, 0, 0, 0, 0]));

        assert_eq!(
            node.node_type,
            NodeType::Leaf(
                vec![
                    KeyValuePair::new(42, RowID::new(0, 0)),
                    KeyValuePair::new(3, RowID::new(0, 1))
                ],
                NextPointer(Some([0, 0, 0, 0, 0, 0, 0, 1])),
                PageId(1_u32.try_into().unwrap())
            )
        );

        assert_eq!(
            sibling.node_type,
            NodeType::Leaf(
                vec![KeyValuePair::new(8, RowID::new(0, 2))],
                NextPointer(None),
                PageId(u64::default())
            )
        );

        Ok(())
    }

    #[test]

    fn split_internal_works() -> Result<(), Error> {
        fn serialize_int_to_10_bytes(n: u64) -> [u8; 10] {
            let mut bytes = [0u8; 10]; // Create a 10-byte array initialized with zeros.
            let n_bytes = n.to_le_bytes(); // Convert the integer to bytes (little-endian).

            // Copy the integer bytes into the fixed-size array (first 8 bytes for u64).
            bytes[..n_bytes.len()].copy_from_slice(&n_bytes);

            bytes
        }

        let key = serialize_int_to_10_bytes(42);
        let key_two = serialize_int_to_10_bytes(3);
        let key_three = serialize_int_to_10_bytes(8);

        // Assuming d = 2 and max capacity nodes
        let mut node = Node::new(
            NodeType::Internal(
                vec![PageId(1), PageId(2), PageId(3), PageId(4)],
                vec![Key(key), Key(key_two), Key(key_three)],
                PageId(1_u32.try_into().unwrap()),
            ),
            PageId(1_u32.try_into().unwrap()),
            true,
        );

        let (median, mut sibling) = node.split(2)?;

        assert_eq!(median, Key(key_two));
        println!("median {:?}", median);

        assert_eq!(
            node.node_type,
            NodeType::Internal(
                vec![PageId(1), PageId(2)],
                vec![Key(key)],
                PageId(1_u32.try_into().unwrap())
            ),
        );

        sibling.set_page_pointer(PageId(2_u32.try_into().unwrap()))?;
        println!("{:?}", sibling);

        assert_eq!(sibling.pointer, Some(PageId(2_u32.try_into().unwrap())));

        assert_eq!(
            sibling.node_type,
            NodeType::Internal(
                vec![PageId(3), PageId(4)],
                vec![Key(key_three)],
                PageId(2_u32.try_into().unwrap())
            )
        );

        println!("\n\n\n {:?}", sibling);

        sibling.remove_key(Key(key_three))?;

        println!("\n\n\n {:?}", sibling);

        Ok(())
    }
}
