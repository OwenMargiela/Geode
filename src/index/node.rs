#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use crate::storage::page::b_plus_tree_page::BTreePage;
use crate::storage::page::btree_page_layout::{
    FromByte,
    INTERNAL_NODE_HEADER_SIZE,
    INTERNAL_NODE_NUM_CHILDREN_OFFSET,
    IS_ROOT_OFFSET,
    KEY_SIZE,
    LEAF_NODE_HEADER_SIZE,
    LEAF_NODE_NUM_PAIRS_OFFSET,
    NEXT_LEAF_POINTER_OFFSET,
    NEXT_LEAF_POINTER_SIZE,
    NODE_TYPE_OFFSET,
    POINTER_OFFSET,
    POINTER_SIZE,
    PTR_SIZE,
    ROW_ID_SEGMENT_SIZE,
    VALUE_SIZE,
};

use super::errors::Error;
use super::node_type::{ Key, KeyValuePair, NextPointer, NodeType, PageId, RowID };

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
            NodeType::Leaf(_, next, _) =>
                match next.0 {
                    Some(raw) => {
                        let mut dst = [0u8; 8];
                        dst.copy_from_slice(&raw);

                        Ok(dst)
                    }
                    None => Err(Error::UnexpectedError),
                }
            _ => Err(Error::UnexpectedError),
        }
    }

    /// sets the pointer to the sibling node
    pub fn set_next_pointer(&mut self, next: [u8; 8]) -> Result<(), Error> {
        let clone = self.clone();
        let data = BTreePage::try_from(&clone)?;
        let mut buf = data.get_data();

        let range = NEXT_LEAF_POINTER_OFFSET..NEXT_LEAF_POINTER_OFFSET + NEXT_LEAF_POINTER_SIZE;

        if buf.len() < NEXT_LEAF_POINTER_OFFSET + NEXT_LEAF_POINTER_SIZE {
            return Err(Error::UnexpectedError);
        }

        buf[range].copy_from_slice(&next);

        let b_tree_page = BTreePage::new(buf);

        *self = Node::try_from(b_tree_page)?;

        Ok(())
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
                        false
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
                        false
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
    pub fn find_key(&self, key: &Key) -> Result<Key, Error> {
        match &self.node_type {
            NodeType::Internal(_, keys, _) => {
                let idx = keys.binary_search(&key).unwrap().clone();

                Ok(keys.get(idx).unwrap().clone())
            }
            _ => Err(Error::UnexpectedError),
        }
    }

    pub fn find_key_index_in_leaf(&self, key: &Key) -> usize {
        match &self.node_type {
            NodeType::Leaf(entries, _, _) => {
                let entry = entries.binary_search_by_key(&key.0, |pair| pair.key).unwrap();
                entry
            }
            _ => 0,
        }
    }

    /// Gets a key-value pair at a certain index within a Leaf node
    pub fn get_key_value_at_index(&self, idx: usize) -> Result<KeyValuePair, Error> {
        match &self.node_type {
            NodeType::Leaf(entries, _, _) => {
                let entry = entries.get(idx);

                if entry.is_some() {
                    return Ok(entry.unwrap().clone());
                } else {
                    return Err(Error::UnexpectedError);
                }
            }
            _ => Err(Error::UnexpectedError),
        }
    }

    /// finds a key-value pair within a Leaf node
    pub fn find_key_value(&self, key: &Key) -> Result<KeyValuePair, Error> {
        match &self.node_type {
            NodeType::Leaf(entries, _, _) => {
                let was_found = entries.binary_search_by_key(key, |p| Key(p.key));

                let idx = match was_found {
                    Ok(idx) => idx,
                    Err(_) => {
                        return Err(Error::KeyNotFound);
                    }
                };

                match entries.get(idx) {
                    None => {
                        return {
                            println!("Printing error");
                            Err(Error::KeyNotFound)
                        };
                    }
                    Some(entry) => {
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
                let idx = entries.binary_search_by_key(&key, |p| p.key).unwrap_or_else(|x| x);

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
                let idx = entries.binary_search_by_key(key, |p| Key(p.key)).unwrap_or_else(|x| x);

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
        take_right: bool
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
                let promotion_key = keys
                    .get(len - 1)
                    .unwrap()
                    .clone();
                let (key, page_id) = self.remove_key_at_index(0, false)?;

                return Ok((
                    NodeKey::GuidePostKey(key),
                    NodeKey::GuidePostKey(promotion_key),
                    Some(page_id),
                ));
            }
            NodeType::Leaf(entries, _, _) => {
                let len = entries.len();
                let promotion_key = entries
                    .get(len - 1)
                    .unwrap()
                    .clone();
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
                let len = entries.len();
                let promotion_key = entries.get(0).unwrap().clone();
                println!("\n\n\nRemoval index {}", len - 1);
                let entry = self.remove_key_value_at_index(len - 1)?;

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
        separator_key: Key
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
                println!("Separator {:?}", separator_key);
                current_parent_node.find_and_update_key(separator_key, promotion_key)?;

                return Ok(());
            }
            NodeType::Internal(_, _, _) => {
                let promotion_key: Key;
                if is_left {
                    println!("Left");
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
                println!("Separator {:?}", separator_key);
                println!("Promoter {:?}", promotion_key);
                current_parent_node.find_and_update_key(separator_key, promotion_key)?;

                // Write updates to disk
                return Ok(());
            }
            _ => {
                return Err(Error::UnexpectedError);
            }
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

                    let mut merged_keys: Vec<Key> = keys
                        .iter()
                        .chain(sibling_keys.iter())
                        .cloned()
                        .collect();

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

        pointer = Some(
            PageId(
                u64::from_le_bytes(
                    page.get_ptr_from_offset(POINTER_OFFSET, POINTER_SIZE).try_into().unwrap()
                )
            )
        );

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

                Ok(
                    Node::new(
                        NodeType::Internal(children, keys, pointer.unwrap()),
                        pointer.unwrap(),
                        is_root
                    )
                )
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

                    slot_index.copy_from_slice(
                        &value[ROW_ID_SEGMENT_SIZE..ROW_ID_SEGMENT_SIZE + 4]
                    );

                    let row_id = RowID::new(
                        u32::from_le_bytes(page_id),
                        u32::from_le_bytes(slot_index)
                    );
                    offset += VALUE_SIZE;

                    // Trim leading or trailing zeros.
                    let key = u64::from_le_bytes(key_raw[0..PTR_SIZE].try_into().unwrap());
                    pairs.push(KeyValuePair::new(key, row_id));
                }

                Ok(
                    Node::new(
                        NodeType::Leaf(pairs, NextPointer(Some(next_pointer)), pointer.unwrap()),
                        pointer.unwrap(),
                        is_root
                    )
                )
            }

            NodeType::Unexpected => {
                println!("Unexpected error in try from");
                Err(Error::UnexpectedError)
            }
        }
    }
}
