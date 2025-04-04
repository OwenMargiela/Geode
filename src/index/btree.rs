use std::{collections::VecDeque, vec};

use crate::{
    buffer::buffer_pool_manager::{AccessType, BufferPoolManager, FileId},
    storage::page::{
        b_plus_tree_page::BTreePage,
        btree_page_layout::{PAGE_SIZE, PTR_SIZE},
        page_guard::{ReadGuard, WriteGuard},
    },
};

use super::{
    errors::Error,
    node::Node,
    node_type::{Key, KeyValuePair, NextPointer, NodeType, PageId, RowID},
};

// Allow for interior mutability within the buffer pool
// Flirt with the idea of using a closure to encapsulate all logic needed
// for reads and writes if needed. Or if I remember >_<
pub struct BTree {
    bpm: BufferPoolManager,

    index_name: String,
    index_file_id: FileId,
    b: usize,
    root_page_id: PageId,
}

pub struct Context<'a> {
    // Save the root page id here so that it's easier to know if the current page is the root page.
    // This probaly should be a u32 instead of a u64
    root_page_id: PageId,

    // Store the ids of the pages that you're modifying here.
    write_set: VecDeque<PageId>,

    // you may want to use this when getting a value, but not necessary
    read_set: VecDeque<ReadGuard<'a>>,
}

pub struct BTreeBuilder {
    /// Path to the tree file

    /// The BTree parameter, an inner node contains no more than 2*b - 1 keys
    /// and no less than b-1 keys and no more than 2*b children and no less than b children
    b: usize,
}

impl BTreeBuilder {
    pub fn new() -> BTreeBuilder {
        BTreeBuilder { b: 0 }
    }
    pub fn b_parameter(mut self, b: usize) -> BTreeBuilder {
        self.b = b;
        self
    }

    pub fn build(&self, index_name: String, mut bpm: BufferPoolManager) -> Result<BTree, Error> {
        let index_file_id = bpm.allocate_file();
        let b: usize = self.b;

        if b == 0 {
            return Err(Error::UnexpectedError);
        }

        let page_id = bpm.new_page(index_file_id);

        {
            let root = Node::new(NodeType::Leaf(vec![], NextPointer(None)), true);
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
            b,
            root_page_id: PageId(page_id.into()),
        })
    }
}

impl<'a> Context<'a> {
    // Page id should be a u32
    pub fn build(root_page_id: PageId) -> Result<Self, crate::index::errors::Error> {
        Ok(Self {
            root_page_id,
            write_set: VecDeque::new(),
            read_set: VecDeque::new(),
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
            NodeType::Leaf(pairs, _) => Ok(pairs.len() == (2 * self.b)),
            NodeType::Internal(_, keys) => Ok(keys.len() == (2 * self.b - 1)),
            NodeType::Unexpected => Err(Error::UnexpectedError),
        }
    }

    pub fn is_node_underflow(&self, node: &Node) -> Result<bool, Error> {
        match &node.node_type {
            // A root cannot really be "underflowing" as it can contain less than b-1 keys / pointers.
            NodeType::Leaf(pairs, _) => Ok(pairs.len() < (self.b - 1) && !node.is_root),
            NodeType::Internal(_, keys) => Ok(keys.len() < (self.b - 1) && !node.is_root),
            NodeType::Unexpected => Err(Error::UnexpectedError),
        }
    }

    pub fn pg_write_helper(&self, page_id: &PageId, node: &Node) {
        let new_root_data = BTreePage::try_from(node).unwrap();

        let write_guard = self
            .bpm
            .write_page(self.index_file_id, page_id.0.try_into().unwrap());

        write_guard
            .get_frame()
            .data
            .copy_from_slice(&new_root_data.get_data());
    }

    pub fn pg_read_helper(&self, page_id: PageId) -> Result<ReadGuard, Error> {
        let read_guard = self
            .bpm
            .read_page(self.index_file_id, page_id.0.try_into().unwrap());

        Ok(read_guard)
    }

    pub fn shutdown(&self) {}

    /// Inserts a key-value pair into the b+ tree object
    pub fn insert(&mut self, kv: KeyValuePair) -> Result<(), Error> {
        // Starts off at the root node

        let root_page_id = self.root_page_id.clone();

        // let mut ctx = Context::build(root_page_id.clone()).unwrap();
        let mut root_raw = [0u8; PAGE_SIZE];

        {
            let root_page_guard = self
                .bpm
                .read_page(self.index_file_id, root_page_id.0.try_into().unwrap());

            root_raw.copy_from_slice(&root_page_guard.get_frame().data);

            // ctx.read_set.push_back(root_page_guard);
        }

        let new_root_id: PageId;

        let root_page = BTreePage::new(root_raw);

        let mut root = Node::try_from(root_page)?;

        let mut new_root: Node;

        if self.is_node_full(&root)? {
            // If so split the old root into to separate nodes (old root, sibling)
            // and initialize the new root with a vector containing
            // the pointers to the old root and the sibling

            new_root = Node::new(NodeType::Internal(vec![], vec![]), true);

            new_root_id = PageId(self.bpm.new_page(self.index_file_id).into());

            self.pg_write_helper(&new_root_id, &new_root);

            root.parent_id = Some(new_root_id.clone());
            root.is_root = false;

            let (median, sibling) = root.split(self.b).unwrap();

            // write the old root with its new data to disk in a *new* location
            let old_root_new_pointer = self.bpm.new_page(self.index_file_id);
            self.pg_write_helper(&PageId(old_root_new_pointer.into()), &root);

            // write the newly created sibling to disk.
            let sibling_pointer = self.bpm.new_page(self.index_file_id);
            self.pg_write_helper(&PageId(sibling_pointer.into()), &sibling);

            // update the new root with its children and key.
            let vec_pointers = vec![
                PageId(old_root_new_pointer.into()),
                PageId(sibling_pointer.into()),
            ];

            new_root.node_type = NodeType::Internal(vec_pointers, vec![median]);

            // write the new_root to disk
            self.pg_write_helper(&new_root_id, &new_root);
        } else {
            new_root_id = PageId(self.bpm.new_page(self.index_file_id).into());
            new_root = root.clone();

            self.pg_write_helper(&new_root_id, &new_root);
        }

        // Continue recursively

        println!("Recursing");
        self.choose_sub_tree(&mut root, new_root_id, kv)?;
        // Finish by setting the root to the new pointer
        self.root_page_id = new_root_id;

        Ok(())
    }

    // Next pointer is currently 8 bytes when it really should be 8

    /// choose_sub_tree (recursively) finds a node rooted at a given non-full node.
    /// to insert a given key-value pair. Here we assume the node is
    /// already a copy of an existing node in a copy-on-write root to node traversal.

    pub fn choose_sub_tree<'a>(
        &'a self,
        node: &mut Node,
        node_pointer: PageId,
        kv: KeyValuePair,
        // mut context: Context<'a>,
    ) -> Result<(), Error> {
        match &mut node.node_type {
            NodeType::Leaf(ref mut pairs, _) => {
                let indx = pairs.binary_search(&kv).unwrap_or_else(|x| x);
                pairs.insert(indx, kv);

                self.pg_write_helper(&node_pointer, node);

                Ok(())
            }

            NodeType::Internal(ref mut children, ref mut keys) => {
                let idx = keys
                    .binary_search(&Key(kv.key.clone()))
                    .unwrap_or_else(|x| x);

                let child_pointer = children.get(idx).ok_or(Error::UnexpectedError)?.clone();

                let root_raw = [0u8; PAGE_SIZE];

                let page_guard = self.pg_read_helper(child_pointer)?;
                // context.read_set.push_back(page_guard);

                let child_page = BTreePage::new(root_raw);
                let mut child = Node::try_from(child_page)?;

                // Copy each branching-node on the root-to-leaf walk.
                // Todo turn the page allocation mechanisms into a function

                let new_child_pointer = self.bpm.new_page(self.index_file_id);
                self.pg_write_helper(&PageId(new_child_pointer.into()), &child);

                children[idx] = PageId(new_child_pointer.try_into().unwrap());

                if self.is_node_full(&child)? {
                    let (median, mut sibling) = child.split(self.b)?;

                    self.pg_write_helper(&PageId(new_child_pointer.into()), &child);

                    // Write the newly split nodes to disk

                    let new_sibling_pointer = self.bpm.new_page(self.index_file_id);
                    self.pg_write_helper(&PageId(new_sibling_pointer.into()), &sibling);

                    // Siblings keys are larger than the splitted child thus need to be inserted
                    // at the next index.

                    children.insert(idx + 1, PageId(new_sibling_pointer.into()));
                    keys.insert(idx, median.clone());

                    // Write the parent page to disk.
                    self.pg_write_helper(&node_pointer, &node);

                    if kv.key <= median.0 {
                        self.choose_sub_tree(
                            &mut child,
                            PageId(new_child_pointer.try_into().unwrap()),
                            kv,
                            // context,
                        )?;
                    } else {
                        self.choose_sub_tree(
                            &mut sibling,
                            PageId(new_sibling_pointer.try_into().unwrap()),
                            kv,
                            // context,
                        )?;
                    }
                } else {
                    self.pg_write_helper(&node_pointer, &node);

                    self.choose_sub_tree(
                        &mut child,
                        PageId(new_child_pointer.try_into().unwrap()),
                        kv,
                        // context,
                    )?;
                }

                Ok(())
            }

            NodeType::Unexpected => Err(Error::UnexpectedError),
        }
    }

    /// Finds a specific key in the BTree.
    pub fn find(&self, key: Key) -> Result<KeyValuePair, Error> {
        let root_page: BTreePage;

        {
            let root_guard = self.pg_read_helper(self.root_page_id)?;

            root_page = BTreePage::new(root_guard.get_frame().data);
        }

        let root = Node::try_from(root_page)?;
        self.find_node(root, key)
    }

    /// Recursively searches a sub tree rooted at a node for a key.
    pub fn find_node(&self, node: Node, search: Key) -> Result<KeyValuePair, Error> {
        match node.node_type {
            NodeType::Internal(children, keys) => {
                let idx = keys.binary_search(&search).unwrap_or_else(|x| x);

                // retrieve child page
                let child_pointer = children.get(idx).ok_or(Error::UnexpectedError)?;
                let page = self.pg_read_helper(*child_pointer);
                let child_page: BTreePage;

                {
                    let child_guard = self.pg_read_helper(self.root_page_id)?;

                    child_page = BTreePage::new(child_guard.get_frame().data);
                }

                let child_node = Node::try_from(child_page)?;

                self.find_node(child_node, search)
            }

            NodeType::Leaf(pairs, _) => {
                // Im stupid I made the key type a thing but in the actual key in the KeyValuePair is not event the type i made
                let key = search.0;
                if let Ok(idx) = pairs.binary_search_by_key(&key, |pair| pair.key.clone()) {
                    return Ok(pairs[idx].clone());
                }

                Err(Error::KeyNotFound)
            }

            NodeType::Unexpected => Err(Error::UnexpectedError),
        }
    }

    /// Returns the value associated with a given key
    pub fn get(&self, key: Key) -> Result<RowID, Error> {
        let entry = self.find(key)?;
        Ok(entry.value)
    }

    /// Returns a key-value pair
    fn get_entry(&self, key: Key) -> Result<KeyValuePair, Error> {
        let entry = self.find(key)?;
        Ok(entry)
    }

    /// delete deletes a given key from the tree.
    pub fn delete(&self, key: Key) -> Result<(), Error> {
        // Get, serialize then copy the contents of the root page
        // Proceed with the recursive delete_key_from_subtree function

        Ok(())
    }

    // delete key from subtree recursively traverses a tree rooted at a node in certain offset
    // until it finds the given key and delete the key-value pair. Here we assume the node is
    // already a copy of an existing node in a copy-on-write root to node traversal.
    // fn delete_key_from_subtree(
    //    &mut self,
    //    key: Key,
    //    node: &mut Node,
    //    node_offset: &Offset,
    // ) -> Result<(), Error>

    /*
    match on node_type -> {
        Leaf -> {
            Similar to insert logic
            Except we remove the kv pair and
            check if any underflows were created

            If so we merge with a siblings recursively up the tree
            }

            Internal -> {

            Retrieve the child page
            Create a new copy of the child page and write it out to disk

            Assign the new pointer to the newly created child page in the parent
            write that update to disk

            We're doing a lot of disk writes this copy on write structure might not be optimal

            Continue recursively to child node

            }


        }



        */

    // borrow_if_needed checks the node for underflow (following a removal of a key),
    // if it underflows it is merged with a sibling node, and than called recoursively
    // up the tree. Since the downward root-to-leaf traversal was done using the copy-on-write
    // technique we are ensured that any merges will only be reflected in the copied parent in the path.
    // fn borrow_if_needed(&mut self, node: Node, key: &Key) -> Result<(), Error> { }

    /*
    If underflow

    fetch sibling node

    merge current and sibling node


    if the parent is the root, and there is a single child - the merged node -
    we can safely replace the root with the child.

    remove the keys that separated the two nodes from each other

    write the new node in place.

    write the updated parent back to disk and continue up the tree
    by passing in the parent pointer.


    */

    // merges two *sibling* nodes, it assumes the following:
    // 1. the two nodes are of the same type.
    // 2. the two nodes do not accumulate to an overflow,
    // i.e. |first.keys| + |second.keys| <= [2*(b-1) for keys or 2*b for offsets].
    // fn merge(&self, first: Node, second: Node) -> Result<Node, Error>

    /*
    match first.node_type
    Leaf -> {
               Make sure other node is a leaf

               merge the vector of KeyValuePairs together
               construct a new leaf node make sure to give the newly constructed
               node the next pointer of the right sibling

               }

               Internal -> {
                Make sure other node is an internal leaf

               Merge keys and offsets
               }




               */
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

#[cfg(test)]
mod tests {
    use crate::{
        buffer::buffer_pool_manager::BufferPoolManager,
        index::{
            errors::Error,
            node_type::{Key, KeyValuePair, RowID},
        },
        storage::disk::manager::Manager,
    };

    use super::BTreeBuilder;

    #[test]
    fn search_works() -> Result<(), Error> {
        let (log_io, log_file_path) = Manager::open_log();
        let manager = Manager::new(log_io, log_file_path);
        let bpm = BufferPoolManager::new(4, manager, 2);

        let mut btree = BTreeBuilder::new()
            .b_parameter(2)
            .build(String::from("index"), bpm)?;

        let kv_pair_one = KeyValuePair::new(42, RowID::new(0, 0));
        let kv_pair_two = KeyValuePair::new(3, RowID::new(0, 1));
        let kv_pair_three = KeyValuePair::new(8, RowID::new(0, 2));

        btree.insert(kv_pair_one)?;

        let value = btree.get(Key::new(42))?;

        assert_eq!(value.page_id, 0_u32.to_le_bytes());
        assert_eq!(value.slot_index, 0_u32.to_le_bytes());

        btree.insert(kv_pair_two)?;

        let value = btree.get(Key::new(3))?;

        assert_eq!(value.page_id, 0_u32.to_le_bytes());
        assert_eq!(value.slot_index, 1_u32.to_le_bytes());

        btree.insert(kv_pair_three)?;

        let value = btree.get(Key::new(8))?;

        assert_eq!(value.page_id, 0_u32.to_le_bytes());
        assert_eq!(value.slot_index, 2_u32.to_le_bytes());

        Ok(())
    }
}
