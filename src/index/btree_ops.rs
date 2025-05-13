#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{ops::RangeBounds, sync::Arc};

use crate::iterators::b_tree_iterator::BTreeIterator;

use super::{
    btree::{BTree, Protocol, Set, WriteOperation},
    errors::Error,
    node_type::{Key, KeyValuePair},
};

// Todo
// Variable length keys and value

impl BTree {
    pub fn insert(&self, entry: KeyValuePair) -> Result<(), Error> {
        let key = Key(entry.key);

        let (mut leaf_node, mut context) =
            self.tree_descent(key, Protocol::Exclusive, WriteOperation::Insert)?;

        leaf_node.insert_entry(entry)?;

        if leaf_node.get_key_array_length() < 2 * self.d {
            let stack = match &mut context.set {
                Set::WriteStack(stack) => stack,
                _ => return Err(Error::UnexpectedError),
            };

            // Update the leaf_node data using the WriteGuard<'_>
            let pointer = stack.pop_front().unwrap().child;

            self.flush_data(&leaf_node, pointer.0.try_into().unwrap());

            drop(context);
            return Ok(());
        } else if leaf_node.get_key_array_length() >= 2 * self.d {
            self.propogate_upwards(leaf_node, context)?;
        }

        Ok(())
    }

    pub fn delete(&self, search: &Key) -> Result<(), Error> {
        let (mut leaf_node, mut context) =
            self.tree_descent(search.clone(), Protocol::Exclusive, WriteOperation::Delete)?;

        leaf_node.remove_entry(search)?;

        // Simple Delete No Split

        if leaf_node.get_key_array_length() >= self.d - 1 || leaf_node.is_root {
            match &mut context.set {
                Set::WriteStack(stack) => stack.front().unwrap(),
                _ => return Err(Error::UnexpectedError),
            };

            // write update to disk
            self.flush_data(&leaf_node, leaf_node.pointer.unwrap().0.try_into().unwrap());
            drop(context);
            return Ok(());
        } else {
            let leaf_node_id = match &mut context.set {
                Set::WriteStack(stack) => {
                    let child = stack.pop_front().unwrap().child;
                    child
                }
                _ => return Err(Error::UnexpectedError),
            };

            self.borrow_if_needed(leaf_node_id, leaf_node, context)?;
        }

        Ok(())
    }

    pub fn scan(&self, range: impl RangeBounds<Key>) -> BTreeIterator {
        let lower_bound_page = match range.start_bound() {
            std::ops::Bound::Excluded(key) | std::ops::Bound::Included(key) => {
                let page = self.find_node(key).unwrap();
                page
            }
            std::ops::Bound::Unbounded => {
                let page = self.find_min().unwrap();
                page
            }
        };

        let leaf_iter = BTreeIterator::create_and_seek_to_key(
            self.bpm.clone(),
            self.index_file_id,
            Arc::new(lower_bound_page),
            range,
        );

        leaf_iter
    }

    pub fn get_value(&self, search: Key) -> Result<Key, Error> {
        let found_node = self.find_node(&search).unwrap();
        let key = found_node.find_key(&search)?;
        Ok(key)
    }

    pub fn get_entry(&self, search: Key) -> Result<KeyValuePair, Error> {
        let found_node = self.find_node(&search).unwrap();
        let entry = found_node.find_key_value(&search)?;
        Ok(entry)
    }
}
