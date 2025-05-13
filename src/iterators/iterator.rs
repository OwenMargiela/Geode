#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

pub trait StorageIterator {
    type Item;

    /// Get the current value.
    fn value(&self) -> Self::Item;

    /// Check if the current iterator is valid.
    fn is_valid(&self) -> bool;

    /// Move to the next position
    fn advance(&mut self) -> Result<(), ()> {
        unimplemented!()
    }
}
