#![allow(unused_variables)] 
#![allow(dead_code)] 

pub trait StorageIterator {
    type Item;

    /// Get the current value.
    fn value(&self) -> Self::Item;

    /// Check if the current iterator is valid.
    fn is_valid(&self) -> bool;

    /// Move to the next position
    fn advance(&mut self) -> anyhow::Result<()> {
        unimplemented!()
    }
}
