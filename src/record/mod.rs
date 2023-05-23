use std::cmp::Ordering;

mod collection;

// pub use collection::Collection;

/// Record the key that was updated along with the state that was changed.
#[derive(Debug)]
pub struct Change<K, V> {
    pub key: K,
    pub update: Update<V>,
}

/// Updates to a given record. A new record will be a [`Update::Set`] operation
/// with old being set to None. Otherwise old will be set.
#[derive(Debug)]
pub enum Update<T> {
    Set { old: Option<T>, new: T },
    Del { old: T },
}

/// For a given primary key and value, collect changes for ALL records. Changes
/// include:
///
/// 1. Creating a new Record
/// 2. Updating a existing Record
/// 3. Removing an existing Record
///
/// Subscribed changes are called everytime after a record is planned to be written
/// to disk.
///
/// Subscribing to a given record is best only if you plan on aggragating records
/// together.
pub trait Aggregate<K, V>: Default {
    fn observe(&mut self, change: Change<&K, &V>);
}

/// Apply a construct to an object. Implementing this trait allows for control over
/// what can be created for a given record. Before saving the record to disk, check
/// that the constraint is valid. Return `false` if the record being created does
/// not match what is expected.
pub trait Constraint<T> {
    fn check(&self, t: &T) -> bool;
}

/// Create an index by implementing a compare function. Declare exactly what the
/// ordering of the function should be and allow the framework to figure out where
/// to store the index.
pub trait SecondaryIndex<V> {
    fn compare(&self, a: &V, b: &V) -> Ordering;
}
