use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;

use crate::Table;

use super::{Change, Constraint, Index, Subscribe, Update};

pub struct CollectionIndex<K, V> {
    index: Vec<K>,
    compare: Box<dyn Index<V>>,
}

pub struct Collection<K: Ord, V> {
    tree: BTreeMap<K, V>,
    indexes: HashMap<String, CollectionIndex<K, V>>,
    constraints: HashMap<String, Box<dyn Constraint<V>>>,
    subscribers: HashMap<String, Box<dyn Subscribe<K, V>>>,
}

impl<K: Ord, V> Table for Collection<K, V> {
    fn create(this: Self)
    where
        Self: Sized,
    {
        todo!()
    }
}

impl<K: Ord, V> Collection<K, V> {
    pub fn new() -> Self {
        Self {
            tree: BTreeMap::new(),
            indexes: HashMap::new(),
            constraints: HashMap::new(),
            subscribers: HashMap::new(),
        }
    }

    pub fn set(&mut self, key: K, val: V)
    where
        K: Debug + Clone,
        V: Debug,
    {
        for (name, constraint) in self.constraints.iter() {
            if !constraint.check(&val) {
                panic!("Can't set value {:?}. Fails on constraint '{}'", val, name);
            }
        }
        let old = self.tree.insert(key.clone(), val);
        let new = self.tree.get(&key).unwrap();

        for index in self.indexes.values_mut() {
            let mut flag = false;
            for (idx, key_ref) in index.index.iter().enumerate() {
                let a = self.tree.get(key_ref).unwrap();
                if index.compare.compare(a, new).is_ge() {
                    index.index.splice(idx..idx, [key.clone()]);
                    flag = true;
                    break;
                }
            }

            if !flag {
                index.index.push(key.clone());
            }
        }

        for subscriber in self.subscribers.values_mut() {
            let change = Change {
                key: &key,
                update: Update::Set {
                    old: old.as_ref(),
                    new,
                },
            };
            subscriber.observe(change);
        }
    }

    pub fn del(&mut self, key: K) {}

    pub fn index<I: Index<V> + 'static>(&mut self, name: impl AsRef<str>, i: I)
    where
        K: Clone,
    {
        let mut index = Vec::new();

        for (key, a) in self.tree.iter() {
            let mut flag = false;

            for (idx, key_ref) in index.iter().enumerate() {
                let b = self.tree.get(key_ref).unwrap();
                if i.compare(a, b).is_ge() {
                    index.splice(idx..idx, [key.clone()]);
                    flag = true;
                    break;
                }
            }

            if !flag {
                index.push(key.clone());
            }
        }

        self.indexes.insert(
            name.as_ref().to_string(),
            CollectionIndex {
                index,
                compare: Box::new(i),
            },
        );
    }

    pub fn constraint<C: Constraint<V> + 'static>(&mut self, name: impl AsRef<str>, c: C) {
        self.constraints
            .insert(name.as_ref().to_string(), Box::new(c));
    }

    pub fn subscriber<S: Subscribe<K, V> + 'static>(&mut self, name: impl AsRef<str>, s: S) {
        self.subscribers
            .insert(name.as_ref().to_string(), Box::new(s));
    }

    pub fn iter(&self) -> impl Iterator<Item = &V> {
        self.tree.values()
    }

    pub fn index_iter(&self, key: impl AsRef<str>) -> Option<impl Iterator<Item = &V>> {
        Some(
            self.indexes
                .get(key.as_ref())?
                .index
                .iter()
                .map(|k| self.tree.get(k).unwrap()),
        )
    }
}
