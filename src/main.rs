use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap},
    fmt::Debug,
};

struct CollectionIndex<K, V> {
    index: Vec<K>,
    compare: Box<dyn Index<V>>,
}

struct Collection<K: Ord, V> {
    tree: BTreeMap<K, V>,
    indexes: HashMap<String, CollectionIndex<K, V>>,
    constraints: HashMap<String, Box<dyn Constraint<V>>>,
    subscribers: HashMap<String, Box<dyn Subscribe<K, V>>>,
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
            if !constraint.constrain(&val) {
                panic!("Can't set value {:?}. Fails on constraint '{}'", val, name);
            }
        }
        let old = self.tree.insert(key.clone(), val);
        let new = self.tree.get(&key).unwrap();

        for index in self.indexes.values_mut() {
            let mut flag = false;
            for (idx, key_ref) in index.index.iter().enumerate() {
                let a = self.tree.get(key_ref).unwrap();
                if index.compare.order(a, new).is_ge() {
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
                if i.order(a, b).is_ge() {
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

trait Index<V> {
    fn order(&self, a: &V, b: &V) -> Ordering;
}

struct Change<K, V> {
    pub key: K,
    pub update: Update<V>,
}

enum Update<T> {
    Set { old: Option<T>, new: T },
    Del { old: T },
}

trait Subscribe<K, V> {
    fn observe(&mut self, change: Change<&K, &V>);
}

trait Constraint<T> {
    fn constrain(&self, t: &T) -> bool;
}

#[derive(Debug)]
struct Counter {
    count: u32,
}

impl Counter {
    fn new() -> Self {
        Self { count: 0 }
    }

    fn start(count: u32) -> Self {
        Self { count }
    }
}

struct CounterValueIndex;
impl Index<Counter> for CounterValueIndex {
    fn order(&self, a: &Counter, b: &Counter) -> Ordering {
        a.count.cmp(&b.count)
    }
}

struct GlobalCount(u32);
impl GlobalCount {
    pub fn new() -> Self {
        Self(0)
    }
}
impl Subscribe<&str, Counter> for GlobalCount {
    fn observe(&mut self, change: Change<&&str, &Counter>) {
        match change.update {
            Update::Set { old, new } if old.is_none() => self.0 += new.count,
            Update::Set { old, new } => self.0 += old.unwrap().count - new.count,
            Update::Del { old } => self.0 -= old.count,
        }
    }
}

struct CreatedCounterMustStartAt0;
impl Constraint<Counter> for CreatedCounterMustStartAt0 {
    fn constrain(&self, t: &Counter) -> bool {
        t.count == 0
    }
}

fn main() {
    let mut collection = Collection::new();
    collection.set("1", Counter::new());

    collection.index("Counter Value", CounterValueIndex {});
    collection.subscriber("Total Count", GlobalCount::new());
    // collection.constraint("New Counter Must Be 0", CreatedCounterMustStartAt0 {});

    collection.set("2", Counter::start(10));
    collection.set("3", Counter::start(5));
    collection.set("4", Counter::start(15));
    collection.set("5", Counter::start(20));
    collection.set("6", Counter::start(7));

    let mut iter = collection.iter();
    println!("Sorted by key");
    for (index, i) in iter.enumerate() {
        println!("{}: {:?}", index, i);
    }
    // assert_eq!(iter.next().unwrap().count, 0);
    // assert_eq!(iter.next().unwrap().count, 10);
    // assert_eq!(iter.next().unwrap().count, 5);
    // assert_eq!(iter.next().unwrap().count, 15);
    // assert_eq!(iter.next().unwrap().count, 20);
    // assert_eq!(iter.next().unwrap().count, 7);

    let mut iter = collection.index_iter("Counter Value").unwrap();
    println!("\nSorted by value");
    for (index, i) in iter.enumerate() {
        println!("{}: {:?}", index, i);
    }

    // assert_eq!(iter.next().unwrap().count, 0);
    // assert_eq!(iter.next().unwrap().count, 5);
    // assert_eq!(iter.next().unwrap().count, 7);
    // assert_eq!(iter.next().unwrap().count, 10);
    // assert_eq!(iter.next().unwrap().count, 15);
    // assert_eq!(iter.next().unwrap().count, 20);
}
