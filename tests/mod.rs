use std::path::Path;

use tokactordb::{Database, FileSystem, Tree, U32};

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct Counter {
    name: String,
    count: usize,
}

impl Counter {
    fn new(name: impl ToString) -> Self {
        Self {
            name: name.to_string(),
            count: 0,
        }
    }
}

struct Db {
    db: Database,
    counter: Tree<U32, Counter>,
}

async fn open(path: impl AsRef<Path>) -> Db {
    let filesystem = FileSystem::in_memory();

    let db = Database::new(filesystem).await.unwrap();

    let counter = db
        .create::<U32, Counter>("example")
        .unwrap()
        .unwrap()
        .await
        .unwrap();

    db.restore(path).await.unwrap();

    Db { db, counter }
}

#[tokio::test]
async fn test_test() {
    let db = open("test").await;

    let test_counter = Counter::new("test");
    let id = db.counter.insert(test_counter.clone()).await.unwrap();
    let get_test_counter = db.counter.get(id).await.unwrap().unwrap();
    assert_eq!(get_test_counter, test_counter);

    drop(db);
}
