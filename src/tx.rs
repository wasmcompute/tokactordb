// pub enum Action {
//     Begin,
//     Set {
//         key: Vec<u8>,
//         value: Vec<u8>,
//         secondary_indexes: Vec<Vec<u8>>,
//     },
//     Delete {
//         key: Vec<u8>,
//     },
//     Commit,
//     Rollback,
// }

// pub struct Transaction {
//     log: Vec<Action>,
// }

// impl Transaction {
//     pub fn new() -> Self {
//         Self { log: Vec::new() }
//     }
// }
