use std::{
    io::{BufRead, Write},
    time::SystemTime,
};

use conventually::{Aggregate, Change, Database, Update};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Ticket {
    name: String,
    completed: bool,
    deleted: bool,

    created_at: u128,
    updated_at: u128,
}

impl Ticket {
    pub fn new(name: impl ToString) -> Self {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        Self {
            name: name.to_string(),
            completed: false,
            deleted: false,
            created_at: now,
            updated_at: now,
        }
    }
}

impl std::fmt::Display for Ticket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let complete = if self.completed { "x" } else { " " };
        let deleted = if self.deleted { "x" } else { " " };
        write!(
            f,
            "| completed: [{}] | achived: [{}] | {}",
            complete, deleted, self.name
        )
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct TotalTickets {
    total: usize,
}
impl Aggregate<u64, Ticket> for TotalTickets {
    fn observe(&mut self, change: Change<&u64, &Ticket>) {
        match change.update {
            Update::Set { old, new: _ } => {
                if old.is_none() {
                    self.total += 1;
                }
            }
            Update::Del { old: _ } => {
                self.total -= 1;
            }
        };
    }
}

struct TotalCompletedTickets {
    total: usize,
}
impl Aggregate<u64, Ticket> for TotalCompletedTickets {
    fn observe(&mut self, change: Change<&u64, &Ticket>) {
        let change = match change.update {
            Update::Set { old, new } => {
                if let Some(old) = old {
                    if old.completed && new.completed {
                        0
                    } else if new.completed {
                        1
                    } else {
                        0
                    }
                } else if new.completed {
                    1
                } else {
                    0
                }
            }
            Update::Del { old } => {
                if old.completed {
                    -1
                } else {
                    0
                }
            }
        };
        self.total = (self.total as isize + change) as usize;
    }
}

struct TotalArchivedTickets {
    total: usize,
}
impl Aggregate<u64, Ticket> for TotalArchivedTickets {
    fn observe(&mut self, change: Change<&u64, &Ticket>) {
        let change = match change.update {
            Update::Set { old, new } => {
                if let Some(old) = old {
                    if old.deleted && new.deleted {
                        0
                    } else if new.deleted {
                        1
                    } else {
                        0
                    }
                } else if new.deleted {
                    1
                } else {
                    0
                }
            }
            Update::Del { old } => {
                if old.deleted {
                    -1
                } else {
                    0
                }
            }
        };
        self.total = (self.total as isize + change) as usize;
    }
}

// struct CompletedTickets;
// impl SubCollection<Ticket> for CompletedTickets {
//     fn include(&self, item: &Ticket) -> bool {
//         item.completed
//     }
// }

fn main() -> anyhow::Result<()> {
    let mut buffer = String::new();
    let mut stdin = std::io::stdin().lock();
    let mut stdout = std::io::stdout().lock();

    let mut db = Database::new();
    // let mut db = Database::restore(".db/wal")?;
    // let mut ticket_store = db.create::<u64, Ticket>("Ticket")?;
    // let mut total_ticket_count = db.aggragate::<u32, Ticket, TotalTickets>("Total Tickets")?;

    println!("\nTask Cli Database!\n");

    loop {
        stdout.write_all("> ".as_bytes())?;
        stdout.flush()?;
        stdin.read_line(&mut buffer)?;

        match buffer.to_lowercase().trim() {
            "create" => {
                write!(stdout, "Name > ")?;
                stdout.flush()?;
                buffer.clear();
                stdin.read_line(&mut buffer)?;
                // ticket_store.create(Ticket::new(buffer.clone()))?;
            }
            "complete" => {
                write!(stdout, "Id > ")?;
                stdout.flush()?;
                buffer.clear();
                stdin.read_line(&mut buffer)?;

                // let id = buffer.trim().parse()?;
                // match ticket_store.get_mut(&id, |ticket| ticket.completed = true) {
                //     Ok(None) => writeln!(stdout, "Key '{}' does not exist", id)?,
                //     Err(err) => {
                //         writeln!(stdout, "Failed to update key '{}' with error: {}", id, err)?
                //     }
                //     _ => {}
                // }
            }
            "achive" => {
                write!(stdout, "Id > ")?;
                stdout.flush()?;
                buffer.clear();
                stdin.read_line(&mut buffer)?;

                // let id = buffer.trim().parse()?;
                // match ticket_store.get_mut(&id, |ticket| ticket.deleted = true) {
                //     Ok(None) => writeln!(stdout, "Key '{}' does not exist", id)?,
                //     Err(err) => {
                //         writeln!(stdout, "Failed to update key '{}' with error: {}", id, err)?
                //     }
                //     _ => {}
                // }
            }
            "list" => {
                // for (id, ticket) in ticket_store.as_iter() {
                //     write!(stdout, "{} | {}", id, ticket)?;
                // }
            }
            "quit" => {
                writeln!(stdout, "Saving Tasks to Disk...")?;
                writeln!(stdout, "Quiting...")?;
                db.close(".db/wal")?;
                break;
            }
            "clear" => {
                print!("\x1B[2J");
            }
            command => {
                writeln!(stdout, "ERROR: Unknown command -> '{}'", command)?;
            }
        }

        buffer.clear();
    }

    println!("Done!");

    // let mut collection = Collection::new();
    // collection.set("1", Counter::new());

    // collection.index("Counter Value", CounterValueIndex {});
    // collection.Aggregater("Total Count", GlobalCount::new());
    // // collection.constraint("New Counter Must Be 0", CreatedCounterMustStartAt0 {});

    // collection.set("2", Counter::start(10));
    // collection.set("3", Counter::start(5));
    // collection.set("4", Counter::start(15));
    // collection.set("5", Counter::start(20));
    // collection.set("6", Counter::start(7));

    // let mut iter = collection.iter();
    // println!("Sorted by key");
    // for (index, i) in iter.enumerate() {
    //     println!("{}: {:?}", index, i);
    // }
    // assert_eq!(iter.next().unwrap().count, 0);
    // assert_eq!(iter.next().unwrap().count, 10);
    // assert_eq!(iter.next().unwrap().count, 5);
    // assert_eq!(iter.next().unwrap().count, 15);
    // assert_eq!(iter.next().unwrap().count, 20);
    // assert_eq!(iter.next().unwrap().count, 7);

    // let mut iter = collection.index_iter("Counter Value").unwrap();
    // println!("\nSorted by value");
    // for (index, i) in iter.enumerate() {
    //     println!("{}: {:?}", index, i);
    // }

    // assert_eq!(iter.next().unwrap().count, 0);
    // assert_eq!(iter.next().unwrap().count, 5);
    // assert_eq!(iter.next().unwrap().count, 7);
    // assert_eq!(iter.next().unwrap().count, 10);
    // assert_eq!(iter.next().unwrap().count, 15);
    // assert_eq!(iter.next().unwrap().count, 20);

    Ok(())
}
