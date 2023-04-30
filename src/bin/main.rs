use std::{
    io::{BufRead, Write},
    time::SystemTime,
};

use conventually::Database;

#[derive(Debug, serde::Serialize)]
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
        let deleted = if self.completed { "x" } else { " " };
        write!(
            f,
            "| completed: [{}] | deleted: [{}] | > {}",
            complete, deleted, self.name
        )
    }
}

fn main() -> anyhow::Result<()> {
    let mut buffer = String::new();
    let mut stdin = std::io::stdin().lock();
    let mut stdout = std::io::stdout().lock();

    let mut ticket_ids = 0;

    let mut db = Database::restore("db");
    let mut ticket_store = db.create::<u64, Ticket>("Ticket");

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
                ticket_store.set(ticket_ids, Ticket::new(buffer.clone()))?;
                ticket_ids += 1;
            }
            "list" => {
                for ticket in ticket_store.as_iter() {
                    write!(stdout, "{}", ticket)?;
                }
            }
            "quit" => {
                writeln!(stdout, "Saving Tasks to Disk...")?;
                writeln!(stdout, "Quiting...")?;
                db.close("db")?;
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
    // collection.subscriber("Total Count", GlobalCount::new());
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
