use std::{
    fmt::Display,
    io::{BufRead, StdinLock, StdoutLock, Write},
    time::SystemTime,
};

use conventually::{Aggregate, Change, Database, SubTree, Tree, Update, ID, U32, U64};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Ticket {
    board: ID<U32, Board>,
    name: String,
    completed: bool,
    deleted: bool,

    created_at: u128,
    updated_at: u128,
}

impl Ticket {
    pub fn new(board: ID<U32, Board>, name: impl ToString) -> Self {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        Self {
            board,
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
            "| {} | completed: [{}] | achived: [{}] | {}",
            self.board, complete, deleted, self.name
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

#[tokio::main(flavor = "current_thread")]
async fn main() {
    run().await.unwrap();
}

struct Cli<'a> {
    buffer: String,
    stdin: StdinLock<'a>,
    stdout: StdoutLock<'a>,
}

impl<'a> Cli<'a> {
    pub fn new() -> Self {
        Self {
            buffer: String::new(),
            stdin: std::io::stdin().lock(),
            stdout: std::io::stdout().lock(),
        }
    }

    pub fn read_line(&mut self, pre_text: impl AsRef<str>) -> anyhow::Result<String> {
        self.stdout.write_all(pre_text.as_ref().as_bytes())?;
        self.stdout.flush()?;
        self.stdin.read_line(&mut self.buffer)?;
        let output = self.buffer.trim().to_string();
        self.buffer.clear();
        Ok(output)
    }

    pub fn write(&mut self, text: impl Display) -> anyhow::Result<()> {
        writeln!(self.stdout, "{}", text)?;
        Ok(())
    }

    pub fn error(&mut self, text: impl AsRef<str>) -> anyhow::Result<()> {
        writeln!(self.stdout, "{}", text.as_ref())?;
        Ok(())
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct Board {
    name: String,
}

impl Board {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

impl std::fmt::Display for Board {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

async fn board<'a>(
    cli: &mut Cli<'a>,
    board_store: &Tree<U32, Board>,
) -> anyhow::Result<Option<ID<U32, Board>>> {
    cli.write("Choose a board!")?;
    loop {
        let cmd = cli.read_line("> ")?.to_lowercase();

        match cmd.as_str() {
            "create" => {
                let name = cli.read_line("Name > ")?;
                board_store.insert(Board::new(name)).await?;
            }
            "list" => {
                let mut list = board_store.list().await;
                while let Some((id, board)) = list.next().await {
                    if let Some(board) = board {
                        cli.write(format!("{} -> {}", id, board))?;
                    }
                }
            }
            "open" => {
                let str_id = cli.read_line("Id > ")?;
                let id = str_id.trim().parse::<u32>()?;
                if board_store.get(id).await.unwrap().is_some() {
                    return Ok(Some(ID::new(id.into())));
                } else {
                    cli.error(format!("Board ID {} does not exist", str_id.trim()))?;
                }
            }
            "exit" | "quit" | "e" | "q" => return Ok(None),
            command => {
                cli.write(format!("ERROR: Unknown command -> '{}'", command))?;
            }
        }
    }
}

async fn tickets<'a>(
    cli: &mut Cli<'a>,
    board: ID<U32, Board>,
    ticket_store: &Tree<U64, Ticket>,
    board_tickets: &SubTree<ID<U32, Board>, Ticket>,
) -> anyhow::Result<()> {
    loop {
        let cmd = cli.read_line("> ")?.to_lowercase();

        match cmd.as_str() {
            "create" => {
                let name = cli.read_line("Name > ")?;
                ticket_store
                    .insert(Ticket::new(board.clone(), name))
                    .await?;
            }
            "complete" | "archive" => {
                let str_id = cli.read_line("Id > ")?;
                let id = str_id.trim().parse::<u64>()?;
                if let Some(mut ticket) = ticket_store.get(id).await.unwrap() {
                    if cmd == "complete" {
                        ticket.completed = true;
                    } else if cmd == "archive" {
                        ticket.deleted = true;
                    }
                    if let Err(err) = ticket_store.update(id, ticket).await {
                        cli.error(format!("Failed to update key '{}' with error: {}", id, err))?;
                    }
                } else {
                    cli.error(format!("ID {} does not exist", str_id.trim()))?;
                }
            }
            "get" => {
                let str_id = cli.read_line("Id > ")?;
                let id = str_id.trim().parse::<u64>()?;
                if let Some(ticket) = ticket_store.get(id).await.unwrap() {
                    cli.write(ticket)?;
                } else {
                    cli.error(format!("ID {} does not exist", str_id.trim()))?;
                }
            }
            "list" => {
                let list = board_tickets.get(board.clone()).await?;
                for ticket in list {
                    cli.write(format!("{}", ticket))?;
                }
            }
            "quit" | "exit" | "q" | "e" => {
                cli.write("Saving Board to Disk...")?;
                return Ok(());
            }
            "clear" => {
                print!("\x1B[2J");
            }
            command => {
                cli.write(format!("ERROR: Unknown command -> '{}'", command))?;
            }
        }
    }
}

async fn run() -> anyhow::Result<()> {
    let mut cli = Cli::new();
    let mut db = Database::new();
    // let mut db = Database::restore(".db/wal")?;
    let board_store = db.create::<U32, Board>("Boards").await?;
    let ticket_store = db.create::<U64, Ticket>("Tickets").await?;
    let board_tickets = db
        .create_index("Board Ticket Index", &ticket_store, |value| {
            Some(&value.board)
        })
        .await?;

    // let mut total_ticket_count = db.aggragate::<u32, Ticket, TotalTickets>("Total Tickets")?;
    db.restore(".db").await?;

    println!("\nTask Cli Database!\n");

    loop {
        if let Some(board) = board(&mut cli, &board_store).await? {
            tickets(&mut cli, board, &ticket_store, &board_tickets).await?;
        } else {
            break;
        }

        let buffer = cli.read_line("Would you like to exit?")?;
        if buffer == "yes" {
            break;
        }
    }

    db.dump(".db").await?;

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
