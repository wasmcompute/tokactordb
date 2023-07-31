use std::{
    fmt::Display,
    io::{BufRead, StdinLock, StdoutLock, Write},
    time::SystemTime,
};

use tokactordb::{
    Aggregate, AggregateTree, Change, Database, FileSystem, SubTree, Tree, Update, ID, U32, U64,
};
use tracing::Level;

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
struct Board {
    name: String,
}

impl std::fmt::Display for Board {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
struct BoardV2 {
    name: String,
    name_len: usize,
}

impl BoardV2 {
    pub fn new(name: String) -> Self {
        let name_len: usize = name.len();
        Self { name, name_len }
    }
}

impl From<Board> for BoardV2 {
    fn from(value: Board) -> Self {
        Self::new(value.name)
    }
}

impl std::fmt::Display for BoardV2 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "'{}'.len() == {}", self.name, self.name_len)
    }
}

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
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

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
struct TicketBoardStatistics {
    total: usize,
    todos: usize,
    complete: usize,
    archived: usize,
}

impl Aggregate<U64, Ticket> for TicketBoardStatistics {
    fn observe(&mut self, change: Change<&U64, &Ticket>) {
        match change.update {
            Update::Set { old, new } => {
                match old {
                    Some(old_ticket) => {
                        // A old ticket was updated to a new ticket
                        if !old_ticket.completed && new.completed {
                            self.complete += 1;
                        } else if old_ticket.completed && !new.completed {
                            self.complete -= 1;
                        }
                        if !old_ticket.deleted && new.deleted {
                            self.archived += 1;
                        } else if old_ticket.deleted && !new.deleted {
                            self.archived -= 1;
                        }
                        if !old_ticket.completed && new.completed
                            || !old_ticket.deleted && new.deleted
                        {
                            self.todos -= 1;
                        } else if !new.completed
                            && !new.deleted
                            && (old_ticket.completed || old_ticket.deleted)
                        {
                            self.todos += 1;
                        }
                    }
                    None => {
                        // creating a new ticket
                        self.complete += if new.completed { 1 } else { 0 };
                        self.archived += if new.deleted { 1 } else { 0 };
                        self.todos += if new.completed || new.deleted { 0 } else { 1 };
                        self.total += 1;
                    }
                }
            }
            Update::Del { old } => {
                self.total -= 1;
                if old.completed {
                    self.complete -= 1;
                }
                if old.deleted {
                    self.archived -= 1;
                }
                if !old.completed && !old.deleted {
                    self.todos -= 1;
                }
            }
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    tracing_subscriber::fmt()
        .pretty()
        // all spans/events with a level higher than TRACE (e.g, info, warn, etc.)
        // will be written to stdout.
        .with_max_level(Level::TRACE)
        .with_writer(std::io::stdout)
        // sets this to be the default, global collector for this application.
        .init();

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

async fn board<'a>(
    cli: &mut Cli<'a>,
    board_store: &Tree<U32, BoardV2>,
) -> anyhow::Result<Option<ID<U32, Board>>> {
    cli.write("Choose a board!")?;
    loop {
        let cmd = cli.read_line("> ")?.to_lowercase();

        match cmd.as_str() {
            "create" => {
                let name = cli.read_line("Name > ")?;
                board_store.insert(BoardV2::new(name)).await?;
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
    board_stats: &AggregateTree<ID<U32, Board>, TicketBoardStatistics>,
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
                let str_id = cli.read_line("Index > ")?;
                let index = str_id.trim().parse::<usize>()?;

                let result = board_tickets
                    .mutate_by_index(board.clone(), index, move |ticket| {
                        if cmd == "complete" {
                            ticket.completed = true;
                        } else if cmd == "archive" {
                            ticket.deleted = true;
                        }
                    })
                    .await
                    .unwrap();

                if result.is_none() {
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
                let stat = board_stats.get(board.clone()).await?.unwrap_or_default();
                cli.write(format!(
                    "{} Todos, {} Complete, {} Archived",
                    stat.todos, stat.complete, stat.archived
                ))?;
                let list = board_tickets.list(board.clone()).await?;
                for ticket in list {
                    cli.write(format!("{}", ticket))?;
                }
                cli.write(format!("Total: {}", stat.total))?;
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
    // Create a filesystem for the database to use
    let filesystem = FileSystem::system("/");
    // Create a new database. Set it up by registering all the tables
    let db = Database::new(filesystem).await?;

    // Add the board table
    let board_store = db
        .create::<U32, Board>("Boards")?
        .migrate::<U32, BoardV2>()
        .await?
        .unwrap()
        .await?;
    // Add the Ticket table (that is linked to a board)
    let ticket_store = db.create::<U64, Ticket>("Tickets")?.unwrap().await?;

    // Create an index of tickets on a given board
    let board_tickets = db
        .create_index("Board Ticket Index", &ticket_store, |value| {
            Some(&value.board)
        })
        .await?;

    // Create some statistics for all tickets
    let board_statistics = db
        .create_aggregate(
            "Board Ticket Stats",
            &ticket_store,
            TicketBoardStatistics::default(),
            |value| Some(&value.board),
        )
        .await?;

    // Now with all of the database tables declared, we want to restore the database
    db.restore().await?;

    println!("\nTask Cli Database!\n");

    loop {
        if let Some(board) = board(&mut cli, &board_store).await? {
            tickets(
                &mut cli,
                board,
                &ticket_store,
                &board_tickets,
                &board_statistics,
            )
            .await?;
        } else {
            break;
        }

        let buffer = cli.read_line("Would you like to exit? ")?;
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
