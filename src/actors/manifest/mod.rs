use std::{
    cmp::Ordering,
    fmt,
    io::{Read, Write},
    path::PathBuf,
};

use super::fs::{DbFile, FileSystemFacade, OpenFileOptions};

fn validate_manifest_file_name(name: &str) -> anyhow::Result<u64> {
    if !name.starts_with("MANIFEST-") {
        anyhow::bail!("File name does not start with 'MANIFEST-'. Found {}", name)
    } else if let Ok(num) = name.trim_start_matches("MANIFEST-").parse::<u64>() {
        Ok(num)
    } else {
        anyhow::bail!("File name does not have a valid number. Found {}", name)
    }
}

pub struct ManifestLog {
    current: Option<String>,
    logs: Vec<Current>,
}

impl ManifestLog {
    /// Recover the manifest file.
    ///
    /// - Validate that it contains log files and a current file.
    /// - Validate that no more then 1 current file exists
    /// - Validate that no manifest log files missing
    ///
    /// Return pointer to log files and current file upon success
    pub async fn recover(
        fs: &FileSystemFacade,
        manifest: &'static str,
        current: &'static str,
    ) -> anyhow::Result<Self> {
        let mut contents = String::new();
        let mut file = fs
            .open(OpenFileOptions::new(manifest).read().create())
            .await?;
        file.read_to_string(&mut contents)?;

        let mut current_opt = None;
        let mut files = vec![];
        for line in contents.split_whitespace() {
            let name = line.trim();
            if name == current {
                if current_opt.is_none() {
                    current_opt = Some(name.to_string());
                } else {
                    anyhow::bail!("Found two {} files inside of Manifest Log", current);
                }
            } else {
                let id = validate_manifest_file_name(name)?;
                files.push(Current::new(name.to_string(), id));
            }
        }

        files.sort();
        let mut i = 1;
        for file in &files {
            if file.sequence_num == i {
                i += 1;
            } else {
                anyhow::bail!(
                    "Missing sequence number {}. Found Manifest Log {:?} instead",
                    i,
                    file
                );
            }
        }

        Ok(Self {
            current: current_opt,
            logs: files,
        })
    }

    /// Validate that the current pass into the function, currently points at the
    /// latest log event that is recorded in a logs list.
    pub fn points_at_latest(&self, current: &Current) -> anyhow::Result<()> {
        if let Some(log) = self.logs.last() {
            if log == current {
                Ok(())
            } else {
                anyhow::bail!(
                    "Current does not point at latest. Expected {} but found {}",
                    current,
                    log
                )
            }
        } else {
            anyhow::bail!("Failed to find any logs because none exist");
        }
    }

    /// Validate that no logs exist inside of the MANIFEST system. Check that the
    /// following is true:
    /// 1. No more then 1 log exists in the system
    /// 2. If 1 log exists, it is empty
    /// Returns ok, if no events have been logged in the system
    pub async fn no_log_events_exist(&self, fs: &FileSystemFacade) -> anyhow::Result<()> {
        if self.logs.len() > 1 {
            anyhow::bail!(
                "MANIFEST system expected there to be no log files but we found {:?}",
                self.logs
            );
        }
        if let Some(log) = self.logs.first() {
            let mut buf = [0; 1024];
            let mut file = fs.read_file(&log.pointer).await?;
            let read = file.read(&mut buf)?;
            if read == 0 {
                Ok(())
            } else {
                anyhow::bail!("Manifest system has written to a log file {} however, we expect it not to exist", log)
            }
        } else {
            Ok(())
        }
    }

    /// Create the first MANIFEST log file. Fails if it already exists. Return
    /// a reference to the newly pointer.
    pub async fn create_first_event_log_file(
        &mut self,
        fs: &FileSystemFacade,
    ) -> anyhow::Result<&Current> {
        let pointer = "MANIFEST-1".to_string();
        fs.open(OpenFileOptions::new(&pointer).create_new()).await?;
        let current = Current::new(pointer, 1);
        self.logs.push(current);
        Ok(self.logs.first().unwrap())
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct Current {
    pointer: String,
    sequence_num: u64,
}

impl Current {
    pub fn new(pointer: String, sequence_num: u64) -> Self {
        Self {
            pointer,
            sequence_num,
        }
    }

    pub async fn recover(fs: &FileSystemFacade, path: impl Into<PathBuf>) -> anyhow::Result<Self> {
        let mut contents = String::new();
        let mut file = fs.read_file(path).await?;
        file.read_to_string(&mut contents)?;

        contents = contents.trim().to_string();
        let sequence_num = validate_manifest_file_name(&contents)?;

        Ok(Self::new(contents, sequence_num))
    }

    pub async fn create(
        fs: &FileSystemFacade,
        log: &Current,
        path: impl Into<PathBuf>,
    ) -> anyhow::Result<Self> {
        let mut file = fs
            .open(OpenFileOptions::new(path).create_new().write())
            .await?;
        file.write_all(log.pointer.as_bytes())?;
        Ok(Self {
            pointer: log.pointer.clone(),
            sequence_num: log.sequence_num,
        })
    }
}

impl Ord for Current {
    fn cmp(&self, other: &Self) -> Ordering {
        self.sequence_num.cmp(&other.sequence_num)
    }
}

impl PartialOrd for Current {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.pointer.partial_cmp(&other.pointer) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        self.sequence_num.partial_cmp(&other.sequence_num)
    }
}

impl fmt::Display for Current {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.pointer)
    }
}

pub struct Manifest {
    log: DbFile,
}

impl Manifest {
    pub async fn recover(fs: FileSystemFacade) -> anyhow::Result<Self> {
        // TODO: Rebase filesystem with the provided `base`. No more joining!
        let mut manifest = ManifestLog::recover(&fs, "MANIFEST", "CURRENT").await?;
        let current = if let Some(name) = &manifest.current {
            Current::recover(&fs, name).await?
        } else {
            // Current doesn't exist
            manifest.no_log_events_exist(&fs).await?;
            let current = manifest.create_first_event_log_file(&fs).await?;
            Current::create(&fs, current, "CURRENT").await?
        };

        manifest.points_at_latest(&current)?;

        // TODO: We need a class to encapsulate writing to an event log
        let log = fs
            .open(OpenFileOptions::new(current.pointer).write())
            .await?;

        // TODO: Read all log files and capture all of the events
        //       validate that all events are in correct order

        Ok(Self { log })
    }
}
