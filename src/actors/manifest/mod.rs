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

pub struct ManifestFileMap {
    current: Option<String>,
    logs: Vec<Current>,
    file: DbFile,
}

impl ManifestFileMap {
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
            .open(OpenFileOptions::new(manifest).read().write().create())
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
        let mut i = 0;
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
            file,
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

    /// Create the manifest file system. If files already exist, then validate they
    /// have no content inside of them. Otherwise, create the files needed to
    /// operate the system so we can guarntee they exist after this point.
    pub async fn create_manifest_system(
        &mut self,
        fs: &FileSystemFacade,
    ) -> anyhow::Result<Current> {
        let opts = OpenFileOptions::new("CURRENT").create_new().write();

        let mut current_file = fs.open(opts.clone()).await?;
        let _ = fs.open(opts.path("MANIFEST-0")).await?;
        self.file.write_all(b"CURRENT\nMANIFEST-0")?;
        self.file.flush()?;
        current_file.write_all(b"MANIFEST-0")?;
        current_file.flush()?;

        let current = Current::new("MANIFEST-0", 0);
        self.current = Some(current.pointer.clone());
        self.logs.push(current.clone());

        Ok(current)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Current {
    pointer: String,
    sequence_num: u64,
}

impl Current {
    pub fn new(pointer: impl ToString, sequence_num: u64) -> Self {
        Self {
            pointer: pointer.to_string(),
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
        let mut manifest = ManifestFileMap::recover(&fs, "MANIFEST", "CURRENT").await?;
        let current = if let Some(name) = &manifest.current {
            Current::recover(&fs, name).await?
        } else {
            // Current doesn't exist
            manifest.no_log_events_exist(&fs).await?;
            manifest.create_manifest_system(&fs).await?
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

#[cfg(test)]
mod tests {
    use tokactor::Actor;

    use crate::{
        actors::{
            fs::{FileSystemFacade, OpenFileOptions},
            manifest::{Current, Manifest, ManifestFileMap},
        },
        FileSystem,
    };

    use super::validate_manifest_file_name;

    const M: &str = "MANIFEST";
    const C: &str = "CURRENT";

    #[test]
    fn invalid_manifest_file_names() {
        assert!(validate_manifest_file_name("name").is_err());
        assert!(validate_manifest_file_name("MANIFEST-name").is_err());
        assert!(validate_manifest_file_name("MANIFEST-123abc").is_err());
        assert!(validate_manifest_file_name("MANIFEST-1!").is_err());
        assert!(validate_manifest_file_name("MANIFEST-1\n").is_err());
        assert!(validate_manifest_file_name("MANIFEST-1\t").is_err());
        assert!(validate_manifest_file_name("\tMANIFEST-1").is_err());
        assert!(validate_manifest_file_name("MANIFEST--1").is_err());
        assert!(
            validate_manifest_file_name(&format!("MANIFEST-{}", u64::MAX as u128 + 1)).is_err()
        );
    }

    #[test]
    fn valid_manifest_file_names() {
        assert_eq!(validate_manifest_file_name("MANIFEST-123").unwrap(), 123);
        assert_eq!(validate_manifest_file_name("MANIFEST-1").unwrap(), 1);
        assert_eq!(
            validate_manifest_file_name(&format!("MANIFEST-{}", u64::MIN)).unwrap(),
            u64::MIN
        );
        assert_eq!(
            validate_manifest_file_name(&format!("MANIFEST-{}", u64::MAX)).unwrap(),
            u64::MAX
        );
    }

    async fn init_fs(map: &[(impl AsRef<str>, impl AsRef<str>)]) -> FileSystemFacade {
        let in_memory = FileSystem::in_memory(map);
        let in_memory_actor = in_memory.start();
        let fs = FileSystemFacade::new(in_memory_actor);
        fs.open_base_dir().await.unwrap();
        fs
    }

    /***************************************************************************
     * Testing Current::recover
     **************************************************************************/

    #[tokio::test]
    async fn recover_current_file() {
        let fs = init_fs(&[("/CURRENT", "MANIFEST-0")]).await;
        let current = Current::recover(&fs, C).await.unwrap();
        assert_eq!(current, Current::new("MANIFEST-0", 0));
    }

    #[tokio::test]
    async fn fail_recovery_of_current_file() {
        let fs = init_fs(&[("/CURRENT", "MANIFEST-NO")]).await;
        assert!(Current::recover(&fs, C).await.is_err());
    }

    /***************************************************************************
     * Testing ManifestFileMap::recover
     **************************************************************************/

    #[tokio::test]
    async fn recover_empty_manifest_file_map() {
        let fs = init_fs(&[] as &[(&str, &str)]).await;
        let map = ManifestFileMap::recover(&fs, M, C).await.unwrap();
        assert_eq!(map.logs, Vec::new());
        assert_eq!(map.current, None);
        assert!(fs.read_file(M).await.is_ok()); // manifest file created
        assert!(fs.read_file(C).await.is_err()); // current file not created
    }

    #[tokio::test]
    async fn fail_to_recover_manifest_file_map_because_2_current_referenced() {
        let map = [("/MANIFEST", "CURRENT\nCURRENT")];
        let fs = init_fs(&map).await;
        assert!(ManifestFileMap::recover(&fs, M, C).await.is_err());
    }

    #[tokio::test]
    async fn fail_to_recover_manifest_file_map_because_incorrect_file_name_found() {
        let map = [("/MANIFEST", "MANIFEST-NOT")];
        let fs = init_fs(&map).await;
        assert!(ManifestFileMap::recover(&fs, M, C).await.is_err());
    }

    #[tokio::test]
    async fn fail_to_recover_manifest_file_map_because_missing_sequence_logs() {
        let map = [("/MANIFEST", "MANIFEST-0\nMANIFEST-1\nMANIFEST-10")];
        let fs = init_fs(&map).await;
        assert!(ManifestFileMap::recover(&fs, M, C).await.is_err());
    }

    #[tokio::test]
    async fn recover_partial_manifest_file_map() {
        let map = [("/MANIFEST", "CURRENT")];
        let fs = init_fs(&map).await;

        let map = ManifestFileMap::recover(&fs, M, C).await.unwrap();
        assert_eq!(map.logs, Vec::new());
        assert_eq!(map.current, Some("CURRENT".to_string()));
        assert!(fs.read_file(M).await.is_ok()); // manifest file created
        assert!(fs.read_file(C).await.is_err()); // current file not created
    }

    #[tokio::test]
    async fn recover_partial_manifest_file_map_and_current() {
        let map = [("/MANIFEST", "CURRENT"), ("/CURRENT", "")];
        let fs = init_fs(&map).await;

        let map = ManifestFileMap::recover(&fs, M, C).await.unwrap();
        assert_eq!(map.logs, Vec::new());
        assert_eq!(map.current, Some("CURRENT".to_string()));
        assert!(fs.read_file(M).await.is_ok()); // manifest file created
        assert!(fs.read_file(C).await.is_ok()); // current file created
    }

    #[tokio::test]
    async fn recover_manifest_file_map() {
        let map = [
            ("/MANIFEST", "CURRENT\nMANIFEST-0\nMANIFEST-1"),
            ("/CURRENT", "MANIFEST-1"),
            ("/MANIFEST-0", ""),
            ("/MANIFEST-1", ""),
        ];
        let fs = init_fs(&map).await;

        let map = ManifestFileMap::recover(&fs, M, C).await.unwrap();
        assert_eq!(
            map.logs,
            vec![Current::new("MANIFEST-0", 0), Current::new("MANIFEST-1", 1),]
        );
        assert_eq!(map.current, Some("CURRENT".to_string()));
        assert!(fs.read_file(M).await.is_ok()); // manifest file created
        assert!(fs.read_file(C).await.is_ok()); // current file created
    }

    /***************************************************************************
     * Testing ManifestFileMap::recover
     **************************************************************************/

    #[tokio::test]
    async fn recover_new_manifest_system() {
        let fs = init_fs(&[] as &[(&str, &str)]).await;
        let _ = Manifest::recover(fs.clone()).await.unwrap();

        assert_eq!(fs.read_full_file(M).await.unwrap(), "CURRENT\nMANIFEST-0");
        assert_eq!(fs.read_full_file(C).await.unwrap(), "MANIFEST-0");
        assert_eq!(fs.read_full_file("MANIFEST-0").await.unwrap(), "");
        assert!(fs.read_full_file("MANIFEST-1").await.is_err());
    }

    #[tokio::test]
    async fn recover_manifest_system() {
        let map = [
            ("/MANIFEST", "CURRENT\nMANIFEST-0\nMANIFEST-1"),
            ("/CURRENT", "MANIFEST-1"),
            ("/MANIFEST-0", ""),
            ("/MANIFEST-1", ""),
        ];
        let fs = init_fs(&map).await;
        let _ = Manifest::recover(fs.clone()).await.unwrap();

        assert_eq!(fs.read_full_file(M).await.unwrap(), map[0].1);
        assert_eq!(fs.read_full_file(C).await.unwrap(), map[1].1);
        assert_eq!(fs.read_full_file("MANIFEST-0").await.unwrap(), map[2].1);
        assert_eq!(fs.read_full_file("MANIFEST-1").await.unwrap(), map[3].1);
    }
}
