use std::path::PathBuf;

use tokactor::{Actor, Ask, Ctx, Scheduler};

use self::{
    memory::InMemoryFs,
    messages::{OpenBaseDir, ValidateOrCreateDir},
    system::FsSystem,
};

mod facade;
mod file;
mod memory;
mod messages;
mod node;
mod system;

pub use facade::FileSystemFacade;
pub use messages::OpenFileOptions;
pub use node::DbFile;

#[derive(Debug)]
enum FileSystemImpl {
    Memory(InMemoryFs),
    System(FsSystem),
}

#[derive(Debug)]
pub struct FileSystem {
    base_path: PathBuf,
    filesystem: FileSystemImpl,
}

impl Actor for FileSystem {}

impl FileSystem {
    pub fn in_memory() -> Self {
        Self {
            base_path: PathBuf::from("/"),
            filesystem: FileSystemImpl::Memory(InMemoryFs::default()),
        }
    }

    pub fn system(base: impl Into<PathBuf>) -> Self {
        Self {
            base_path: base.into(),
            filesystem: FileSystemImpl::System(FsSystem::default()),
        }
    }

    fn open_base_dir(&mut self) -> anyhow::Result<()> {
        match &mut self.filesystem {
            FileSystemImpl::Memory(ref mut memory) => {
                memory.create_dir(&self.base_path)?;
                Ok(())
            }
            FileSystemImpl::System(system) => system.open_base_dir(&self.base_path),
        }
    }

    fn create_dir_if_not_exist(&mut self, path: impl Into<PathBuf>) -> anyhow::Result<()> {
        let absolute_path = self.base_path.join(path.into());
        match &mut self.filesystem {
            FileSystemImpl::Memory(memory) => {
                memory.create_dir(absolute_path)?;
            }
            FileSystemImpl::System(system) => {
                system.create_dir(absolute_path)?;
            }
        }
        Ok(())
    }

    fn open_file(&mut self, mut options: OpenFileOptions) -> anyhow::Result<DbFile> {
        options.path = self.base_path.join(options.path);
        let output = match &mut self.filesystem {
            FileSystemImpl::Memory(memory) => memory.open_file(options),
            FileSystemImpl::System(system) => system.open_file(options),
        }?;
        Ok(output)
    }
}

impl Ask<OpenBaseDir> for FileSystem {
    type Result = anyhow::Result<()>;

    fn handle(&mut self, _: OpenBaseDir, _: &mut Ctx<Self>) -> Self::Result {
        self.open_base_dir()
    }

    fn scheduler() -> Scheduler {
        Scheduler::Blocking
    }
}

impl Ask<ValidateOrCreateDir> for FileSystem {
    type Result = anyhow::Result<()>;

    fn handle(
        &mut self,
        ValidateOrCreateDir(path): ValidateOrCreateDir,
        _: &mut Ctx<Self>,
    ) -> Self::Result {
        self.create_dir_if_not_exist(path)
    }

    fn scheduler() -> Scheduler {
        Scheduler::Blocking
    }
}

impl Ask<OpenFileOptions> for FileSystem {
    type Result = anyhow::Result<DbFile>;

    fn handle(&mut self, options: OpenFileOptions, _: &mut Ctx<Self>) -> Self::Result {
        self.open_file(options)
    }

    fn scheduler() -> Scheduler {
        Scheduler::Blocking
    }
}

#[cfg(test)]
mod tests {
    use std::{
        env::temp_dir,
        io::{Read, Write},
    };

    use crate::{actors::fs::OpenFileOptions, FileSystem};

    #[test]
    fn succeed_open_base_dir() {
        let mut mem = FileSystem::in_memory();
        assert!(mem.open_base_dir().is_ok());
        assert!(mem.open_base_dir().is_ok())
    }

    #[test]
    fn fail_open_file_with_no_write_and_truncate() {
        let mut mem = FileSystem::in_memory();
        let output = mem.open_file(OpenFileOptions::new("file").truncate());
        assert!(output.is_err());
    }

    #[test]
    fn fail_to_open_cause_create_not_set() {
        let mut mem = FileSystem::in_memory();
        assert!(mem.open_file(OpenFileOptions::new("file").read()).is_err());
    }

    #[test]
    fn fail_open_file_with_create_new_because_exists() {
        let mut mem = FileSystem::in_memory();
        assert!(mem.open_base_dir().is_ok());
        assert!(mem
            .open_file(OpenFileOptions::new("file").create_new())
            .is_ok());
        assert!(mem
            .open_file(OpenFileOptions::new("file").create_new())
            .is_err());
    }

    #[test]
    fn fail_open_file_because_it_is_directory() {
        let mut mem = FileSystem::in_memory();
        assert!(mem.create_dir_if_not_exist("dir").is_ok());
        assert!(mem.open_file(OpenFileOptions::new("dir").create()).is_err());
    }

    #[test]
    fn fail_to_open_file_because_no_parent_file() {
        let mut mem = FileSystem::in_memory();
        assert!(mem.open_base_dir().is_ok());
        assert!(mem
            .open_file(OpenFileOptions::new("valid").create())
            .is_err());
        assert!(mem
            .open_file(OpenFileOptions::new("not/valid").create())
            .is_err());
        assert!(mem
            .open_file(OpenFileOptions::new("still/not/valid").create())
            .is_err());
    }

    #[test]
    fn fail_to_open_parent_file_not_dir() {
        let mut mem = FileSystem::in_memory();
        assert!(mem.open_base_dir().is_ok());
        assert!(mem
            .open_file(OpenFileOptions::new("dir_file").create())
            .is_ok());
        assert!(mem
            .open_file(OpenFileOptions::new("dir_file/file").create())
            .is_err());
    }

    #[test]
    fn succeed_open_file_with_no_read_or_write() {
        let mut mem = FileSystem::in_memory();
        assert!(mem.open_base_dir().is_ok());
        assert!(mem.open_file(OpenFileOptions::new("file").create()).is_ok());
        let mut file = mem.open_file(OpenFileOptions::new("file")).unwrap();

        let mut buffer: [u8; 1] = [1_u8; 1];
        assert!(file.read(&mut buffer).is_err());
        assert!(file.write(&buffer).is_err());
    }

    #[test]
    fn succeed_open_file_with_truncate_option() {
        let mut mem = FileSystem::in_memory();
        assert!(mem.open_base_dir().is_ok());
        let mut file = mem
            .open_file(OpenFileOptions::new("file").create().write())
            .unwrap();

        assert!(file.write_all(b"hello").is_ok());
        let mut file2 = mem
            .open_file(OpenFileOptions::new("file").truncate().write().read())
            .unwrap();

        let mut buffer: [u8; 1] = [1_u8; 1];
        assert_eq!(file2.read(&mut buffer).ok(), Some(0));
    }

    #[test]
    fn succeed_open_with_append_option() {
        let mut mem = FileSystem::in_memory();
        assert!(mem.open_base_dir().is_ok());
        let options = OpenFileOptions::new("file").create().write().read();

        let mut file1 = mem.open_file(options.clone().truncate()).unwrap();
        assert!(file1.write_all(b"hello").is_ok());
        assert!(file1.flush().is_ok());

        let mut file2 = mem.open_file(options.append()).unwrap();
        assert!(file2.write_all(b" world").is_ok());
        assert!(file2.flush().is_ok());

        let mut str = String::new();
        assert_eq!(file1.read_to_string(&mut str).ok(), Some(6));
        assert_eq!(&str, " world");

        let mut str = String::new();
        assert_eq!(file2.read_to_string(&mut str).ok(), Some(0));
        assert_eq!(&str, "");
    }

    #[test]
    fn succeed_create_dir_if_not_exist() {
        let mut mem = FileSystem::in_memory();
        assert!(mem.open_base_dir().is_ok());
        assert!(mem.create_dir_if_not_exist("new-dir").is_ok());
        assert!(mem.create_dir_if_not_exist("new-dir").is_ok());
        assert!(mem
            .open_file(OpenFileOptions::new("new-file").create())
            .is_ok());
        assert!(mem.create_dir_if_not_exist("new-file").is_err());
    }
}
