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
    pub fn in_memory(input: impl Into<InMemoryFs>) -> Self {
        Self {
            base_path: PathBuf::from("/"),
            filesystem: FileSystemImpl::Memory(input.into()),
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

    fn create_dir_if_not_exist(&mut self, path: impl Into<PathBuf>) -> std::io::Result<()> {
        let absolute_path = self.base_path.join(path.into());
        match &mut self.filesystem {
            FileSystemImpl::Memory(memory) => {
                memory.create_dir(absolute_path)?;
            }
            FileSystemImpl::System(system) => {
                if let Err(err) = system.create_dir(absolute_path) {
                    if err.kind() != std::io::ErrorKind::AlreadyExists {
                        return Err(err);
                    }
                }
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
        self.create_dir_if_not_exist(path)?;
        Ok(())
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
    fn succeed_open_base_dir_in_memory() {
        let mem = FileSystem::in_memory(());
        succeed_open_base_dir(mem);
    }

    #[test]
    fn succeed_open_base_dir_in_system() {
        let mem = FileSystem::system(temp_dir().join("ex1"));
        succeed_open_base_dir(mem);
    }

    fn succeed_open_base_dir(mut fs: FileSystem) {
        assert!(fs.open_base_dir().is_ok());
        assert!(fs.open_base_dir().is_ok());
    }

    #[test]
    fn fail_open_file_with_no_write_and_truncate_in_memory() {
        let mem = FileSystem::in_memory(());
        fail_open_file_with_invalid_permissions(mem);
    }

    #[test]
    fn fail_open_file_with_no_write_and_truncate_in_system() {
        let mem = FileSystem::system(temp_dir().join("ex2"));
        fail_open_file_with_invalid_permissions(mem);
    }

    fn fail_open_file_with_invalid_permissions(mut mem: FileSystem) {
        assert!(mem
            .open_file(OpenFileOptions::new("file").truncate())
            .is_err());
        assert!(mem
            .open_file(OpenFileOptions::new("file").create())
            .is_err());
    }

    #[test]
    fn fail_to_open_cause_create_not_set_in_memory() {
        let mem = FileSystem::in_memory(());
        fail_to_open_cause_create_not_set(mem);
    }

    #[test]
    fn fail_to_open_cause_create_not_set_in_system() {
        let mem = FileSystem::system(temp_dir().join("ex3"));
        fail_to_open_cause_create_not_set(mem);
    }

    fn fail_to_open_cause_create_not_set(mut mem: FileSystem) {
        assert!(mem.open_file(OpenFileOptions::new("file").read()).is_err());
    }

    #[test]
    fn fail_open_file_with_create_new_because_exists_in_memory() {
        let mem = FileSystem::in_memory(());
        fail_open_file_with_create_new_because_exists(mem);
    }

    #[test]
    fn fail_open_file_with_create_new_because_exists_in_system() {
        let mem = FileSystem::system(temp_dir().join("ex4"));
        fail_open_file_with_create_new_because_exists(mem);
    }

    fn fail_open_file_with_create_new_because_exists(mut mem: FileSystem) {
        assert!(mem.open_base_dir().is_ok());
        assert!(mem
            .open_file(OpenFileOptions::new("file").create().write())
            .is_ok());
        assert!(mem
            .open_file(OpenFileOptions::new("file").create_new().write())
            .is_err());
    }

    #[test]
    fn fail_open_file_because_it_is_directory_in_memory() {
        let mem = FileSystem::in_memory(());
        fail_open_file_because_it_is_directory(mem);
    }

    #[test]
    fn fail_open_file_because_it_is_directory_in_system() {
        let mem = FileSystem::system(temp_dir().join("ex5"));
        fail_open_file_because_it_is_directory(mem);
    }

    fn fail_open_file_because_it_is_directory(mut mem: FileSystem) {
        assert!(mem.open_base_dir().is_ok());
        assert!(mem.create_dir_if_not_exist("dir").is_ok());
        assert!(mem.open_file(OpenFileOptions::new("dir").create()).is_err());
    }

    #[test]
    fn fail_to_open_file_because_no_parent_file_in_memory() {
        let mem = FileSystem::in_memory(());
        fail_to_open_file_because_no_parent_file(mem);
    }

    #[test]
    fn fail_to_open_file_because_no_parent_file_in_system() {
        let mem = FileSystem::system(temp_dir().join("ex6"));
        fail_to_open_file_because_no_parent_file(mem);
    }

    fn fail_to_open_file_because_no_parent_file(mut mem: FileSystem) {
        assert!(mem.open_base_dir().is_ok());
        assert!(mem
            .open_file(OpenFileOptions::new("valid").write().create())
            .is_ok());
        assert!(mem
            .open_file(OpenFileOptions::new("not/valid").write().create())
            .is_err());
        assert!(mem
            .open_file(OpenFileOptions::new("still/not/valid").write().create())
            .is_err());
    }

    #[test]
    fn fail_to_open_parent_file_not_dir_in_memory() {
        let mem = FileSystem::in_memory(());
        fail_to_open_parent_file_not_dir(mem);
    }

    #[test]
    fn fail_to_open_parent_file_not_dir_in_system() {
        let mem = FileSystem::system(temp_dir().join("ex7"));
        fail_to_open_parent_file_not_dir(mem);
    }

    fn fail_to_open_parent_file_not_dir(mut mem: FileSystem) {
        assert!(mem.open_base_dir().is_ok());
        assert!(mem
            .open_file(OpenFileOptions::new("dir_file").write().create())
            .is_ok());
        assert!(mem
            .open_file(OpenFileOptions::new("dir_file/file").write().create())
            .is_err());
    }

    #[test]
    fn succeed_open_file_with_no_read_or_write_in_memory() {
        let mem = FileSystem::in_memory(());
        succeed_open_file_with_no_read_or_write(mem);
    }

    #[test]
    fn succeed_open_file_with_no_read_or_write_in_system() {
        let mem = FileSystem::system(temp_dir().join("ex12"));
        succeed_open_file_with_no_read_or_write(mem);
    }

    fn succeed_open_file_with_no_read_or_write(mut mem: FileSystem) {
        assert!(mem.open_base_dir().is_ok());
        assert!(mem
            .open_file(OpenFileOptions::new("file").write().create())
            .is_ok());

        let mut buffer: [u8; 1] = [1_u8; 1];
        let mut file = mem.open_file(OpenFileOptions::new("file").read()).unwrap();
        assert!(file.write(&buffer).is_err());

        let mut file = mem.open_file(OpenFileOptions::new("file").write()).unwrap();
        assert!(file.read(&mut buffer).is_err());
    }

    #[test]
    fn succeed_open_file_with_truncate_option_in_memory() {
        let mem = FileSystem::in_memory(());
        succeed_open_file_with_truncate_option(mem);
    }

    #[test]
    fn succeed_open_file_with_truncate_option_in_system() {
        let mem = FileSystem::system(temp_dir().join("ex11"));
        succeed_open_file_with_truncate_option(mem);
    }

    fn succeed_open_file_with_truncate_option(mut mem: FileSystem) {
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
    fn succeed_open_with_append_option_in_memory() {
        let mem = FileSystem::in_memory(());
        succeed_open_with_append_option(mem);
    }

    #[test]
    fn succeed_open_with_append_option_in_system() {
        let mem = FileSystem::system(temp_dir().join("ex10"));
        succeed_open_with_append_option(mem);
    }

    fn succeed_open_with_append_option(mut mem: FileSystem) {
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
    fn succeed_in_writing_more_then_max_buffer_in_memory() {
        let mem = FileSystem::in_memory(());
        succeed_in_writing_more_then_max_buffer(mem);
    }

    #[test]
    fn succeed_in_writing_more_then_max_buffer_in_system() {
        let mem = FileSystem::system(temp_dir().join("ex9"));
        succeed_in_writing_more_then_max_buffer(mem);
    }

    fn succeed_in_writing_more_then_max_buffer(mut mem: FileSystem) {
        assert!(mem.open_base_dir().is_ok());
        let options = OpenFileOptions::new("file").create().write().read();

        let buf_write = [1_u8; 16000];
        let mut file1 = mem.open_file(options.clone().truncate()).unwrap();
        assert!(file1.write_all(&buf_write).is_ok());
        assert!(file1.flush().is_ok());

        let mut buf_read = Vec::new();
        let mut file = mem.open_file(options).unwrap();
        assert!(file.read_to_end(&mut buf_read).is_ok());
        assert_eq!(buf_read.len(), buf_write.len());
        assert_eq!(buf_read, buf_write);
    }

    #[test]
    fn succeed_create_dir_if_not_exist_in_memory() {
        let mem = FileSystem::in_memory(());
        succeed_create_dir_if_not_exist(mem);
    }

    #[test]
    fn succeed_create_dir_if_not_exist_in_system() {
        let mem = FileSystem::system(temp_dir().join("ex8"));
        succeed_create_dir_if_not_exist(mem);
    }

    fn succeed_create_dir_if_not_exist(mut mem: FileSystem) {
        assert!(mem.open_base_dir().is_ok());
        assert!(mem.create_dir_if_not_exist("new-dir").is_ok());
        assert!(mem.create_dir_if_not_exist("new-dir").is_ok());
        assert!(mem
            .open_file(OpenFileOptions::new("new-file").write().create())
            .is_ok());
        assert!(mem.create_dir_if_not_exist("new-file").is_err());
    }
}
