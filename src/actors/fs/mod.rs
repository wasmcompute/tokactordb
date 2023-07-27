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
            base_path: PathBuf::new(),
            filesystem: FileSystemImpl::Memory(InMemoryFs::default()),
        }
    }

    pub fn system(base: impl Into<PathBuf>) -> Self {
        Self {
            base_path: base.into(),
            filesystem: FileSystemImpl::System(FsSystem::default()),
        }
    }

    fn open_base_dir(&self) -> anyhow::Result<()> {
        match &self.filesystem {
            FileSystemImpl::Memory(_) => {
                // no need to open this because we are in memory
                Ok(())
            }
            FileSystemImpl::System(system) => system.open_base_dir(&self.base_path),
        }
    }

    fn create_dir_if_not_exist(&mut self, path: PathBuf) -> anyhow::Result<()> {
        let absolute_path = self.base_path.join(path);
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
