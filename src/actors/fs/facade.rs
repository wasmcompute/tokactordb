use std::path::PathBuf;

use tokactor::ActorRef;

use crate::FileSystem;

use super::messages::{OpenBaseDir, ValidateOrCreateDir};

pub struct FileSystemFacade {
    inner: ActorRef<FileSystem>,
}

impl FileSystemFacade {
    pub fn new(inner: ActorRef<FileSystem>) -> Self {
        Self { inner }
    }

    pub async fn open_base_dir(&self) -> anyhow::Result<()> {
        let result = self.inner.ask(OpenBaseDir).await?;
        result?;
        Ok(())
    }

    pub async fn validate_or_create_dir(&self, path: impl Into<PathBuf>) -> anyhow::Result<()> {
        let path = path.into();
        let result = self.inner.ask(ValidateOrCreateDir(path)).await?;
        result
    }
}
