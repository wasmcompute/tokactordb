use std::path::PathBuf;

use tokactor::ActorRef;

use crate::FileSystem;

use super::{
    messages::{OpenBaseDir, OpenFileOptions, ValidateOrCreateDir},
    DbFile,
};

pub struct FileSystemFacade {
    rebase: Option<PathBuf>,
    inner: ActorRef<FileSystem>,
}

impl FileSystemFacade {
    pub fn new(inner: ActorRef<FileSystem>) -> Self {
        Self {
            rebase: None,
            inner,
        }
    }

    pub async fn open_base_dir(&self) -> anyhow::Result<()> {
        let result = self.inner.ask(OpenBaseDir).await?;
        result?;
        Ok(())
    }

    pub async fn validate_or_create_dir(&self, path: impl Into<PathBuf>) -> anyhow::Result<()> {
        let path = path.into();
        let path = Self::do_rebase(self.rebase.as_ref(), path);
        self.inner.ask(ValidateOrCreateDir(path)).await?
    }

    pub async fn read_file(&self, path: impl Into<PathBuf>) -> anyhow::Result<DbFile> {
        let path = path.into();
        let path = Self::do_rebase(self.rebase.as_ref(), path);
        self.open(OpenFileOptions::new(path).read()).await
    }

    pub async fn open(&self, mut options: OpenFileOptions) -> anyhow::Result<DbFile> {
        options.path = Self::do_rebase(self.rebase.as_ref(), options.path);
        self.inner.ask(options).await?
    }

    pub fn rebase(&self, path: impl Into<PathBuf>) -> FileSystemFacade {
        let path = path.into();
        let rebase = Self::do_rebase(self.rebase.as_ref(), path);
        Self {
            rebase: Some(rebase),
            inner: self.inner.clone(),
        }
    }

    fn do_rebase(base: Option<&PathBuf>, path: PathBuf) -> PathBuf {
        base.map(|existing| existing.join(&path)).unwrap_or(path)
    }
}
