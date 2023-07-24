use std::path::{Path, PathBuf};

#[derive(Debug, Default)]
pub struct FsSystem {}

impl FsSystem {
    pub fn open_base_dir(&self, path: impl AsRef<Path>) -> anyhow::Result<()> {
        let path = path.as_ref();
        if !path.exists() {
            std::fs::create_dir_all(path)?;
            Ok(())
        } else if path.is_file() {
            anyhow::bail!("Can't restore because this is a file and not a directory")
        } else {
            Ok(())
        }
    }

    pub fn create_dir(&self, path: PathBuf) -> anyhow::Result<()> {
        std::fs::create_dir(path)?;
        Ok(())
    }
}
