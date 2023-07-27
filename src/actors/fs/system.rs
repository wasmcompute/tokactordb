use std::{
    io,
    path::{Path, PathBuf},
};

use super::{messages::OpenFileOptions, DbFile};

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

    pub fn open_file(&self, options: OpenFileOptions) -> io::Result<DbFile> {
        let mut opt = std::fs::OpenOptions::new();
        opt.read(options.read);
        opt.write(options.write);
        opt.truncate(options.truncate);
        opt.create(options.create);
        opt.create_new(options.create_new);
        opt.append(options.append);

        let file = opt.open(options.path)?;
        let file = DbFile::system(file);
        Ok(file)
    }
}
