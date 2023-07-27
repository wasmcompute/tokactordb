use std::{
    collections::HashMap,
    io::{self, ErrorKind},
    path::PathBuf,
};

use super::{file::FNode, messages::OpenFileOptions, DbFile};

#[derive(Debug)]
enum Ident {
    Dir(),
    File(FNode),
}

impl Ident {
    /// Returns `true` if the ident is [`Dir`].
    ///
    /// [`Dir`]: Ident::Dir
    #[must_use]
    fn is_dir(&self) -> bool {
        matches!(self, Self::Dir(..))
    }
}

#[derive(Debug, Default)]
pub struct InMemoryFs {
    file_system: HashMap<PathBuf, Ident>,
}

impl InMemoryFs {
    pub fn create_dir(&mut self, path: PathBuf) -> io::Result<()> {
        if let Some(entry) = self.file_system.get(&path) {
            match entry {
                Ident::Dir() => {}
                Ident::File(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        format!("{:?} already exists as a file", path),
                    ))
                }
            }
        } else {
            self.file_system.insert(path, Ident::Dir());
        }
        Ok(())
    }

    pub fn open_file(&mut self, options: OpenFileOptions) -> io::Result<DbFile> {
        if !options.write && options.truncate {
            return Err(io::Error::new(
                ErrorKind::InvalidInput,
                "Can't truncate file without write access",
            ));
        }
        if let Some(file) = self.file_system.get(&options.path) {
            if options.create_new {
                return Err(io::Error::new(ErrorKind::NotFound, "File already exists"));
            }
            match file {
                Ident::Dir() => {
                    return Err(io::Error::new(
                        ErrorKind::AlreadyExists,
                        "Path already exists as a directory",
                    ))
                }
                Ident::File(_) => todo!(),
            }
        }
        // File was not found, search the parent directory above to validate the path exists
        if let Some(parent) = options.path.parent() {
            if let Some(file) = self.file_system.get(parent) {
                if file.is_dir() && (options.create || options.create_new) {
                    let file = FNode::new();
                    self.file_system
                        .insert(options.path, Ident::File(file.clone()));
                    return Ok(DbFile::in_memory(file));
                }
            }
        }

        Err(io::Error::new(
            ErrorKind::NotFound,
            "Directory does not exist",
        ))
    }
}
