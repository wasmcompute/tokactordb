use std::{
    collections::HashMap,
    io::{self, ErrorKind},
    path::{Path, PathBuf},
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
    pub fn create_dir(&mut self, path: impl AsRef<Path>) -> io::Result<()> {
        let path = path.as_ref().to_path_buf();
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
            Err(io::Error::new(
                ErrorKind::InvalidInput,
                "Can't truncate file without write access",
            ))
        } else if !options.write && !options.read {
            Err(io::Error::new(
                ErrorKind::InvalidInput,
                "Can't open file without read and write permissions",
            ))
        } else if let Some(file) = self.file_system.get(&options.path) {
            match file {
                Ident::Dir() => Err(io::Error::new(
                    ErrorKind::AlreadyExists,
                    "Path already exists as a directory",
                )),
                Ident::File(file) => {
                    if options.create_new {
                        Err(io::Error::new(ErrorKind::NotFound, "File already exists"))
                    } else {
                        let mut pointer = 0;
                        if options.write && options.truncate {
                            file.truncate();
                        } else if options.write && options.append {
                            pointer = file.end_of_file_pointer();
                        }
                        Ok(DbFile::in_memory(
                            file.clone(),
                            pointer,
                            options.read,
                            options.write,
                        ))
                    }
                }
            }
        } else {
            if let Some(parent) = options.path.parent() {
                // File was not found, search the parent directory above to validate the path exists
                if let Some(file) = self.file_system.get(parent) {
                    // create the file, if you can
                    if file.is_dir() {
                        if options.write && (options.create || options.create_new) {
                            let file = FNode::new();
                            self.file_system
                                .insert(options.path, Ident::File(file.clone()));
                            return Ok(DbFile::in_memory(file, 0, options.read, options.write));
                        } else {
                            return Err(io::Error::new(
                                ErrorKind::InvalidInput,
                                "Can't create file without write permissions",
                            ));
                        }
                    }
                }
            }

            Err(io::Error::new(
                ErrorKind::NotFound,
                "Directory does not exist",
            ))
        }
    }
}

impl From<()> for InMemoryFs {
    fn from(_: ()) -> Self {
        let map = HashMap::new();
        Self { file_system: map }
    }
}

impl<K, V> From<&[(K, V)]> for InMemoryFs
where
    K: AsRef<str>,
    V: AsRef<str>,
{
    fn from(values: &[(K, V)]) -> Self {
        let mut map = HashMap::new();
        for (key, value) in values {
            let k = PathBuf::from(key.as_ref());
            let v = FNode::from(value);
            map.insert(k, Ident::File(v));
        }
        Self { file_system: map }
    }
}
