use std::{collections::HashMap, path::PathBuf};

use super::file::FNode;

#[derive(Debug)]
enum Ident {
    Dir(),
    File(FNode),
}

impl Ident {
    /// Returns `true` if the ident is [`File`].
    ///
    /// [`File`]: Ident::File
    #[must_use]
    fn is_file(&self) -> bool {
        matches!(self, Self::File(..))
    }

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
    pub fn create_dir(&self, path: PathBuf) -> std::io::Result<()> {
        if let Some(entry) = self.file_system.get(&path) {
            match entry {
                Ident::Dir() => {}
                Ident::File(_) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::AlreadyExists,
                        format!("{:?} already exists as a file", path),
                    ))
                }
            }
        } else {
            self.file_system.insert(path, Ident::Dir());
        }
        Ok(())
    }
}
