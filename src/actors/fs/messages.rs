use std::path::PathBuf;

#[derive(Debug)]
pub struct OpenBaseDir;

#[derive(Debug)]
pub struct ValidateOrCreateDir(pub PathBuf);

#[derive(Debug)]
pub struct OpenFileOptions {
    pub read: bool,
    pub write: bool,
    pub truncate: bool,
    pub create: bool,
    pub create_new: bool,
    pub append: bool,
    pub path: PathBuf,
}

impl OpenFileOptions {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            read: false,
            write: false,
            truncate: false,
            create: false,
            create_new: false,
            append: false,
            path: path.into(),
        }
    }

    pub fn read(mut self) -> Self {
        self.read = true;
        self
    }

    pub fn write(mut self) -> Self {
        self.write = true;
        self
    }

    pub fn truncate(mut self) -> Self {
        self.truncate = true;
        self
    }

    pub fn create_new(mut self) -> Self {
        self.create_new = true;
        self
    }

    pub fn create(mut self) -> Self {
        self.create = true;
        self
    }

    pub fn append(mut self) -> Self {
        self.append = true;
        self
    }
}
