use std::path::PathBuf;

use super::builder::TreeVersion;

#[derive(Debug)]
pub struct RequestWal();

#[derive(Debug)]
pub struct NewTreeRoot {
    pub name: String,
    pub versions: Vec<TreeVersion>,
}

impl NewTreeRoot {
    pub fn new(name: String, versions: Vec<TreeVersion>) -> Self {
        Self { name, versions }
    }
}

#[derive(Debug)]
pub struct RestoreDbPath {
    pub directory: PathBuf,
}

impl RestoreDbPath {
    pub fn new(directory: PathBuf) -> Self {
        Self { directory }
    }
}

#[derive(Debug)]
pub struct RestoreComplete;
