use std::path::PathBuf;

#[derive(Debug)]
pub struct OpenBaseDir;

#[derive(Debug)]
pub struct ValidateOrCreateDir(pub PathBuf);
