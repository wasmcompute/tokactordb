use std::{
    fs,
    io::{Read, Write},
};

use tokactor::Actor;

use super::file::FNode;

#[derive(Debug)]
pub enum DbFile {
    Memory { file: FNode, read_pointer: usize },
    System(fs::File),
}

impl Actor for DbFile {}

impl DbFile {
    pub fn in_memory(node: FNode) -> Self {
        Self::Memory {
            file: node,
            read_pointer: 0,
        }
    }

    pub fn system(file: fs::File) -> Self {
        Self::System(file)
    }
}

impl Write for DbFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            DbFile::Memory {
                file,
                read_pointer: _,
            } => file.inner.write().unwrap().write(buf),
            DbFile::System(file) => file.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            DbFile::Memory {
                file,
                read_pointer: _,
            } => file.inner.write().unwrap().flush(),
            DbFile::System(file) => file.flush(),
        }
    }
}

impl Read for DbFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            DbFile::Memory { file, read_pointer } => {
                let lock = file.inner.read().unwrap();
                let remaining = &lock.as_reader()[*read_pointer..];
                let read = if remaining.is_empty() {
                    0
                } else if remaining.len() > buf.len() {
                    buf.clone_from_slice(&remaining[..buf.len()]);
                    buf.len()
                } else {
                    buf.clone_from_slice(remaining);
                    remaining.len()
                };
                *read_pointer += read;
                Ok(read)
            }
            DbFile::System(file) => file.read(buf),
        }
    }
}
