use std::{
    fs,
    io::{ErrorKind, Read, Write},
};

use tokactor::Actor;

use super::file::FNode;

#[derive(Debug)]
pub enum DbFile {
    Memory {
        file: FNode,
        pointer: usize,
        read: bool,
        write: bool,
    },
    System(fs::File),
}

impl Actor for DbFile {}

impl DbFile {
    pub fn in_memory(node: FNode, pointer: usize, read: bool, write: bool) -> Self {
        Self::Memory {
            file: node,
            pointer,
            read,
            write,
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
                pointer,
                read: _,
                write,
            } => {
                if !*write {
                    Err(std::io::Error::new(
                        ErrorKind::PermissionDenied,
                        "Write permissions not given when openning file",
                    ))
                } else {
                    let mut lock = file.inner.write().unwrap();
                    let mut writer = lock.as_writer(pointer);
                    let len = writer.write(buf)?;
                    Ok(len)
                }
            }
            DbFile::System(file) => file.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            DbFile::Memory {
                file,
                pointer,
                read: _,
                write,
            } => {
                if !*write {
                    Err(std::io::Error::new(
                        ErrorKind::PermissionDenied,
                        "Write permissions not given when openning file",
                    ))
                } else {
                    file.inner.write().unwrap().as_writer(pointer).flush()
                }
            }
            DbFile::System(file) => file.flush(),
        }
    }
}

impl Read for DbFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            DbFile::Memory {
                file,
                pointer,
                read,
                write: _,
            } => {
                if !*read {
                    return Err(std::io::Error::new(
                        ErrorKind::PermissionDenied,
                        "Read permissions not given when openning file",
                    ));
                }
                let lock = file.inner.read().unwrap();
                let reader = lock.as_reader()?;
                let remaining = &reader[*pointer..];
                let read = if remaining.is_empty() {
                    0
                } else if remaining.len() > buf.len() {
                    buf.clone_from_slice(&remaining[..buf.len()]);
                    buf.len()
                } else {
                    buf[..remaining.len()].clone_from_slice(remaining);
                    remaining.len()
                };
                *pointer += read;
                Ok(read)
            }
            DbFile::System(file) => file.read(buf),
        }
    }
}
