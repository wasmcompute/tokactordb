use std::{
    io::Write,
    path::Path,
    sync::{Arc, RwLock},
};

const MAX_BUFFER_SIZE: usize = 4096;

#[derive(Debug, Clone)]
pub struct FNode {
    pub inner: Arc<RwLock<Disk>>,
}

impl FNode {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(Disk::new())),
        }
    }

    pub fn truncate(&self) {
        self.inner.write().unwrap().truncate();
    }

    pub fn end_of_file_pointer(&self) -> usize {
        self.inner.write().unwrap().contents.len()
    }
}

impl Default for FNode {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub struct Disk {
    buffer_pointer: usize,
    buffer: [u8; MAX_BUFFER_SIZE],
    contents: Vec<u8>,
}

impl Disk {
    pub fn new() -> Self {
        Self {
            buffer_pointer: 0,
            buffer: [0_u8; MAX_BUFFER_SIZE],
            contents: Vec::new(),
        }
    }

    pub fn dump(&mut self, path: impl AsRef<Path>) -> anyhow::Result<()> {
        println!("Dumping to {:?}", path.as_ref());
        let mut p = 0;
        self.as_writer(&mut p).flush()?;
        std::fs::write(path, &self.contents)?;
        Ok(())
    }

    pub fn as_reader(&self) -> std::io::Result<&[u8]> {
        Ok(self.contents.as_ref())
    }

    pub fn is_empty(&self) -> bool {
        self.contents.is_empty()
    }

    pub fn as_writer<'a>(&'a mut self, pointer: &'a mut usize) -> RefDisk<'a> {
        RefDisk {
            disk: self,
            pointer,
        }
    }

    fn truncate(&mut self) {
        self.contents.clear();
        self.buffer = [0_u8; MAX_BUFFER_SIZE];
        self.buffer_pointer = 0;
    }
}

impl Default for Disk {
    fn default() -> Self {
        Self::new()
    }
}

pub struct RefDisk<'a> {
    disk: &'a mut Disk,
    pointer: &'a mut usize,
}

impl<'a> Write for RefDisk<'a> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut avaliable_bytes = MAX_BUFFER_SIZE - self.disk.buffer_pointer;
        if avaliable_bytes == 0 {
            self.flush()?;
            avaliable_bytes = MAX_BUFFER_SIZE;
        }
        let stream = if buf.len() > avaliable_bytes {
            &buf[..avaliable_bytes]
        } else {
            buf
        };
        self.disk.buffer[self.disk.buffer_pointer..self.disk.buffer_pointer + stream.len()]
            .clone_from_slice(stream);
        *self.pointer += stream.len();
        self.disk.buffer_pointer += stream.len();
        Ok(stream.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.disk
            .contents
            .extend_from_slice(&self.disk.buffer[..self.disk.buffer_pointer]);
        self.disk.buffer_pointer = 0;
        Ok(())
    }
}
