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

    pub fn append(&self) {
        self.inner.write().unwrap().append();
    }
}

impl Default for FNode {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
pub struct Disk {
    write_pointer: usize,
    buffer: [u8; MAX_BUFFER_SIZE],
    contents: Vec<u8>,
}

impl Disk {
    pub fn new() -> Self {
        Self {
            write_pointer: 0,
            buffer: [0_u8; MAX_BUFFER_SIZE],
            contents: Vec::new(),
        }
    }

    pub fn dump(&mut self, path: impl AsRef<Path>) -> anyhow::Result<()> {
        println!("Dumping to {:?}", path.as_ref());
        self.flush()?;
        std::fs::write(path, &self.contents)?;
        Ok(())
    }

    pub fn as_reader(&self) -> std::io::Result<&[u8]> {
        Ok(self.contents.as_ref())
    }

    pub fn is_empty(&self) -> bool {
        self.contents.is_empty()
    }

    fn truncate(&mut self) {
        self.write_pointer = 0;
        self.contents.clear();
        self.buffer = [0_u8; MAX_BUFFER_SIZE];
    }

    fn append(&mut self) {
        self.write_pointer = self.contents.len();
    }
}

impl Default for Disk {
    fn default() -> Self {
        Self::new()
    }
}

impl Write for Disk {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let avaliable_bytes = MAX_BUFFER_SIZE - self.write_pointer;
        if avaliable_bytes == 0 {
            self.flush()?;
        }
        let stream = if buf.len() > avaliable_bytes {
            &buf[..avaliable_bytes]
        } else {
            buf
        };
        self.buffer[self.write_pointer..self.write_pointer + stream.len()].clone_from_slice(stream);
        self.write_pointer += stream.len();
        Ok(stream.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.contents
            .extend_from_slice(&self.buffer[..self.write_pointer]);
        self.write_pointer = 0;
        Ok(())
    }
}
