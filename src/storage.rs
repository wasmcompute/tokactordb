pub struct WAL {
    inner: Vec<u8>,
}

impl WAL {
    pub fn new() -> Self {
        Self { inner: Vec::new() }
    }
}
