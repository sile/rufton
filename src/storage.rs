use crate::node::JsonLineValue;

#[derive(Debug)]
pub struct FileStorage {
    file: std::fs::File,
}

impl FileStorage {
    pub fn open<P: AsRef<std::path::Path>>(path: P) -> std::io::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        Ok(Self { file })
    }

    pub fn load_entries(&mut self) -> std::io::Result<Vec<JsonLineValue>> {
        todo!()
    }
}
