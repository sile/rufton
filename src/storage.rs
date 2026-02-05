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
        use std::io::{BufRead, BufReader, Seek, SeekFrom};

        // Reset file pointer to the beginning
        self.file.seek(SeekFrom::Start(0))?;

        let reader = BufReader::new(&mut self.file);
        let mut entries = Vec::new();

        for line in reader.lines() {
            let line = line?;
            let trimmed = line.trim();

            // Skip empty lines
            if trimmed.is_empty() {
                continue;
            }

            // Parse JSON using nojson
            match nojson::RawJsonOwned::parse(trimmed) {
                Ok(raw_json) => {
                    let value = JsonLineValue::new_internal(raw_json.value());
                    entries.push(value);
                }
                Err(e) => eprintln!("Warning: Failed to parse JSON line: {}", e),
            }
        }

        Ok(entries)
    }
}
