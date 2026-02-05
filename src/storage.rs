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
            .truncate(false)
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

    pub fn append_entry(&mut self, entry: &JsonLineValue) -> std::io::Result<()> {
        use std::io::Write;

        // Write the entry to the file
        writeln!(self.file, "{}", entry)?;

        // Ensure data is flushed to disk
        self.file.flush()?;

        Ok(())
    }

    pub fn save_snapshot(&mut self, entry: &JsonLineValue) -> std::io::Result<()> {
        use std::io::Write;

        // Truncate the file to clear all existing content
        self.file.set_len(0)?;

        // Reset file pointer to the beginning
        use std::io::Seek;
        self.file.seek(std::io::SeekFrom::Start(0))?;

        // Write the snapshot entry to the file
        writeln!(self.file, "{}", entry)?;

        // Ensure data is flushed to disk
        self.file.flush()?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::{JsonLineValue, RaftNode, StorageEntry};
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_file_storage_with_single_node_cluster() {
        // Create a temporary directory for the test
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let storage_path = temp_dir.path().join("storage.jsonl");

        // Initialize a single node cluster and save entries
        let mut node = RaftNode::start(noraft::NodeId::new(0));
        assert!(node.init_cluster());

        // Collect storage entries from actions
        let mut entries = Vec::new();
        while let Some(action) = node.next_action() {
            if let crate::node::Action::AppendStorageEntry(entry) = action {
                entries.push(entry);
            }
        }

        // Write entries to storage
        {
            let mut storage = FileStorage::open(&storage_path).expect("Failed to open storage");
            for entry in &entries {
                storage
                    .append_entry(entry)
                    .expect("Failed to append entry to storage");
            }
        }

        // Verify entries were written to file
        let file_content = fs::read_to_string(&storage_path).expect("Failed to read storage file");
        assert!(!file_content.is_empty(), "Storage file should not be empty");

        // Load entries from storage and verify they match
        let mut storage = FileStorage::open(&storage_path).expect("Failed to open storage");
        let loaded_entries = storage
            .load_entries()
            .expect("Failed to load entries from storage");

        assert_eq!(
            entries.len(),
            loaded_entries.len(),
            "Number of loaded entries should match saved entries"
        );

        // Verify each entry matches
        for (saved, loaded) in entries.iter().zip(loaded_entries.iter()) {
            assert_eq!(
                saved.get().as_raw_str(),
                loaded.get().as_raw_str(),
                "Entry content should match"
            );
        }
    }

    #[test]
    fn test_file_storage_append_and_load() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let storage_path = temp_dir.path().join("test_storage.jsonl");

        // Create and append entries
        let entry1 = JsonLineValue::new_internal(StorageEntry::Term(noraft::Term::new(1)));
        let entry2 = JsonLineValue::new_internal(StorageEntry::NodeGeneration(5));

        {
            let mut storage = FileStorage::open(&storage_path).expect("Failed to open storage");
            storage
                .append_entry(&entry1)
                .expect("Failed to append entry1");
            storage
                .append_entry(&entry2)
                .expect("Failed to append entry2");
        }

        // Load and verify
        let mut storage = FileStorage::open(&storage_path).expect("Failed to open storage");
        let entries = storage.load_entries().expect("Failed to load entries");

        assert_eq!(entries.len(), 2, "Should have loaded 2 entries");
    }

    #[test]
    fn test_file_storage_snapshot() {
        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let storage_path = temp_dir.path().join("snapshot_storage.jsonl");

        let entry1 = JsonLineValue::new_internal(StorageEntry::Term(noraft::Term::new(1)));
        let entry2 = JsonLineValue::new_internal(StorageEntry::Term(noraft::Term::new(2)));
        let entry3 = JsonLineValue::new_internal(StorageEntry::Term(noraft::Term::new(3)));

        // Write multiple entries
        {
            let mut storage = FileStorage::open(&storage_path).expect("Failed to open storage");
            storage
                .append_entry(&entry1)
                .expect("Failed to append entry1");
            storage
                .append_entry(&entry2)
                .expect("Failed to append entry2");
        }

        // Verify we have 2 entries
        {
            let mut storage = FileStorage::open(&storage_path).expect("Failed to open storage");
            let entries = storage.load_entries().expect("Failed to load entries");
            assert_eq!(entries.len(), 2);
        }

        // Save snapshot (should clear and write only one entry)
        {
            let mut storage = FileStorage::open(&storage_path).expect("Failed to open storage");
            storage
                .save_snapshot(&entry3)
                .expect("Failed to save snapshot");
        }

        // Verify only snapshot entry remains
        {
            let mut storage = FileStorage::open(&storage_path).expect("Failed to open storage");
            let entries = storage.load_entries().expect("Failed to load entries");
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].get().as_raw_str(), entry3.get().as_raw_str());
        }
    }
}
