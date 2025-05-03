#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use crate::db_types::container::{data_type_string, Serializable};
use std::io::{self, Cursor, Read};

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Varchar {
    pub store: Vec<u8>,
    pub charlen: usize, // Current length
    pub maxlen: usize,  // Maximum allowed length
    pub type_id_string: &'static str,
}

impl Varchar {
    pub fn new(new_data: &str, len: usize) -> Varchar {
        let bytes = new_data.as_bytes().to_vec();
        let length = bytes.len().min(len);
        let store = bytes;

        Varchar {
            store,
            charlen: length,
            maxlen: len,
            type_id_string: data_type_string::VARCHAR,
        }
    }

    pub fn unwrap_value(&self) -> String {
        String::from_utf8_lossy(&self.store).into_owned()
    }

    pub fn get_type(&self) -> &str {
        self.type_id_string
    }

    pub fn get_raw_bytes(&self) -> Vec<u8> {
        self.store.clone()
    }
}

impl Serializable for Varchar {
    fn serialize(&self, buffer: &mut Vec<u8>) -> io::Result<()> {
        buffer.extend_from_slice(&(self.charlen as u64).to_le_bytes()); // Header with the length
        buffer.extend_from_slice(&self.store); // Actual string data
        Ok(())
    }

    fn deserialize(cursor: &mut Cursor<&[u8]>) -> io::Result<Self> {
        // Read the length from the header
        let mut length_bytes = [0u8; 8];
        cursor.read_exact(&mut length_bytes)?;
        let charlen = u64::from_le_bytes(length_bytes) as usize;

        // Read the actual string data
        let mut store = vec![0u8; charlen];
        cursor.read_exact(&mut store)?;

        Ok(Varchar {
            store,
            charlen,
            maxlen: charlen, // Assuming maxlen is the same as charlen for simplicity
            type_id_string: data_type_string::VARCHAR,
        })
    }
}

#[cfg(test)]
mod test {
    use std::fs::{remove_file, File};
    use std::io::{Cursor, Read, Write};

    use crate::db_types::container::data_type_string;
    use crate::db_types::container::Serializable;
    use crate::db_types::types::varchar::Varchar;

    #[test]
    fn wrap_unwrap_works() {
        let hello = Varchar::new("Hello", 16);
        let value = hello.unwrap_value();
        assert_eq!(value, "Hello");
        assert_eq!(hello.get_type(), data_type_string::VARCHAR);
    }

    #[test]
    fn order_works() {
        let mut varchar_things = vec![
            Varchar::new("apple", 16),
            Varchar::new("banana", 16),
            Varchar::new("cherry", 16),
            Varchar::new("apple", 16),
            Varchar::new("date", 16),
        ];
        varchar_things.sort();
        assert_eq!(varchar_things[0].unwrap_value(), "apple");
        assert_eq!(varchar_things[1].unwrap_value(), "apple");
        assert_eq!(varchar_things[2].unwrap_value(), "banana");
        assert_eq!(varchar_things[3].unwrap_value(), "cherry");
        assert_eq!(varchar_things[4].unwrap_value(), "date");
    }

    #[test]
    fn test_file_io() {
        let file_name = "varchar_test.bin";

        // Create and write to the file
        let original_varchar = Varchar::new("Hello, world!", 16);
        {
            let mut file = File::create(file_name).expect("Failed to create file");
            let mut buffer = Vec::new();
            original_varchar
                .serialize(&mut buffer)
                .expect("Serialization failed");
            file.write_all(&buffer).expect("Failed to write to file");
        }

        // Read from the file
        let mut buffer = Vec::new();
        {
            let mut file = File::open(file_name).expect("Failed to open file");
            file.read_to_end(&mut buffer)
                .expect("Failed to read from file");
        }

        // Clean up the test file
        remove_file(file_name).expect("Failed to delete the test file");

        // Deserialize the content
        let mut cursor = Cursor::new(buffer.as_slice());
        let deserialized_varchar =
            Varchar::deserialize(&mut cursor).expect("Deserialization failed");

        // Verify the content
        assert_eq!(
            original_varchar.unwrap_value(),
            deserialized_varchar.unwrap_value()
        );
        assert_eq!(original_varchar.get_type(), deserialized_varchar.get_type());
    }
}
