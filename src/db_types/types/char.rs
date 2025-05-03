#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use crate::db_types::container::data_type_string;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Char {
    store: Vec<u8>,
    charlen: usize,
    type_id_string: &'static str,
}

impl Char {
    pub fn new(new_data: &str, len: usize) -> Char {
        let u8_array = Char::string_pad(new_data, len);
        Char {
            store: u8_array,
            charlen: len,
            type_id_string: data_type_string::CHAR,
        }
    }

    pub fn unwrap_value(&self) -> String {
        if let Some(index) = self.store.iter().position(|&byte| byte == b'\0') {
            String::from_utf8_lossy(&self.store[..index]).into_owned()
        } else {
            String::from_utf8_lossy(&self.store).into_owned()
        }
    }

    pub fn get_type(&self) -> &str {
        self.type_id_string
    }

    pub fn get_raw_bytes(&self) -> Vec<u8> {
        self.store.clone()
    }

    fn string_pad(new_data: &str, len: usize) -> Vec<u8> {
        let mut padding = vec![b'\0'; len];
        let bytes = new_data.as_bytes();
        let length = bytes.len().min(len);
        padding[..length].copy_from_slice(&bytes[..length]);
        padding
    }
}

#[cfg(test)]
mod test {
    use crate::db_types::container::data_type_string;
    use crate::db_types::types::char::Char;

    #[test]
    fn wrap_unwrap_works() {
        let hello = Char::new("Hello", 16);
        let value = hello.unwrap_value();
        assert_eq!(value, "Hello");
        assert_eq!(hello.get_type(), data_type_string::CHAR);
    }

    #[test]
    fn order_works() {
        let mut char_things = vec![
            Char::new("apple", 16),
            Char::new("banana", 16),
            Char::new("cherry", 16),
            Char::new("apple", 16),
            Char::new("date", 16),
        ];
        char_things.sort();
        assert_eq!(char_things[0].unwrap_value(), "apple");
        assert_eq!(char_things[1].unwrap_value(), "apple");
        assert_eq!(char_things[2].unwrap_value(), "banana");
        assert_eq!(char_things[3].unwrap_value(), "cherry");
        assert_eq!(char_things[4].unwrap_value(), "date");
    }

    #[test]
    fn test_file_io() {
        use std::fs::{remove_file, File};
        use std::io::{Read, Write};

        let file_name = "char_test.bin";

        // Create and write to the file
        let original_char = Char::new("Hello, world!", 16);
        {
            let mut file = File::create(file_name).expect("Failed to create file");
            file.write_all(&original_char.store)
                .expect("Failed to write to file");
        }

        // Read from the file
        let mut buffer = vec![0u8; 16];
        {
            let mut file = File::open(file_name).expect("Failed to open file");
            file.read_exact(&mut buffer)
                .expect("Failed to read from file");
        }

        // Clean up the test file
        remove_file(file_name).expect("Failed to delete the test file");

        // Verify the content
        let read_char = Char {
            store: buffer,
            charlen: 16,
            type_id_string: data_type_string::CHAR,
        };
        assert_eq!(original_char.unwrap_value(), read_char.unwrap_value());
        assert_eq!(original_char.get_type(), read_char.get_type());
    }
}
