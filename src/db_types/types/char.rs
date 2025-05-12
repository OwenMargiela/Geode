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
