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
