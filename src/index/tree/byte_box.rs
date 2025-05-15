#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use byteorder::{LittleEndian, WriteBytesExt};
use bytes::Bytes;
use std::{cmp::Ordering, fmt};

#[derive(Clone)]
pub struct ByteBox {
    pub data: Bytes,
    pub datatype: DataType,
    pub data_size: usize,
    pub data_length: u8,
}

impl ByteBox {
    pub fn new(data: Bytes, datatype: String) -> ByteBox {
        let len = data.len();
        ByteBox {
            data,
            data_length: len as u8,
            data_size: len,
            datatype: DataType::None,
        }
    }

    pub fn big_int(val: i64) -> ByteBox {
        let mut buffer: Vec<u8> = Vec::new();
        buffer
            .write_i64::<LittleEndian>(val)
            .expect("Error writing a signed 64-bit integer");

        ByteBox::new(Bytes::copy_from_slice(&buffer), "BIGINT".to_string())
    }

    pub fn int(val: i32) -> ByteBox {
        let mut buffer: Vec<u8> = Vec::new();
        buffer
            .write_i32::<LittleEndian>(val)
            .expect("Error writing a signed 32-bit integer");

        ByteBox::new(Bytes::copy_from_slice(&buffer), "INT".to_string())
    }

    pub fn small_int(val: i16) -> ByteBox {
        let mut buffer: Vec<u8> = Vec::new();
        buffer
            .write_i16::<LittleEndian>(val)
            .expect("Error writing a signed 16-bit integer");

        ByteBox::new(Bytes::copy_from_slice(&buffer), "SMALLINT".to_string())
    }

    pub fn decimal(val: f32) -> ByteBox {
        let mut buffer: Vec<u8> = Vec::new();
        buffer
            .write_f32::<LittleEndian>(val)
            .expect("Error writing an IEEE754 single-precision (4 bytes) floating point number");

        ByteBox::new(Bytes::copy_from_slice(&buffer), "DECIMAL".to_string())
    }

    pub fn char(val: &str, charlen: usize) -> ByteBox {
        let char_bytes = val.as_bytes();
        let len = char_bytes.len();

        ByteBox {
            data: Bytes::copy_from_slice(char_bytes),
            datatype: DataType::Char(len),
            data_size: len,
            data_length: len as u8,
        }
    }

    pub fn varchar(val: &str, charlen: usize) -> ByteBox {
        let varchar_bytes = val.as_bytes();
        let len = varchar_bytes.len();

        ByteBox {
            data: Bytes::copy_from_slice(varchar_bytes),
            datatype: DataType::Varchar(charlen),
            data_size: charlen,
            data_length: len as u8,
        }
    }

    pub fn boolean(val: bool) -> ByteBox {
        let val = val as u8;
        let mut buffer: Vec<u8> = Vec::new();
        buffer
            .write_u8(val)
            .expect("Error writing an unsigned 8-bit integer");

        ByteBox::new(Bytes::copy_from_slice(&buffer), "BOOLEAN".to_string())
    }
}

// Implementing `PartialEq` for equality comparison
impl PartialEq for ByteBox {
    fn eq(&self, other: &Self) -> bool {
        self.datatype == other.datatype && self.data == other.data
    }
}

impl Eq for ByteBox {}

// Implementing `PartialOrd` for partial ordering
impl PartialOrd for ByteBox {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.datatype != other.datatype {
            None
        } else {
            self.data.partial_cmp(&other.data)
        }
    }
}

impl Ord for ByteBox {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.datatype.cmp(&other.datatype) {
            Ordering::Equal => self.data.cmp(&other.data),
            other => other,
        }
    }
}

impl fmt::Debug for ByteBox {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("ByteBox")
            .field("datatype", &self.datatype)
            .field("data_size", &self.data_size)
            .field("data_length", &self.data_length)
            .field("data", &self.data) // Uses the debug implementation of `Bytes`
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum DataType {
    BigInt,
    Int,
    SmallInt,
    Decimal,
    Char(usize),
    Varchar(usize),
    Boolean,
    None,
}

impl DataType {
    pub fn to_string(&self) -> String {
        match self {
            DataType::BigInt => "BIGINT",
            DataType::Int => "INT",
            DataType::SmallInt => "SMALLINT",
            DataType::Decimal => "DECIMAL",
            DataType::Char(_) => "CHAR",
            DataType::Varchar(_) => "VARCHAR",
            DataType::Boolean => "BOOLEAN",
            DataType::None => "UNINITIALIZED",
        }
        .to_string()
    }

    pub fn to_byte_box(&self, data: &[u8]) -> ByteBox {
        DataType::from_bytes(data, self)
    }
    pub fn from_bytes(data: &[u8], dtype: &DataType) -> ByteBox {
        let data_len = data.len();
        match dtype {
            DataType::Char(charlen) => ByteBox {
                data: Bytes::copy_from_slice(data),
                datatype: dtype.clone(),
                data_size: charlen.clone(),
                data_length: data_len as u8,
            },
            DataType::Varchar(charlen) => ByteBox {
                data: Bytes::copy_from_slice(data),
                datatype: dtype.clone(),
                data_size: charlen.clone(),
                data_length: data_len as u8,
            },
            _ => ByteBox {
                data: Bytes::copy_from_slice(data),
                datatype: dtype.clone(),
                data_size: data_len,
                data_length: data_len as u8,
            },
        }
    }
}
