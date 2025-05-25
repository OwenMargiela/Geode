#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use byteorder::{ LittleEndian, WriteBytesExt };
use bytes::Bytes;
use std::{ cmp::Ordering, fmt };

use std::io::Cursor;
use byteorder::ReadBytesExt;

use crate::catalog::schema::SchemaDataValue;

#[derive(Clone)]
pub struct ByteBox {
    pub data: Bytes,
    pub datatype: DataType,

    pub data_size: usize,
    pub data_length: u8,
}

#[derive(Debug)]
enum ByteValue<'a> {
    I64(i64),
    F32(f32),
    Bool(bool),
    Str(&'a str),
    None,
}

impl<'a> PartialEq for ByteValue<'a> {
    fn eq(&self, other: &Self) -> bool {
        use ByteValue::*;
        match (self, other) {
            (I64(a), I64(b)) => a == b,
            (F32(a), F32(b)) => a == b,
            (Bool(a), Bool(b)) => a == b,
            (Str(a), Str(b)) => a == b,
            (None, None) => true,
            _ => false,
        }
    }
}

impl<'a> Eq for ByteValue<'a> {}

impl<'a> PartialOrd for ByteValue<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        use ByteValue::*;
        match (self, other) {
            (I64(a), I64(b)) => Some(a.cmp(b)),
            (F32(a), F32(b)) => a.partial_cmp(b),
            (Bool(a), Bool(b)) => Some(a.cmp(b)),
            (Str(a), Str(b)) => Some(a.cmp(b)),
            (None, None) => Some(Ordering::Equal),
            _ => Option::None,
        }
    }
}

impl<'a> Ord for ByteValue<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

impl ByteBox {
    fn to_value(&self) -> ByteValue {
        use ByteValue::*;
        let mut rdr = Cursor::new(&self.data);

        match self.datatype {
            DataType::BigInt => rdr.read_i64::<LittleEndian>().map(I64).unwrap_or(None),
            DataType::Int =>
                rdr
                    .read_i32::<LittleEndian>()
                    .map(|v| I64(v as i64))
                    .unwrap_or(None),
            DataType::SmallInt =>
                rdr
                    .read_i16::<LittleEndian>()
                    .map(|v| I64(v as i64))
                    .unwrap_or(None),
            DataType::Decimal => rdr.read_f32::<LittleEndian>().map(F32).unwrap_or(None),
            DataType::Boolean => {
                if self.data.len() == 1 { Bool(self.data[0] != 0) } else { None }
            }
            DataType::Char(_) | DataType::Varchar(_) => {
                match std::str::from_utf8(&self.data) {
                    Ok(s) => Str(s),
                    Err(_) => None,
                }
            }
            DataType::None => None,
            _ => { None }
        }
    }
}

impl ByteBox {
    pub fn new(data: Bytes, datatype: DataType) -> ByteBox {
        let len = data.len();
        ByteBox {
            data,
            data_length: len as u8,
            data_size: len,
            datatype,
        }
    }

    pub fn big_int(val: i64) -> ByteBox {
        let mut buffer: Vec<u8> = Vec::new();
        buffer.write_i64::<LittleEndian>(val).expect("Error writing a signed 64-bit integer");

        ByteBox::new(Bytes::copy_from_slice(&buffer), DataType::BigInt)
    }

    pub fn int(val: i32) -> ByteBox {
        let mut buffer: Vec<u8> = Vec::new();
        buffer.write_i32::<LittleEndian>(val).expect("Error writing a signed 32-bit integer");

        ByteBox::new(Bytes::copy_from_slice(&buffer), DataType::Int)
    }

    pub fn small_int(val: i16) -> ByteBox {
        let mut buffer: Vec<u8> = Vec::new();
        buffer.write_i16::<LittleEndian>(val).expect("Error writing a signed 16-bit integer");

        ByteBox::new(Bytes::copy_from_slice(&buffer), DataType::SmallInt)
    }

    pub fn decimal(val: f32) -> ByteBox {
        let mut buffer: Vec<u8> = Vec::new();
        buffer
            .write_f32::<LittleEndian>(val)
            .expect("Error writing an IEEE754 single-precision (4 bytes) floating point number");

        ByteBox::new(Bytes::copy_from_slice(&buffer), DataType::Decimal)
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

        if len > charlen {
            panic!("String Upperbound crossed");
        }

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
        buffer.write_u8(val).expect("Error writing an unsigned 8-bit integer");

        ByteBox::new(Bytes::copy_from_slice(&buffer), DataType::Boolean)
    }
}

// Implementing `PartialEq` for equality comparison
impl PartialEq for ByteBox {
    fn eq(&self, other: &Self) -> bool {
        self.datatype == other.datatype && self.data == other.data
    }
}

impl Eq for ByteBox {}

impl PartialOrd for ByteBox {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        if self.datatype != other.datatype {
            return None;
        }
        Some(self.to_value().cmp(&other.to_value()))
    }
}

impl Ord for ByteBox {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

impl fmt::Debug for ByteBox {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value_str = match self.datatype {
            DataType::BigInt => {
                let mut rdr = Cursor::new(&self.data);
                match rdr.read_i64::<LittleEndian>() {
                    Ok(v) => format!("{}", v),
                    Err(_) => "Invalid BigInt".to_string(),
                }
            }
            DataType::Int => {
                let mut rdr = Cursor::new(&self.data);
                match rdr.read_i32::<LittleEndian>() {
                    Ok(v) => format!("{}", v),
                    Err(_) => "Invalid Int".to_string(),
                }
            }
            DataType::SmallInt => {
                let mut rdr = Cursor::new(&self.data);
                match rdr.read_i16::<LittleEndian>() {
                    Ok(v) => format!("{}", v),
                    Err(_) => "Invalid SmallInt".to_string(),
                }
            }
            DataType::Decimal => {
                let mut rdr = Cursor::new(&self.data);
                match rdr.read_f32::<LittleEndian>() {
                    Ok(v) => format!("{}", v),
                    Err(_) => "Invalid Decimal".to_string(),
                }
            }
            DataType::Boolean => {
                if self.data.len() == 1 {
                    match self.data[0] {
                        0 => "false".to_string(),
                        1 => "true".to_string(),
                        _ => "Invalid Boolean".to_string(),
                    }
                } else {
                    "Invalid Boolean".to_string()
                }
            }
            DataType::Char(_) | DataType::Varchar(_) =>
                match std::str::from_utf8(&self.data) {
                    Ok(s) => format!("{:?}", s),
                    Err(_) => "Invalid UTF-8 string".to_string(),
                }

            DataType::Tuple(ref values) => {
                let mut result = String::from("( ");
                for val in values.iter() {
                    result.push_str(", ");

                    use std::fmt::Write as _;
                    write!(&mut result, "{}: {:?}", val.name, val.data).ok();
                }
                result.push_str(" )");
                result
            }

            DataType::None => { "None".to_string() }
        };

        write!(
            f,
            "ByteBox {{ value: {}, datatype: {:?}, data_size: {}, data_length: {} }}",
            value_str,
            self.datatype,
            self.data_size,
            self.data_length
        )
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

    Tuple(Vec<SchemaDataValue>),

    None,
}

impl DataType {
    pub fn to_string(&self) -> String {
        (
            match self {
                DataType::BigInt => "BIGINT",
                DataType::Int => "INT",
                DataType::SmallInt => "SMALLINT",
                DataType::Decimal => "DECIMAL",
                DataType::Char(_) => "CHAR",
                DataType::Varchar(_) => "VARCHAR",
                DataType::Boolean => "BOOLEAN",
                DataType::None => "UNINITIALIZED",
                DataType::Tuple(_) => "TUPLE",
            }
        ).to_string()
    }

    pub fn to_byte_box(&self, data: &[u8]) -> ByteBox {
        DataType::from_bytes(data, self)
    }
    pub fn from_bytes(data: &[u8], dtype: &DataType) -> ByteBox {
        let data_len = data.len();
        match dtype {
            DataType::Char(charlen) =>
                ByteBox {
                    data: Bytes::copy_from_slice(data),
                    datatype: dtype.clone(),
                    data_size: charlen.clone(),
                    data_length: data_len as u8,
                },
            DataType::Varchar(charlen) =>
                ByteBox {
                    data: Bytes::copy_from_slice(data),
                    datatype: dtype.clone(),
                    data_size: charlen.clone(),
                    data_length: data_len as u8,
                },
            _ =>
                ByteBox {
                    data: Bytes::copy_from_slice(data),
                    datatype: dtype.clone(),
                    data_size: data_len,
                    data_length: data_len as u8,
                },
        }
    }
}
