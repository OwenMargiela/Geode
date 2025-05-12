#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use byteorder::{LittleEndian, WriteBytesExt};
use std::ops::{Add, Div, Mul, Rem, Sub};
use std::str;

use super::types::char::Char;
use super::types::decimal::Decimal;
use super::types::int::Int;
use super::types::smallint::SmallInt;

use super::types::bigint::BigInt;
use super::types::varchar::Varchar;

pub mod data_type_string {
    pub const VARCHAR: &str = "VARCHAR";
    pub const CHAR: &str = "CHAR";
    pub const BOOLEAN: &str = "BOOLEAN";
    pub const DECIMAL: &str = "DECIMAL";

    pub const BIGINT: &str = "BIGINT";
    pub const INT: &str = "INT";
    pub const SMALLINT: &str = "SMALLINT";
}

pub struct SchemaDataValue<'a> {
    pub column_name: &'a str,
    pub data: ByteBox<'a>,
}

#[derive(Clone)]
pub struct ByteBox<'a> {
    pub data: Vec<u8>,
    pub datatype: &'a str,
    pub data_size: usize,
    pub data_length: u8,
}

impl<'a> ByteBox<'a> {
    pub fn new(data: Vec<u8>, datatype: &'a str) -> ByteBox<'a> {
        let len = data.len();
        ByteBox {
            data: data,
            data_length: len as u8,
            data_size: len,
            datatype: &datatype,
        }
    }

    pub fn big_int(val: i64) -> ByteBox<'a> {
        let mut buffer: Vec<u8> = Vec::new();
        buffer
            .write_i64::<LittleEndian>(val)
            .expect("Error writing a signed 64 bit integer");

        ByteBox::new(buffer, data_type_string::BIGINT)
    }

    pub fn int(val: i32) -> ByteBox<'a> {
        let mut buffer: Vec<u8> = Vec::new();
        buffer
            .write_i32::<LittleEndian>(val)
            .expect("Error writing a signed 32 bit integer");

        ByteBox::new(buffer, data_type_string::INT)
    }

    pub fn small_int(val: i16) -> ByteBox<'a> {
        let mut buffer: Vec<u8> = Vec::new();
        buffer
            .write_i16::<LittleEndian>(val)
            .expect("Error writing a signed 16 bit integer");

        ByteBox::new(buffer, data_type_string::SMALLINT)
    }

    pub fn decimal(val: f32) -> ByteBox<'a> {
        let mut buffer: Vec<u8> = Vec::new();
        buffer
            .write_f32::<LittleEndian>(val)
            .expect("Error writing a IEEE754 single-precision (4 bytes) floating point number");

        ByteBox::new(buffer, data_type_string::DECIMAL)
    }

    pub fn char(val: &str, charlen: usize) -> ByteBox<'a> {
        let char = Char::new(val, charlen);
        let data = char.get_raw_bytes();
        let len = data.len();

        ByteBox {
            data: data,
            datatype: data_type_string::CHAR,
            data_size: len,
            data_length: len as u8,
        }
    }

    pub fn varchar(val: &str, charlen: usize) -> ByteBox<'a> {
        let varchar = Varchar::new(val, charlen);
        let data = varchar.get_raw_bytes();
        let len = data.len();

        ByteBox {
            data: data,
            datatype: data_type_string::VARCHAR,
            data_size: charlen,
            data_length: len as u8,
        }
    }

    pub fn boolean(val: bool) -> ByteBox<'a> {
        let val = val as u8;
        let mut buffer: Vec<u8> = Vec::new();
        buffer
            .write_u8(val)
            .expect("Error Writing an unsigned 8 bit integer");

        ByteBox::new(buffer, data_type_string::BOOLEAN)
    }
}

pub trait DataBox<'a, T> {
    type Output;
    type Input;

    fn wrap(new_data: Self::Input) -> Self;

    fn unwrap_value(&self) -> Self::Output;

    fn cast_container(&self, type_id: &str) -> Option<&str>;

    fn is_coercable_to(&self, type_id: &str) -> bool;

    fn get_type(&self) -> &str;

    fn to_byte_box(&self) -> ByteBox;
}

// Trait for all numeric types
pub trait NumbericType<'a>: DataBox<'a, Self::Value>
where
    Self::Value: PartialOrd + Ord,
    Self::Value: Add<Output = Self> + Sub<Output = Self> + Mul<Output = Self> + Div<Output = Self>,
{
    type Value;

    fn as_int(&self) -> Option<Int>;
    fn as_small_int(&self) -> Option<SmallInt>;
    fn as_big_int(&self) -> Option<BigInt>;
    fn as_decimal(&self) -> Option<Decimal>;
}

pub trait NumerOps<'a, T = Self>
where
    T: NumbericType<'a>,
    Self: NumbericType<'a>
        + Add<T, Output = T>
        + Sub<T, Output = T>
        + Mul<T, Output = T>
        + Div<T, Output = T>
        + Rem<T, Output = T>,
{
}

use std::io::{self, Cursor};

pub trait Serializable {
    fn serialize(&self, buffer: &mut Vec<u8>) -> io::Result<()>;
    fn deserialize(cursor: &mut Cursor<&[u8]>) -> io::Result<Self>
    where
        Self: Sized;
}
