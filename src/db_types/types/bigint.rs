use std::{
    io,
    ops::{Add, Div, Mul, Rem, Sub},
};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::db_types::container::{
    data_type_string, ByteBox, DataBox, NumbericType, NumerOps, Serializable,
};

use super::{decimal::Decimal, int::Int, smallint::SmallInt};

// pub struct BigIntContainer(i64);
pub struct BigInt<'type_id> {
    store: i64,
    type_id_string: &'type_id str,
}

impl<'a> Serializable for BigInt<'a> {
    fn serialize(&self, buffer: &mut Vec<u8>) -> std::io::Result<()> {
        buffer.write_i64::<LittleEndian>(self.store)?;
        Ok(())
    }

    fn deserialize(cursor: &mut std::io::Cursor<&[u8]>) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        if cursor.get_ref().len() < 8 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough data for i64 Desrialization",
            ));
        }
        let value = cursor.read_i64::<LittleEndian>()?;
        Ok(BigInt::wrap(value))
    }
}

impl<'a> DataBox<'a, BigInt<'a>> for BigInt<'a> {
    type Output = i64;
    type Input = i64;

    fn cast_container(&self, type_id: &str) -> Option<&str> {
        // Depending on the type id of the second parameter
        // We will determine whether or not the current value
        // of the dat encapsulated within the BigInt datatype
        // can be stored withing other types
        // This implementation is should be exhaustive

        match type_id {
            data_type_string::BIGINT => Some(data_type_string::BIGINT),
            data_type_string::INT => Some(data_type_string::INT),
            data_type_string::SMALLINT => Some(data_type_string::SMALLINT),
            data_type_string::DECIMAL => Some(data_type_string::DECIMAL),
            _ => None,
        }
    }

    fn unwrap_value(&self) -> i64 {
        self.store
    }

    fn wrap(new_data: i64) -> Self {
        BigInt {
            store: new_data,
            type_id_string: data_type_string::BIGINT,
        }
    }
    fn is_coercable_to(&self, type_id: &str) -> bool {
        match type_id {
            "INT" => {
                let value = self.unwrap_value();

                value >= i32::MIN as i64 && value <= i32::MAX as i64
            }
            "SMALLINT" => {
                let value = self.unwrap_value();
                value >= i16::MIN as i64 && value <= i16::MAX as i64
            }
            "DECIMAL" => true,
            "BIGINT" => true,
            _ => false,
        }
    }

    fn get_type(&self) -> &str {
        self.type_id_string
    }

    fn to_byte_box(&self) -> ByteBox<'a> {
        let mut buffer: Vec<u8> = Vec::new();
        self.serialize(&mut buffer)
            .expect("Error during serialization");

        ByteBox::new(buffer, &self.type_id_string)
    }
}

impl<'a> NumbericType<'a> for BigInt<'a> {
    type Value = BigInt<'a>;

    fn as_big_int(&self) -> Option<BigInt> {
        Some(Self {
            store: self.store,
            type_id_string: self.type_id_string,
        })
    }

    fn as_decimal(&self) -> Option<Decimal> {
        Some(Decimal::wrap(self.store as f32))
    }

    fn as_int(&self) -> Option<Int> {
        if self.is_coercable_to(data_type_string::INT) {
            Some(Int::wrap(self.store as i32))
        } else {
            None
        }
    }

    fn as_small_int(&self) -> Option<SmallInt> {
        if self.is_coercable_to(data_type_string::SMALLINT) {
            Some(SmallInt::wrap(self.store as i16))
        } else {
            None
        }
    }
}

impl<'a> PartialOrd for BigInt<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.unwrap_value().cmp(&other.unwrap_value()))
    }
}

impl<'a> Ord for BigInt<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.unwrap_value().cmp(&other.unwrap_value())
    }
}

impl<'a> PartialEq for BigInt<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.unwrap_value() == other.unwrap_value()
    }
}

impl<'a> Eq for BigInt<'a> {}

impl<'a> NumerOps<'a> for BigInt<'a> {}

impl<'a> Add for BigInt<'a> {
    type Output = BigInt<'a>;

    fn add(self, rhs: Self) -> Self::Output {
        let res = self.unwrap_value() + rhs.unwrap_value();
        BigInt::wrap(res)
    }
}

impl<'a> Sub for BigInt<'a> {
    type Output = BigInt<'a>;
    fn sub(self, rhs: Self) -> Self::Output {
        let res = self.unwrap_value() - rhs.unwrap_value();
        BigInt::wrap(res)
    }
}

impl<'a> Div for BigInt<'a> {
    type Output = BigInt<'a>;
    fn div(self, rhs: Self) -> Self::Output {
        let res = self.unwrap_value() / rhs.unwrap_value();
        BigInt::wrap(res)
    }
}

impl<'a> Mul for BigInt<'a> {
    type Output = BigInt<'a>;
    fn mul(self, rhs: Self) -> Self::Output {
        let res = self.unwrap_value() * rhs.unwrap_value();
        BigInt::wrap(res)
    }
}

impl<'a> Rem for BigInt<'a> {
    type Output = BigInt<'a>;
    fn rem(self, rhs: Self) -> Self::Output {
        let res = self.unwrap_value() % rhs.unwrap_value();
        BigInt::wrap(res)
    }
}

impl<'a> Copy for BigInt<'a> {}

impl<'a> Clone for BigInt<'a> {
    fn clone(&self) -> Self {
        *self
    }
}
