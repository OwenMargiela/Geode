// use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
// use std::ops::{Add, Mul,Div, Rem, Sub};

// use crate::db_types::container::{type_ids, DataBox, DataContainer, Releasable};
// use crate::db_types::marshaling::Serializable;

use std::{
    io,
    ops::{Add, Div, Mul, Rem, Sub},
};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::db_types::container::{
    data_type_string::{self},
    ByteBox, DataBox, NumbericType, NumerOps, Serializable,
};

use super::{bigint::BigInt, decimal::Decimal, int::Int};

// pub struct BigIntContainer(i64);
pub struct SmallInt<'type_id> {
    pub store: i16,
    pub type_id_string: &'type_id str,
}

impl<'a> Serializable for SmallInt<'a> {
    fn serialize(&self, buffer: &mut Vec<u8>) -> std::io::Result<()> {
        buffer.write_i16::<LittleEndian>(self.store)?;
        Ok(())
    }

    fn deserialize(cursor: &mut std::io::Cursor<&[u8]>) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        if cursor.get_ref().len() < 2 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough data for i16 deserialization",
            ));
        }
        let value = cursor.read_i16::<LittleEndian>()?;
        Ok(SmallInt::wrap(value))
    }
}

impl<'a> DataBox<'a, SmallInt<'a>> for SmallInt<'a> {
    type Output = i16;
    type Input = i16;

    fn cast_container(&self, type_id: &str) -> Option<&str> {
        //         // Depending on the type id of the second parameter
        //         // We will determine whether or not the current value
        //         // of the dat encapsulated within the Int datatype
        //         // can be stored withing other types
        //         // This implementation is should be exhaustive

        match type_id {
            data_type_string::BIGINT => Some(data_type_string::BIGINT),
            data_type_string::INT => Some(data_type_string::INT),
            data_type_string::SMALLINT => Some(data_type_string::SMALLINT),
            data_type_string::DECIMAL => Some(data_type_string::DECIMAL),
            _ => None,
        }

        // if self.is_coercable_to(type_id) {
        //     //  Since the other types have not be implemented just as yet, lets return somthing generic
        //     Some(DataType::INT)
        // } else {
        //     None
        // }
    }

    fn unwrap_value(&self) -> i16 {
        self.store
    }

    fn wrap(new_data: i16) -> Self {
        SmallInt {
            store: new_data,
            type_id_string: data_type_string::SMALLINT,
        }
    }
    fn is_coercable_to(&self, type_id: &str) -> bool {
        match type_id {
            data_type_string::INT => true,

            data_type_string::SMALLINT => true,
            data_type_string::DECIMAL => true,
            data_type_string::BIGINT => true,
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

impl<'a> NumbericType<'a> for SmallInt<'a> {
    type Value = SmallInt<'a>;

    fn as_big_int(&self) -> Option<BigInt> {
        Some(BigInt::wrap(self.store as i64))
    }

    fn as_decimal(&self) -> Option<Decimal> {
        Some(Decimal::wrap(self.store as f32))
    }

    fn as_int(&self) -> Option<Int> {
        Some(Int::wrap(self.store as i32))
    }

    fn as_small_int(&self) -> Option<SmallInt> {
        Some(Self {
            store: self.store,
            type_id_string: self.type_id_string,
        })
    }
}

impl<'a> PartialOrd for SmallInt<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.unwrap_value().cmp(&other.unwrap_value()))
    }
}

impl<'a> Ord for SmallInt<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.unwrap_value().cmp(&other.unwrap_value())
    }
}

impl<'a> PartialEq for SmallInt<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.unwrap_value() == other.unwrap_value()
    }
}

impl<'a> Eq for SmallInt<'a> {}

impl<'a> NumerOps<'a> for SmallInt<'a> {}

impl<'a> Add for SmallInt<'a> {
    type Output = SmallInt<'a>;

    fn add(self, rhs: Self) -> Self::Output {
        let res = self.unwrap_value() + rhs.unwrap_value();
        SmallInt::wrap(res)
    }
}

impl<'a> Sub for SmallInt<'a> {
    type Output = SmallInt<'a>;
    fn sub(self, rhs: Self) -> Self::Output {
        let res = self.unwrap_value() - rhs.unwrap_value();
        SmallInt::wrap(res)
    }
}

impl<'a> Div for SmallInt<'a> {
    type Output = SmallInt<'a>;
    fn div(self, rhs: Self) -> Self::Output {
        let res = self.unwrap_value() / rhs.unwrap_value();
        SmallInt::wrap(res)
    }
}

impl<'a> Mul for SmallInt<'a> {
    type Output = SmallInt<'a>;
    fn mul(self, rhs: Self) -> Self::Output {
        let res = self.unwrap_value() * rhs.unwrap_value();
        SmallInt::wrap(res)
    }
}

impl<'a> Rem for SmallInt<'a> {
    type Output = SmallInt<'a>;
    fn rem(self, rhs: Self) -> Self::Output {
        let res = self.unwrap_value() % rhs.unwrap_value();
        SmallInt::wrap(res)
    }
}

// Copy Trait

impl<'a> Copy for SmallInt<'a> {}

impl<'a> Clone for SmallInt<'a> {
    fn clone(&self) -> Self {
        *self
    }
}
