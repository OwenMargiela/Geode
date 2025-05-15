// use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
// use std::ops::{Add, Mul,Div, Rem, Sub};

// use crate::db_types::container::{type_ids, DataBox, DataContainer, Releasable};
// use crate::db_types::marshaling::Serializable;

use std::{
    io::{Cursor, Error, ErrorKind, Result},
    ops::{Add, Div, Mul, Rem, Sub},
};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use crate::db_types::container::{
    data_type_string, ByteBox, DataBox, NumbericType, NumerOps, Serializable,
};

use super::{bigint::BigInt, int::Int, smallint::SmallInt};

pub struct Decimal<'type_id> {
    store: f32,
    pub type_id_string: &'type_id str,
}

impl<'a> Serializable for Decimal<'a> {
    fn serialize(&self, buffer: &mut Vec<u8>) -> Result<()> {
        buffer.write_f32::<LittleEndian>(self.store)?;
        Ok(())
    }

    fn deserialize(cursor: &mut Cursor<&[u8]>) -> Result<Self>
    where
        Self: Sized,
    {
        if cursor.get_ref().len() < 4 {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "Not enough data for i32 desrialization",
            ));
        }
        let value = cursor.read_f32::<LittleEndian>()?;
        Ok(Decimal::wrap(value))
    }
}

impl<'a> DataBox<Decimal<'a>> for Decimal<'a> {
    type Output = f32;
    type Input = f32;

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
    }

    fn unwrap_value(&self) -> f32 {
        self.store
    }

    fn wrap(new_data: f32) -> Self {
        Decimal {
            store: new_data,
            type_id_string: data_type_string::DECIMAL,
        }
    }
    fn is_coercable_to(&self, type_id: &str) -> bool {
        match type_id {
            data_type_string::INT => true,

            data_type_string::SMALLINT => {
                let value = self.unwrap_value() as i32;
                value >= i16::MIN as i32 && value <= i16::MAX as i32
            }
            data_type_string::DECIMAL => true,
            data_type_string::BIGINT => true,
            _ => false,
        }
    }

    fn get_type(&self) -> &str {
        self.type_id_string
    }

    fn to_byte_box(&self) -> ByteBox {
        let mut buffer: Vec<u8> = Vec::new();
        self.serialize(&mut buffer)
            .expect("Error during serialization");

        ByteBox::new(buffer, self.type_id_string.to_string())
    }
}

impl<'a> NumbericType<'a> for Decimal<'a> {
    type Value = Decimal<'a>;

    fn as_big_int(&self) -> Option<BigInt> {
        Some(BigInt::wrap(self.store as i64))
    }

    fn as_decimal(&self) -> Option<Decimal> {
        Some(Self {
            store: self.store,
            type_id_string: self.type_id_string,
        })
    }

    fn as_int(&self) -> Option<Int> {
        Some(Int::wrap(self.store as i32))
    }

    fn as_small_int(&self) -> Option<SmallInt> {
        if self.is_coercable_to(data_type_string::SMALLINT) {
            Some(SmallInt::wrap(self.store as i16))
        } else {
            None
        }
    }
}

impl<'a> PartialOrd for Decimal<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.unwrap_value().partial_cmp(&other.unwrap_value())
    }
}

impl<'a> Ord for Decimal<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(&other).expect("Could not order")
    }
}

impl<'a> PartialEq for Decimal<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.unwrap_value() == other.unwrap_value()
    }
}

impl<'a> Eq for Decimal<'a> {}

impl<'a> NumerOps<'a> for Decimal<'a> {}

impl<'a> Add for Decimal<'a> {
    type Output = Decimal<'a>;

    fn add(self, rhs: Self) -> Self::Output {
        let res = self.unwrap_value() + rhs.unwrap_value();
        Decimal::wrap(res)
    }
}

impl<'a> Sub for Decimal<'a> {
    type Output = Decimal<'a>;
    fn sub(self, rhs: Self) -> Self::Output {
        let res = self.unwrap_value() - rhs.unwrap_value();
        Decimal::wrap(res)
    }
}

impl<'a> Div for Decimal<'a> {
    type Output = Decimal<'a>;
    fn div(self, rhs: Self) -> Self::Output {
        let res = self.unwrap_value() / rhs.unwrap_value();
        Decimal::wrap(res)
    }
}

impl<'a> Mul for Decimal<'a> {
    type Output = Decimal<'a>;
    fn mul(self, rhs: Self) -> Self::Output {
        let res = self.unwrap_value() * rhs.unwrap_value();
        Decimal::wrap(res)
    }
}

impl<'a> Rem for Decimal<'a> {
    type Output = Decimal<'a>;
    fn rem(self, rhs: Self) -> Self::Output {
        let res = self.unwrap_value() % rhs.unwrap_value();
        Decimal::wrap(res)
    }
}

// Copy Trait

impl<'a> Copy for Decimal<'a> {}

impl<'a> Clone for Decimal<'a> {
    fn clone(&self) -> Self {
        *self
    }
}
