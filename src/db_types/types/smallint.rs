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
    data_type_string::{self}, ByteBox, DataBox, NumbericType, NumerOps, Serializable
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

#[cfg(test)]
mod tests {
    use std::fs::{self, File};
    use std::io::{Cursor, Read, Write};

    use super::*;

    #[test]
    fn wrap_unwrap_works() {
        let int = SmallInt::wrap(10);
        let val = int.unwrap_value();

        assert_eq!(val, 10);
        assert_eq!(int.type_id_string, data_type_string::SMALLINT);
    }

    #[test]
    fn is_coercable_works() {
        // Futher types will be created to facilitate small and long floating point types
        // Testing maximum values for SMALLINT and INT
        let mut int = SmallInt::wrap(i16::MAX);
        assert_eq!(int.is_coercable_to(data_type_string::INT), true);
        assert_eq!(int.is_coercable_to(data_type_string::DECIMAL), true);
        assert_eq!(int.is_coercable_to(data_type_string::SMALLINT), true);
        assert_eq!(int.is_coercable_to(data_type_string::BIGINT), true);

        // Testing minimum values for i32
        int = SmallInt::wrap(i16::MIN);
        assert_eq!(int.is_coercable_to(data_type_string::BIGINT), true);
        assert_eq!(int.is_coercable_to(data_type_string::DECIMAL), true);
        assert_eq!(int.is_coercable_to(data_type_string::SMALLINT), true);
        assert_eq!(int.is_coercable_to(data_type_string::INT), true);
    }

    #[test]

    fn cast_as_works() {
        let int = Int::wrap(i32::MAX);
        let newint = int.as_int().expect("Cast Failed");

        assert_eq!(int.unwrap_value(), newint.unwrap_value());
    }

    #[test]
    fn test_serializability() {
        //
        let mut buffer: Vec<u8> = Vec::new();
        let b = SmallInt::wrap(1230);

        // Serialize
        b.serialize(&mut buffer).unwrap();
        println!("Serialized Buffer: {:?}", buffer);

        // Deserialize
        let mut cursor = Cursor::new(buffer.as_slice());
        let deserialized = SmallInt::deserialize(&mut cursor).unwrap();

        let mut new_buffer: Vec<u8> = Vec::new();
        println!(
            "Deserialized Buffer: {:?}",
            deserialized.serialize(&mut new_buffer)
        );

        assert_eq!(deserialized.unwrap_value(), 1230);
    }

    #[test]
    fn test_file_serializability() {
        let file_name = "test_file_serialization.bin";
        let mut buffer: Vec<u8> = Vec::new();

        // Original value to serialize
        let original_value = SmallInt::wrap(1230);

        // Step 1: Serialize and write to a file
        {
            let mut file = File::create(file_name).expect("Failed to create file");
            original_value
                .serialize(&mut buffer)
                .expect("Serialization failed");
            file.write_all(&buffer).expect("Failed to write to file");
        }

        // Step 2: Read the raw bytes from the file
        let mut file = File::open(file_name).expect("Failed to open file");
        file.read_to_end(&mut buffer).expect("Failed to read file");

        // Step 3: Deserialize the raw bytes
        let deserialized_value = {
            let mut cursor = Cursor::new(buffer.as_slice());
            SmallInt::deserialize(&mut cursor).unwrap()
        };

        // Step 4: Test equality
        assert_eq!(
            original_value.unwrap_value(),
            deserialized_value.unwrap_value(),
            "Original and deserialized values do not match"
        );

        // Step 5: Delete the file
        fs::remove_file(file_name).expect("Failed to delete test file");

        println!("Test passed: Serialization and deserialization work as expected.");
    }

    #[test]

    fn order_works() {
        // creating a list vector of Int to sort
        let mut big_int_things = vec![
            SmallInt::wrap(42),
            SmallInt::wrap(42),
            SmallInt::wrap(100),
            SmallInt::wrap(99),
            SmallInt::wrap(1000),
        ];

        big_int_things.sort();

        // Asserting the sort order
        assert_eq!(big_int_things[0].unwrap_value(), 42);
        assert_eq!(big_int_things[1].unwrap_value(), 42);
        assert_eq!(big_int_things[2].unwrap_value(), 99);
        assert_eq!(big_int_things[3].unwrap_value(), 100);
        assert_eq!(big_int_things[4].unwrap_value(), 1000);

        // Asserting using boolean operators
        assert_eq!(
            big_int_things[0].unwrap_value() < big_int_things[4].unwrap_value(),
            true
        ); // 42 < 1000 is true
        assert_eq!(
            big_int_things[1].unwrap_value() > big_int_things[3].unwrap_value(),
            false
        ); // 42 > 99 is false
        assert_eq!(
            big_int_things[2].unwrap_value() <= big_int_things[3].unwrap_value(),
            true
        ); // 99 <= 100 is true
        assert_eq!(
            big_int_things[3].unwrap_value() >= big_int_things[1].unwrap_value(),
            true
        ); // 100 >= 42 is true
        assert_eq!(
            big_int_things[4].unwrap_value() == big_int_things[4].unwrap_value(),
            true
        ); // 1000 == 1000 is true
        assert_eq!(
            big_int_things[0].unwrap_value() != big_int_things[2].unwrap_value(),
            true
        ); // 42 != 99 is true
    }

    #[test]
    fn test_numeric_operations_with_edge_cases() {
        let zero = SmallInt::wrap(0);
        assert_eq!((zero + zero).unwrap_value(), 0);
        assert_eq!((zero - zero).unwrap_value(), 0);
        assert_eq!((zero * zero).unwrap_value(), 0);
        // Division by zero should be handled appropriately, usually resulting in an error or special case.

        let neg_one = SmallInt::wrap(-1);
        assert_eq!((neg_one + neg_one).unwrap_value(), -2);
        assert_eq!((neg_one - neg_one).unwrap_value(), 0);
        assert_eq!((neg_one * neg_one).unwrap_value(), 1);
        assert_eq!((neg_one / neg_one).unwrap_value(), 1);

        let pos_one = SmallInt::wrap(1);
        assert_eq!((neg_one + pos_one).unwrap_value(), 0);
        assert_eq!((neg_one - pos_one).unwrap_value(), -2);
        assert_eq!((neg_one * pos_one).unwrap_value(), -1);
        assert_eq!((neg_one / pos_one).unwrap_value(), -1);

        // Max/Min Integer Values
        let max = SmallInt::wrap(i16::MAX);
        let min = SmallInt::wrap(i16::MIN);
        assert_eq!((max + SmallInt::wrap(0)).unwrap_value(), i16::MAX);
        assert_eq!((min + SmallInt::wrap(0)).unwrap_value(), i16::MIN);
    }
}
