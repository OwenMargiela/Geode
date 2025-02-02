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

impl<'a> DataBox<'a, Decimal<'a>> for Decimal<'a> {
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

        // if self.is_coercable_to(type_id) {
        //     //  Since the other types have not be implemented just as yet, lets return somthing generic
        //     Some(DataType::INT)
        // } else {
        //     None
        // }
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

    fn to_byte_box(&self) -> ByteBox<'a> {
        let mut buffer: Vec<u8> = Vec::new();
        self.serialize(&mut buffer)
            .expect("Error during serialization");

        ByteBox::new(buffer, &self.type_id_string)
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

#[cfg(test)]
mod tests {
    use std::fs::{self, File};
    use std::io::{Cursor, Read, Write};

    use super::*;

    #[test]
    fn wrap_unwrap_works() {
        let decimal = Decimal::wrap(10.00);
        let val = decimal.unwrap_value();

        assert_eq!(val, 10.00);
        assert_eq!(decimal.type_id_string, data_type_string::DECIMAL);
    }

    #[test]
    fn is_coercable_works() {
        // Futher types will be created to facilitate small and long floating point types
        // Testing maximum values for SMALLINT and INT
        let mut float = Decimal::wrap(f32::MAX);
        assert_eq!(float.is_coercable_to(data_type_string::INT), true);
        assert_eq!(float.is_coercable_to(data_type_string::DECIMAL), true);
        assert_eq!(float.is_coercable_to(data_type_string::SMALLINT), false);
        assert_eq!(float.is_coercable_to(data_type_string::INT), true);

        // Testing minimum values for i32
        float = Decimal::wrap(f32::MIN);
        assert_eq!(float.is_coercable_to(data_type_string::BIGINT), true);
        assert_eq!(float.is_coercable_to(data_type_string::SMALLINT), false);
        assert_eq!(float.is_coercable_to(data_type_string::INT), true);

        // Testing edge cases for INT and SMALLINT
        float = Decimal::wrap(i16::MAX as f32);
        assert_eq!(float.is_coercable_to(data_type_string::SMALLINT), true);
        assert_eq!(float.is_coercable_to(data_type_string::BIGINT), true);
        assert_eq!(float.is_coercable_to(data_type_string::INT), true);
    }

    #[test]
    fn test_serializability() {
        //
        let mut buffer: Vec<u8> = Vec::new();
        let b = Decimal::wrap(12345678.2341);

        // Serialize
        b.serialize(&mut buffer).unwrap();
        println!("Serialized Buffer: {:?}", buffer);

        // Deserialize
        let mut cursor = Cursor::new(buffer.as_slice());
        let deserialized = Decimal::deserialize(&mut cursor).unwrap();

        assert_eq!(deserialized.store, 12345678.2341);
    }

    #[test]
    fn test_file_serializability() {
        let file_name = "test_file_serialization.bin";
        let mut buffer: Vec<u8> = Vec::new();

        // Original value to serialize
        let original_value = Decimal::wrap(12345678.2341);

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
            Decimal::deserialize(&mut cursor).unwrap()
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
            Decimal::wrap(42.211),
            Decimal::wrap(42.112),
            Decimal::wrap(100.467),
            Decimal::wrap(99.23),
            Decimal::wrap(1000.00),
        ];

        big_int_things.sort();

        // Asserting the sort order
        // TODO Deal with edge cases regarding NAND
        assert_eq!(big_int_things[0].unwrap_value(), 42.112);
        assert_eq!(big_int_things[1].unwrap_value(), 42.211);
        assert_eq!(big_int_things[2].unwrap_value(), 99.23);
        assert_eq!(big_int_things[3].unwrap_value(), 100.467);
        assert_eq!(big_int_things[4].unwrap_value(), 1000.00);

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
        let zero = Decimal::wrap(0.0);
        assert_eq!((zero + zero).unwrap_value(), 0.0);
        assert_eq!((zero - zero).unwrap_value(), 0.0);
        assert_eq!((zero * zero).unwrap_value(), 0.0);
        // Division by zero should be handled appropriately, usually resulting in an error or special case.

        let neg_one = Decimal::wrap(-1.0);
        assert_eq!((neg_one + neg_one).unwrap_value(), -2.0);
        assert_eq!((neg_one - neg_one).unwrap_value(), 0.0);
        assert_eq!((neg_one * neg_one).unwrap_value(), 1.0);
        assert_eq!((neg_one / neg_one).unwrap_value(), 1.0);

        let pos_one = Decimal::wrap(1.0);
        assert_eq!((neg_one + pos_one).unwrap_value(), 0.0);
        assert_eq!((neg_one - pos_one).unwrap_value(), -2.0);
        assert_eq!((neg_one * pos_one).unwrap_value(), -1.0);
        assert_eq!((neg_one / pos_one).unwrap_value(), -1.0);

        // Max/Min Decimal Values
        let max = Decimal::wrap(f32::MAX);
        let min = Decimal::wrap(f32::MIN);
        assert_eq!((max + Decimal::wrap(0.0)).unwrap_value(), f32::MAX);
        assert_eq!((min + Decimal::wrap(0.0)).unwrap_value(), f32::MIN);
    }
}
