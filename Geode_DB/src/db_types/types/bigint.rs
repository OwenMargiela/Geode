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
        //         // Depending on the type id of the second parameter
        //         // We will determine whether or not the current value
        //         // of the dat encapsulated within the BigInt datatype
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

// Copy Trait

impl<'a> Copy for BigInt<'a> {}

impl<'a> Clone for BigInt<'a> {
    fn clone(&self) -> Self {
        *self
    }
}

#[cfg(test)]
mod tests {
    use std::fs::{self, File};
    use std::i64;
    use std::io::{Cursor, Read, Write};

    use super::*;

    #[test]
    fn wrap_unwrap_works() {
        let bigint = BigInt::wrap(10);
        let val = bigint.unwrap_value();

        assert_eq!(val, 10);
        assert_eq!(bigint.type_id_string, data_type_string::BIGINT);
    }

    #[test]
    fn is_coercable_works() {
        // Futher types will be created to facilitate small and long floating point types
        // Testing maximum values for SMALLINT and INT
        let mut bigint = BigInt::wrap(i64::MAX);
        assert_eq!(bigint.is_coercable_to(data_type_string::BIGINT), true);
        assert_eq!(bigint.is_coercable_to(data_type_string::DECIMAL), true);
        assert_eq!(bigint.is_coercable_to(data_type_string::SMALLINT), false);
        assert_eq!(bigint.is_coercable_to(data_type_string::INT), false);

        // Testing minimum values for i64
        bigint = BigInt::wrap(i64::MIN);
        assert_eq!(bigint.is_coercable_to(data_type_string::BIGINT), true);
        assert_eq!(bigint.is_coercable_to(data_type_string::SMALLINT), false);
        assert_eq!(bigint.is_coercable_to(data_type_string::INT), false);

        // Testing edge cases for INT and SMALLINT
        bigint = BigInt::wrap(i32::MAX as i64);
        assert_eq!(bigint.is_coercable_to(data_type_string::INT), true);
        assert_eq!(bigint.is_coercable_to(data_type_string::SMALLINT), false);

        bigint = BigInt::wrap(i32::MIN as i64);
        assert_eq!(bigint.is_coercable_to(data_type_string::INT), true);
        assert_eq!(bigint.is_coercable_to(data_type_string::SMALLINT), false);

        bigint = BigInt::wrap(i16::MAX as i64);
        assert_eq!(bigint.is_coercable_to(data_type_string::SMALLINT), true);

        bigint = BigInt::wrap(i16::MIN as i64);
        assert_eq!(bigint.is_coercable_to(data_type_string::SMALLINT), true);
    }

    #[test]

    fn cast_as_works() {
        let bigint = BigInt::wrap(i64::MAX);
        let newbigint = bigint.as_big_int().expect("Cast Failed");

        assert_eq!(bigint.unwrap_value(), newbigint.unwrap_value());
    }

    #[test]
    fn test_serializability() {
        //
        let mut buffer: Vec<u8> = Vec::new();
        let b = BigInt::wrap(1234567890);

        // Serialize
        b.serialize(&mut buffer).unwrap();
        println!("Serialized Buffer: {:?}", buffer);

        // Deserialize
        let mut cursor = Cursor::new(buffer.as_slice());
        let deserialized = BigInt::deserialize(&mut cursor).unwrap();

        assert_eq!(deserialized.store, 1234567890);
    }

    #[test]
    fn test_file_serializability() {
        let file_name = "test_file_serialization.bin";
        let mut buffer: Vec<u8> = Vec::new();

        // Original value to serialize
        let original_value = BigInt::wrap(1234567890);

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
            BigInt::deserialize(&mut cursor).unwrap()
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
        // creating a list vector of BigInt to sort
        let mut big_int_things = vec![
            BigInt::wrap(42),
            BigInt::wrap(42),
            BigInt::wrap(100),
            BigInt::wrap(99),
            BigInt::wrap(1000),
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
        let zero = BigInt::wrap(0);
        assert_eq!((zero + zero).unwrap_value(), 0);
        assert_eq!((zero - zero).unwrap_value(), 0);
        assert_eq!((zero * zero).unwrap_value(), 0);
        // Division by zero should be handled appropriately, usually resulting in an error or special case.

        let neg_one = BigInt::wrap(-1);
        assert_eq!((neg_one + neg_one).unwrap_value(), -2);
        assert_eq!((neg_one - neg_one).unwrap_value(), 0);
        assert_eq!((neg_one * neg_one).unwrap_value(), 1);
        assert_eq!((neg_one / neg_one).unwrap_value(), 1);

        let pos_one = BigInt::wrap(1);
        assert_eq!((neg_one + pos_one).unwrap_value(), 0);
        assert_eq!((neg_one - pos_one).unwrap_value(), -2);
        assert_eq!((neg_one * pos_one).unwrap_value(), -1);
        assert_eq!((neg_one / pos_one).unwrap_value(), -1);

        // Max/Min Integer Values
        let max = BigInt::wrap(i64::MAX);
        let min = BigInt::wrap(i64::MIN);
        assert_eq!((max + BigInt::wrap(0)).unwrap_value(), i64::MAX);
        assert_eq!((min + BigInt::wrap(0)).unwrap_value(), i64::MIN);

        // Large Values
        let large_num1 = BigInt::wrap(1_000_000_000);
        let large_num2 = BigInt::wrap(2_000_000_000);
        assert_eq!((large_num1 + large_num2).unwrap_value(), 3_000_000_000);
    }
}
