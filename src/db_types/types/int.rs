// // use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
// // use std::ops::{Add, Mul,Div, Rem, Sub};

// // use crate::db_types::container::{type_ids, DataBox, DataContainer, Releasable};
// // use crate::db_types::marshaling::Serializable;

// use std::{
//     io::{Cursor, Error, ErrorKind, Result},
//     ops::{Add, Div, Mul, Rem, Sub},
// };

// use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

// use crate::db_types::container::{
//     data_type_string, ByteBox, DataBox, NumbericType, NumerOps, Serializable,
// };

// use super::{bigint::BigInt, decimal::Decimal, smallint::SmallInt};

// // pub struct BigIntContainer(i64);
// pub struct Int<'type_id> {
//     store: i32,
//     pub type_id_string: &'type_id str,
// }

// impl<'a> Serializable for Int<'a> {
//     fn serialize(&self, buffer: &mut Vec<u8>) -> Result<()> {
//         buffer.write_i32::<LittleEndian>(self.store)?;
//         Ok(())
//     }

//     fn deserialize(cursor: &mut Cursor<&[u8]>) -> Result<Self>
//     where
//         Self: Sized,
//     {
//         if cursor.get_ref().len() < 4 {
//             return Err(Error::new(
//                 ErrorKind::UnexpectedEof,
//                 "Not enough data for i32 desrialization",
//             ));
//         }
//         let value = cursor.read_i32::<LittleEndian>()?;
//         Ok(Int::wrap(value))
//     }
// }

// impl<'a> DataBox< Int<'a>> for Int<'a> {
//     type Output = i32;
//     type Input = i32;

//     fn cast_container(&self, type_id: &str) -> Option<&str> {
//         //         // Depending on the type id of the second parameter
//         //         // We will determine whether or not the current value
//         //         // of the dat encapsulated within the Int datatype
//         //         // can be stored withing other types
//         //         // This implementation is should be exhaustive

//         match type_id {
//             data_type_string::BIGINT => Some(data_type_string::BIGINT),
//             data_type_string::INT => Some(data_type_string::INT),
//             data_type_string::SMALLINT => Some(data_type_string::SMALLINT),
//             data_type_string::DECIMAL => Some(data_type_string::DECIMAL),
//             _ => None,
//         }

//         // if self.is_coercable_to(type_id) {
//         //     //  Since the other types have not be implemented just as yet, lets return somthing generic
//         //     Some(DataType::INT)
//         // } else {
//         //     None
//         // }
//     }

//     fn unwrap_value(&self) -> i32 {
//         self.store
//     }

//     fn wrap(new_data: i32) -> Self {
//         Int {
//             store: new_data,
//             type_id_string: data_type_string::INT,
//         }
//     }
//     fn is_coercable_to(&self, type_id: &str) -> bool {
//         match type_id {
//             data_type_string::INT => true,

//             data_type_string::SMALLINT => {
//                 let value = self.unwrap_value();
//                 value >= i16::MIN as i32 && value <= i16::MAX as i32
//             }
//             data_type_string::DECIMAL => true,
//             data_type_string::BIGINT => true,
//             _ => false,
//         }
//     }

//     fn get_type(&self) -> &str {
//         self.type_id_string
//     }

//     fn to_byte_box(&self) -> ByteBox {
//         let mut buffer: Vec<u8> = Vec::new();
//         self.serialize(&mut buffer)
//             .expect("Error during serialization");

//         ByteBox::new(buffer, self.type_id_string.to_string())
//     }
// }

// impl<'a> NumbericType<'a> for Int<'a> {
//     type Value = Int<'a>;

//     fn as_big_int(&self) -> Option<BigInt> {
//         Some(BigInt::wrap(self.store as i64))
//     }

//     fn as_decimal(&self) -> Option<Decimal> {
//         Some(Decimal::wrap(self.store as f32))
//     }

//     fn as_int(&self) -> Option<Int> {
//         Some(Self {
//             store: self.store,
//             type_id_string: self.type_id_string,
//         })
//     }

//     fn as_small_int(&self) -> Option<SmallInt> {
//         if self.is_coercable_to(data_type_string::SMALLINT) {
//             Some(SmallInt::wrap(self.store as i16))
//         } else {
//             None
//         }
//     }
// }

// impl<'a> PartialOrd for Int<'a> {
//     fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
//         Some(self.unwrap_value().cmp(&other.unwrap_value()))
//     }
// }

// impl<'a> Ord for Int<'a> {
//     fn cmp(&self, other: &Self) -> std::cmp::Ordering {
//         self.unwrap_value().cmp(&other.unwrap_value())
//     }
// }

// impl<'a> PartialEq for Int<'a> {
//     fn eq(&self, other: &Self) -> bool {
//         self.unwrap_value() == other.unwrap_value()
//     }
// }

// impl<'a> Eq for Int<'a> {}

// impl<'a> NumerOps<'a> for Int<'a> {}

// impl<'a> Add for Int<'a> {
//     type Output = Int<'a>;

//     fn add(self, rhs: Self) -> Self::Output {
//         let res = self.unwrap_value() + rhs.unwrap_value();
//         Int::wrap(res)
//     }
// }

// impl<'a> Sub for Int<'a> {
//     type Output = Int<'a>;
//     fn sub(self, rhs: Self) -> Self::Output {
//         let res = self.unwrap_value() - rhs.unwrap_value();
//         Int::wrap(res)
//     }
// }

// impl<'a> Div for Int<'a> {
//     type Output = Int<'a>;
//     fn div(self, rhs: Self) -> Self::Output {
//         let res = self.unwrap_value() / rhs.unwrap_value();
//         Int::wrap(res)
//     }
// }

// impl<'a> Mul for Int<'a> {
//     type Output = Int<'a>;
//     fn mul(self, rhs: Self) -> Self::Output {
//         let res = self.unwrap_value() * rhs.unwrap_value();
//         Int::wrap(res)
//     }
// }

// impl<'a> Rem for Int<'a> {
//     type Output = Int<'a>;
//     fn rem(self, rhs: Self) -> Self::Output {
//         let res = self.unwrap_value() % rhs.unwrap_value();
//         Int::wrap(res)
//     }
// }

// // Copy Trait

// impl<'a> Copy for Int<'a> {}

// impl<'a> Clone for Int<'a> {
//     fn clone(&self) -> Self {
//         *self
//     }
// }
