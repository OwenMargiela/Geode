// #![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
// #![allow(dead_code)] // TODO(you): remove this lint after implementing this mod


// use std::io::{Error, ErrorKind, Result};

// use crate::db_types::container::{data_type_string, Serializable};
// use byteorder::{ReadBytesExt, WriteBytesExt};

// pub struct Boolean<'type_id> {
//     store: u8,
//     type_id_string: &'type_id str,
// }

// impl<'a> Boolean<'a> {
//     pub fn wrap(new_data: bool) -> Boolean<'a> {
//         let bool_val: u8;
//         match new_data {
//             true => bool_val = 1,
//             false => bool_val = 0,
//         }

//         Boolean {
//             store: bool_val,
//             type_id_string: data_type_string::BOOLEAN,
//         }
//     }

//     pub fn unwrap_value(&self) -> bool {
//         if self.store == 1 {
//             true
//         } else {
//             false
//         }
//     }

//     pub fn get_type(&self) -> &str {
//         self.type_id_string
//     }
// }

// impl<'a> Serializable for Boolean<'a> {
//     fn deserialize(cursor: &mut std::io::Cursor<&[u8]>) -> Result<Self>
//     where
//         Self: Sized,
//     {
//         if cursor.get_ref().len() < 1 {
//             return Err(Error::new(
//                 ErrorKind::UnexpectedEof,
//                 "Not enough data for i32 desrialization",
//             ));
//         }

//         let value = cursor.read_u8()?;

//         match value {
//             1 => Ok(Boolean::wrap(true)),
//             2 => Ok(Boolean::wrap(false)),
//             _ => Ok(Boolean::wrap(false)),
//         }
//     }

//     fn serialize(&self, buffer: &mut Vec<u8>) -> Result<()> {
//         buffer.write_u8(self.store)?;
//         Ok(())
//     }
// }

// impl<'a> Copy for Boolean<'a> {}

// impl<'a> Clone for Boolean<'a> {
//     fn clone(&self) -> Self {
//         *self
//     }
// }
