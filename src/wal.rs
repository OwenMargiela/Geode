// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::fs::{ File, OpenOptions };

use std::hash::Hasher;
use std::io::{ BufWriter, Read, Write };
use std::path::Path;
use std::sync::{ Arc, Mutex };

use anyhow::{ Context, Ok, Result };
use bytes::{ Buf, BufMut };

use crate::index::tree::byte_box::{ ByteBox, DataType };

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

pub const SET_COMMAND: &str = "SET";
pub const PUT_COMMAND: &str = "PUT";
pub const DEL_COMMAND: &str = "DEL";

#[derive(Debug, Clone)]
pub enum Commands {
    SET,
    PUT,
    DEL,
}

impl Commands {
    pub fn as_str(&self) -> &'static str {
        match self {
            Commands::SET => "SET",
            Commands::PUT => "PUT",
            Commands::DEL => "DEL",
        }
    }
}

#[derive(Debug, Clone)]
pub struct WalEntry {
    command: Commands,
    table_id: u32,
    key: ByteBox,
    data: Option<ByteBox>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(
                Mutex::new(
                    BufWriter::new(
                        OpenOptions::new()
                            .read(true)
                            .create_new(true)
                            .write(true)
                            .open(path)
                            .context("failed to create WAL")?
                    )
                )
            ),
        })
    }

    pub fn re_init_log(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .context("failed to recover from WAL")?;

        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn build_buf(path: impl AsRef<Path>) -> Vec<WalEntry> {
        let mut wal_entries: Vec<WalEntry> = Vec::new();
        let path = path.as_ref();
        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(path)
            .context("failed to recover from WAL")
            .unwrap();

        let mut buf = Vec::new();
        file.read_to_end(&mut buf).unwrap();

        let mut rbuf: &[u8] = buf.as_slice();

        while rbuf.has_remaining() {
            let entry_len = rbuf.get_u32() as usize;

            if rbuf.remaining() < entry_len {
                panic!("Incomplete WAL");
            }

            let mut entry = &rbuf[..entry_len];

            let mut command_buf = [0u8; 3];
            entry.read_exact(&mut command_buf).unwrap();
            let command = String::from_utf8(command_buf.to_vec()).unwrap();
            entry.advance(3);

            let table_id = entry.get_u32_le();

            let type_len = entry.get_u32_le();
            let mut data_type_raw = vec![0; type_len as usize];
            entry.read_exact(&mut data_type_raw).unwrap();
            entry.advance(type_len as usize);

            let key_len = entry.get_u32_le();
            let mut key_buf = vec![0; key_len as usize];
            entry.read_exact(&mut key_buf).unwrap();
            entry.advance(key_len as usize);

            let data_type_string = String::from_utf8(data_type_raw.clone()).unwrap();
            let data_type = map_type(&data_type_string, key_len as usize);
            let key = data_type.to_byte_box(&key_buf);

            let mut value: Option<ByteBox> = None;
            if command != "DEL" {
                let val_type_len = entry.get_u32_le();
                let mut val_type_raw = vec![0; val_type_len as usize];
                entry.read_exact(&mut val_type_raw).unwrap();
                entry.advance(val_type_len as usize);

                let val_len = entry.get_u32_le();
                let mut val_buf = vec![0; val_len as usize];
                entry.read_exact(&mut val_buf).unwrap();
                entry.advance(val_len as usize);

                let val_type_string = String::from_utf8(val_type_raw).unwrap();
                let val_type = map_type(&val_type_string, val_len as usize);
                value = Some(val_type.to_byte_box(&val_buf));
            }

            let checksum = entry.get_u32_le();

            let mut hasher = crc32fast::Hasher::new();
            hasher.write_u32(table_id);
            hasher.update(&data_type_raw);
            hasher.write_u32(key_len);
            hasher.update(&key.data);
            if let Some(ref val) = value {
                let val_type_str = val.datatype.to_string();
                hasher.update(val_type_str.as_bytes());
                hasher.write_u32(val.data_length as u32);
                hasher.update(&val.data);
            }

            if hasher.finalize() != checksum {
                panic!("Checksum mismatch");
            }

            let command = match command.as_str() {
                "DEL" => Commands::DEL,
                "SET" => Commands::SET,
                "PUT" => Commands::PUT,
                _ => panic!("Unknown Command"),
            };

            wal_entries.push(WalEntry { command, table_id, key, data: value });
            rbuf.advance(entry_len);
        }

        wal_entries
    }

    pub fn put(
        &self,
        key: ByteBox,
        value: ByteBox,
        command: Commands,
        table_id: u32
    ) -> Result<()> {
        let mut file = self.file.lock().unwrap();
        let mut buf = Vec::<u8>::new();

        match command {
            Commands::DEL => {
                buf.put_u32_le(0);
                buf.put_slice(DEL_COMMAND.as_bytes());
                buf.put_u32_le(table_id);

                let data_type = key.datatype.to_string();
                buf.put_u32_le(data_type.len() as u32);
                buf.put_slice(data_type.as_bytes());

                buf.put_u32_le(key.data_length as u32);
                buf.put_slice(&key.data);

                let mut hasher = crc32fast::Hasher::new();
                hasher.write_u32(table_id);
                hasher.update(data_type.as_bytes());
                hasher.write_u32(key.data_length as u32);
                hasher.update(&key.data);

                let checksum = hasher.finalize();
                buf.put_u32_le(checksum);

                let buf_len = buf.len() as u32;
                (&mut buf[..4]).copy_from_slice(&buf_len.to_le_bytes());

                file.write_all(&buf)?;
                return Ok(());
            }
            Commands::PUT | Commands::SET => {
                buf.put_u32_le(0);
                buf.put_slice(command.as_str().as_bytes());
                buf.put_u32_le(table_id);

                let key_type = key.datatype.to_string();
                buf.put_u32_le(key_type.len() as u32);
                buf.put_slice(key_type.as_bytes());

                buf.put_u32_le(key.data_length as u32);
                buf.put_slice(&key.data);

                let val_type = value.datatype.to_string();
                buf.put_u32_le(val_type.len() as u32);
                buf.put_slice(val_type.as_bytes());

                buf.put_u32_le(value.data_length as u32);
                buf.put_slice(&value.data);

                let mut hasher = crc32fast::Hasher::new();
                hasher.write_u32(table_id);
                hasher.update(key_type.as_bytes());
                hasher.write_u32(key.data_length as u32);
                hasher.update(&key.data);
                hasher.update(val_type.as_bytes());
                hasher.write_u32(value.data_length as u32);
                hasher.update(&value.data);

                let checksum = hasher.finalize();
                buf.put_u32_le(checksum);

                let buf_len = buf.len() as u32;
                (&mut buf[..4]).copy_from_slice(&buf_len.to_le_bytes());

                file.write_all(&buf)?;
                Ok(())
            }
        }
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock().unwrap();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}

pub(crate) fn map_type(data_type_string: &str, len: usize) -> DataType {
    match data_type_string {
        "BIGINT" => DataType::BigInt,
        "INT" => DataType::Int,
        "SMALLINT" => DataType::SmallInt,
        "DECIMAL" => DataType::Decimal,
        "CHAR" => DataType::Char(len),
        "VARCHAR" => DataType::Varchar(len),
        "BOOLEAN" => DataType::Boolean,
        _ => DataType::None,
    }
}
