#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use crate::index::tree::byte_box::{ ByteBox, DataType };

#[derive(Clone, Debug, PartialEq)]
pub struct Column {
    /// Column name. Can't be empty.
    pub name: String,
    /// Column datatype.
    pub datatype: DataType,
}

pub struct SchemaBuilder {
    colummns: Vec<Column>,
}

pub struct Schema {
    pub columns: Vec<Column>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SchemaDataValue {
    pub name: String,
    pub data: ByteBox,
}

impl SchemaBuilder {
    pub(crate) fn new() -> Self {
        SchemaBuilder { colummns: Vec::new() }
    }

    pub(crate) fn add_big_int(mut self, name: String) -> Self {
        self.colummns.push(Column { name, datatype: DataType::BigInt });
        self
    }

    pub(crate) fn add_int(mut self, name: String) -> Self {
        self.colummns.push(Column { name, datatype: DataType::Int });
        self
    }

    pub(crate) fn add_small_int(mut self, name: String) -> Self {
        self.colummns.push(Column { name, datatype: DataType::SmallInt });
        self
    }

    pub(crate) fn add_decimal(mut self, name: String) -> Self {
        self.colummns.push(Column { name, datatype: DataType::Decimal });
        self
    }

    pub(crate) fn add_bool(mut self, name: String) -> Self {
        self.colummns.push(Column { name, datatype: DataType::Boolean });
        self
    }

    pub(crate) fn add_char(mut self, name: String, size: usize) -> Self {
        self.colummns.push(Column { name, datatype: DataType::Char(size) });
        self
    }

    pub(crate) fn add_varchar(mut self, name: String, size: usize) -> Self {
        self.colummns.push(Column { name, datatype: DataType::Varchar(size) });
        self
    }

    pub(crate) fn build(mut self) -> Schema {
        self.colummns.sort_by(|a, b| {
            if a.datatype.to_string() == "VARCHAR" && b.datatype.to_string() != "VARCHAR" {
                std::cmp::Ordering::Greater
            } else if a.datatype.to_string() != "VARCHAR" && b.datatype.to_string() == "VARCHAR" {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Equal
            }
        });

        Schema { columns: self.colummns }
    }
}

impl Schema {
    pub(crate) fn get_col_idx(&self, col_name: String) -> Option<u8> {
        let idx: u8;
        for col_idx in 0..self.columns.len() {
            let col = self.columns.get(col_idx).unwrap();
            if col.name == col_name {
                idx = col_idx as u8;
                return Some(idx);
            }
        }

        None
    }

    pub(crate) fn validate_fields(&self, values: &Vec<ByteBox>) -> bool {
        let schema_len = self.columns.len();

        if schema_len != values.len() {
            return false;
        }

        for i in 0..schema_len {
            let elem = &values[i];
            let cur_column = self.columns.get(i).unwrap();

            if elem.datatype != cur_column.datatype {
                return false;
            }
        }

        true
    }
}
