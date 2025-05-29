#![allow(unused_variables)] 
#![allow(dead_code)] 

use bincode::{ Decode, Encode };

use crate::index::tree::byte_box::{ ByteBox, DataType };

#[derive(Clone, Encode, Decode, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Column {
    /// Column name. Can't be empty.
    pub name: String,
    /// Column datatype.
    pub datatype: DataType,
}

pub struct SchemaBuilder {
    colummns: Vec<Column>,
}

#[derive(Debug, Clone, Encode, Decode, PartialEq, Eq, PartialOrd, Ord)]
pub struct Schema {
    pub columns: Vec<Column>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SchemaDataValue {
    pub name: String,
    pub data: ByteBox,
}

pub type SchemaData = Vec<SchemaDataValue>;

pub struct SchemaDataBuilder {
    pub columns: Vec<SchemaDataValue>,
}

impl SchemaDataBuilder {
    pub(crate) fn new() -> Self {
        SchemaDataBuilder { columns: Vec::new() }
    }

    pub(crate) fn add_big_int(mut self, name: String, data: ByteBox) -> Self {
        match data.datatype {
            DataType::BigInt => {
                self.columns.push(SchemaDataValue { name, data });
            }

            _ => panic!("Type Mismatch"),
        }
        self
    }

    pub(crate) fn add_int(mut self, name: String, data: ByteBox) -> Self {
        match data.datatype {
            DataType::Int => {
                self.columns.push(SchemaDataValue { name, data });
            }

            _ => panic!("Type Mismatch"),
        }
        self
    }

    pub(crate) fn add_small_int(mut self, name: String, data: ByteBox) -> Self {
        match data.datatype {
            DataType::SmallInt => {
                self.columns.push(SchemaDataValue { name, data });
            }

            _ => panic!("Type Mismatch"),
        }
        self
    }

    pub(crate) fn add_decimal(mut self, name: String, data: ByteBox) -> Self {
        match data.datatype {
            DataType::Decimal => {
                self.columns.push(SchemaDataValue { name, data });
            }

            _ => panic!("Type Mismatch"),
        }
        self
    }

    pub(crate) fn add_bool(mut self, name: String, data: ByteBox) -> Self {
        match data.datatype {
            DataType::Boolean => {
                self.columns.push(SchemaDataValue { name, data });
            }

            _ => panic!("Type Mismatch"),
        }
        self
    }

    pub(crate) fn add_char(mut self, name: String, data: ByteBox) -> Self {
        match data.datatype {
            DataType::Char(_) => {
                self.columns.push(SchemaDataValue { name, data });
            }

            _ => panic!("Type Mismatch"),
        }
        self
    }

    pub(crate) fn add_varchar(mut self, name: String, data: ByteBox) -> Self {
        match data.datatype {
            DataType::Varchar(_) => {
                self.columns.push(SchemaDataValue { name, data });
            }

            _ => panic!("Type Mismatch"),
        }
        self
    }

    pub(crate) fn build(mut self) -> SchemaData {
        self.columns.sort_by(|a, b| {
            if
                a.data.datatype.to_string() == "VARCHAR" &&
                b.data.datatype.to_string() != "VARCHAR"
            {
                std::cmp::Ordering::Greater
            } else if
                a.data.datatype.to_string() != "VARCHAR" &&
                b.data.datatype.to_string() == "VARCHAR"
            {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Equal
            }
        });

        self.columns
    }
}
impl SchemaBuilder {
    pub(crate) fn new() -> Self {
        SchemaBuilder { colummns: Vec::new() }
    }

    pub(crate) fn add_big_int(&mut self, name: String) -> &Self {
        self.colummns.push(Column { name, datatype: DataType::BigInt });
        self
    }

    pub(crate) fn add_int(&mut self, name: String) -> &Self {
        self.colummns.push(Column { name, datatype: DataType::Int });
        self
    }

    pub(crate) fn add_small_int(&mut self, name: String) -> &Self {
        self.colummns.push(Column { name, datatype: DataType::SmallInt });
        self
    }

    pub(crate) fn add_decimal(&mut self, name: String) -> &Self {
        self.colummns.push(Column { name, datatype: DataType::Decimal });
        self
    }

    pub(crate) fn add_bool(&mut self, name: String) -> &Self {
        self.colummns.push(Column { name, datatype: DataType::Boolean });
        self
    }

    pub(crate) fn add_char(&mut self, name: String, size: usize) -> &Self {
        self.colummns.push(Column { name, datatype: DataType::Char(size) });
        self
    }

    pub(crate) fn add_varchar(&mut self, name: String, size: usize) -> &Self {
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
