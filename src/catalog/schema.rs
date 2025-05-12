#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cell::RefCell;

use crate::db_types::container::{data_type_string, ByteBox};

#[derive(Clone, Copy)]

pub struct Column<'a> {
    pub column_name: &'a str,
    pub column_type: &'a str,
    pub is_fixed_length: bool,
    pub is_numeric_type: bool,
    pub is_nullable: bool,
    pub size: u8,
    pub offset_position_in_tuple: u8,
}

impl<'a> Column<'a> {
    pub(crate) fn new(name: &'a str, col_type: &'a str, offset_position: u8) -> Self {
        let fixed_length_bool: bool;

        match col_type {
            data_type_string::VARCHAR => fixed_length_bool = false,
            _ => fixed_length_bool = true,
        }

        let numeric_type_bool;

        match col_type {
            data_type_string::VARCHAR => numeric_type_bool = false,
            data_type_string::CHAR => numeric_type_bool = false,
            data_type_string::BOOLEAN => numeric_type_bool = false,
            _ => numeric_type_bool = true,
        }

        Self {
            column_name: &name,
            column_type: &col_type,
            is_fixed_length: fixed_length_bool,
            is_numeric_type: numeric_type_bool,
            offset_position_in_tuple: offset_position,
            is_nullable: false,
            size: u8::MIN,
        }
    }
    pub(crate) fn get_column_name(&self) -> &str {
        &self.column_name
    }
    pub(crate) fn get_column_type(&self) -> &str {
        &self.column_type
    }

    pub(crate) fn get_is_fixed_length(&self) -> &bool {
        &self.is_fixed_length
    }

    pub(crate) fn get_is_numeric_type(&self) -> &bool {
        &self.is_numeric_type
    }

    pub(crate) fn get_offset_position_in_tuple(&self) -> &u8 {
        &self.offset_position_in_tuple
    }

    pub(crate) fn set_offset_potion_in_tuple(&mut self, new_position: u8) {
        self.offset_position_in_tuple = new_position;
    }

    pub(crate) fn get_size(&self) -> &u8 {
        &self.size
    }

    pub(crate) fn set_size(&mut self, new_size: u8) {
        self.size = new_size;
    }
}

pub struct SchemaBuilder<'a> {
    columns: Vec<Column<'a>>,
}
impl<'a> SchemaBuilder<'a> {
    pub(crate) fn new() -> Self {
        Self {
            columns: Vec::new(),
        }
    }

    pub(crate) fn add_big_int(mut self, column_name: &'a str) -> Self {
        let schema_length = self.columns.len();

        self.columns.push(Column::new(
            column_name,
            data_type_string::BIGINT,
            schema_length as u8,
        ));
        self
    }

    pub(crate) fn add_int(mut self, column_name: &'a str) -> Self {
        let schema_length = self.columns.len();

        self.columns.push(Column::new(
            column_name,
            data_type_string::INT,
            schema_length as u8,
        ));
        self
    }

    pub(crate) fn add_small_int(mut self, column_name: &'a str) -> Self {
        let schema_length = self.columns.len();

        self.columns.push(Column::new(
            column_name,
            data_type_string::SMALLINT,
            schema_length as u8,
        ));
        self
    }

    pub(crate) fn add_decimal(mut self, column_name: &'a str) -> Self {
        let schema_length = self.columns.len();

        self.columns.push(Column::new(
            column_name,
            data_type_string::DECIMAL,
            schema_length as u8,
        ));
        self
    }

    pub(crate) fn add_boolean(mut self, column_name: &'a str) -> Self {
        let schema_length = self.columns.len();

        self.columns.push(Column::new(
            column_name,
            data_type_string::BOOLEAN,
            schema_length as u8,
        ));
        self
    }

    pub(crate) fn add_char(mut self, column_name: &'a str, size: u8) -> Self {
        let schema_length = self.columns.len();

        self.columns.push(Column::new(
            column_name,
            data_type_string::CHAR,
            schema_length as u8,
        ));

        if let Some(col) = self.columns.last_mut() {
            col.set_size(size);
        }

        self
    }

    pub(crate) fn add_varchar(mut self, column_name: &'a str, size: u8) -> Self {
        let schema_length = self.columns.len();

        self.columns.push(Column::new(
            column_name,
            data_type_string::VARCHAR,
            schema_length as u8,
        ));

        if let Some(col) = self.columns.last_mut() {
            col.set_size(size);
        }

        self
    }

    // Makes the previously added column nullable

    pub(crate) fn set_null(self) -> Self {
        let res = self.set_null_result().unwrap();
        res
    }

    pub(self) fn set_null_result(mut self) -> Result<Self, String> {
        let columns = &mut self.columns;
        let len = columns.len();
        if len < 1 {
            return Err(String::from("There exist no previous column"));
        }

        columns[len - 1].is_nullable = true;

        Ok(self)
    }

    pub(crate) fn build(mut self) -> Schema<'a> {
        let size = self.columns.len();

        // Varchar push down
        let num_of_vars = RefCell::new(0);

        self.columns.sort_by(|a, b| {
            if a.column_type == "VARCHAR" && b.column_type != "VARCHAR" {
                *num_of_vars.borrow_mut() += 1;
                std::cmp::Ordering::Greater
            } else if a.column_type != "VARCHAR" && b.column_type == "VARCHAR" {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Equal
            }
        });

        Schema {
            columns: self.columns,
            length: size,
            version: 0,
            number_of_var_length_fields: num_of_vars.take(),
        }
    }
}
pub struct Schema<'a> {
    pub length: usize,
    // Schemas and shrink and grow over time,
    // best to use a vec
    pub columns: Vec<Column<'a>>,
    pub version: u8,
    pub number_of_var_length_fields: u8,
}

impl<'a> Schema<'a> {
    pub(crate) fn get_col_idx(&self, col_name: &str) -> Option<u8> {
        let idx: u8;
        for col_idx in 0..self.columns.len() {
            let col = self.columns.get(col_idx).unwrap();
            if col.column_name == col_name {
                idx = col_idx as u8;
                return Some(idx);
            }
        }

        None
    }

    pub(crate) fn icrement_version(&mut self) {
        self.version += 1;
    }

    pub(crate) fn validate_fields(&self, values: &Vec<ByteBox>) -> bool {
        let schema_len = self.length;

        if schema_len != values.len() {
            return false;
        }

        for i in 0..schema_len {
            let elem = &values[i];
            let cur_column = self.columns[i];

            if elem.datatype != cur_column.column_type {
                return false;
            }
        }

        true
    }

    // pub(crate) fn get_variable_length_offset(&self, values: &Vec<ByteBox>) -> Vec<u8> {
    //     let mut varlen_offsets: Vec<u8> = Vec::new();
    //     let mut current_offset = 0;

    //     for (i, column) in self.columns.iter().enumerate() {
    //         if column.column_type == data_type_string::VARCHAR {
    //             varlen_offsets.push(current_offset as u8);

    //             current_offset += values[i].data_length + 1;
    //         // account for length field
    //         } else {
    //             current_offset += values[i].data_length; // only the data length, no length field prefix
    //         }
    //     }

    //     varlen_offsets
    // }

    pub fn get_columns(&self) -> &Vec<Column> {
        &self.columns
    }
}
