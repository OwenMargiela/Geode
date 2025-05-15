#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cell::RefCell;

use crate::db_types::container::{data_type_string, ByteBox};

#[derive(Clone, Copy)]

pub struct Column<'a> {
    pub column_name: &'a str,
    pub column_type: &'a str,
}

impl<'a> Column<'a> {
    pub(crate) fn new(name: &'a str, col_type: &'a str) -> Self {
        let fixed_length_bool: bool;

        Self {
            column_name: &name,
            column_type: &col_type,
        }
    }
    pub(crate) fn get_column_name(&self) -> &str {
        &self.column_name
    }
    pub(crate) fn get_column_type(&self) -> &str {
        &self.column_type
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

        self.columns
            .push(Column::new(column_name, data_type_string::BIGINT));
        self
    }

    pub(crate) fn add_int(mut self, column_name: &'a str) -> Self {
        let schema_length = self.columns.len();

        self.columns
            .push(Column::new(column_name, data_type_string::INT));
        self
    }

    pub(crate) fn add_small_int(mut self, column_name: &'a str) -> Self {
        let schema_length = self.columns.len();

        self.columns
            .push(Column::new(column_name, data_type_string::SMALLINT));
        self
    }

    pub(crate) fn add_decimal(mut self, column_name: &'a str) -> Self {
        let schema_length = self.columns.len();

        self.columns
            .push(Column::new(column_name, data_type_string::DECIMAL));
        self
    }

    pub(crate) fn add_boolean(mut self, column_name: &'a str) -> Self {
        let schema_length = self.columns.len();

        self.columns
            .push(Column::new(column_name, data_type_string::BOOLEAN));
        self
    }

    pub(crate) fn add_char(mut self, column_name: &'a str, size: u8) -> Self {
        let schema_length = self.columns.len();

        self.columns
            .push(Column::new(column_name, data_type_string::CHAR));

        self
    }

    pub(crate) fn add_varchar(mut self, column_name: &'a str, size: u8) -> Self {
        let schema_length = self.columns.len();

        self.columns
            .push(Column::new(column_name, data_type_string::VARCHAR));

        self
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

    pub fn get_columns(&self) -> &Vec<Column> {
        &self.columns
    }
}
