#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::collections::HashMap;

use header_const::{
    BASE_VERSION, DATA_PTR_OFFSET, HEADER_LENGTH, HEADER_LENGTH_OFFSET, TUPLE_LENGTH,
    VARLENGTH_FIELDS, VARLEN_OFFSET,
};

use crate::{
    catalog::schema::Schema,
    db_types::container::{data_type_string, ByteBox, SchemaDataValue},
};

pub struct Tuple {
    // Row Identification - unimplemented
    pub data: Vec<u8>,
}

pub mod header_const {
    // Sizes
    pub const HEADER_LENGTH: usize = 2;
    pub const TUPLE_LENGTH: usize = 2;
    pub const BASE_VERSION: usize = 1;
    pub const VARLENGTH_FIELDS: usize = 1;

    // Offsets
    pub const TUPLE_LENGTH_OFFSET: usize = 0;
    pub const HEADER_LENGTH_OFFSET: usize = 2;
    pub const VERSION_OFFSET: usize = TUPLE_LENGTH + HEADER_LENGTH;
    pub const VARLEN_OFFSET: usize = TUPLE_LENGTH + HEADER_LENGTH + BASE_VERSION;
    pub const DATA_PTR_OFFSET: usize =
        HEADER_LENGTH + TUPLE_LENGTH + BASE_VERSION + VARLENGTH_FIELDS;
}

pub fn schema_reorder(input_values: &mut Vec<SchemaDataValue>, schema: &Schema) {
    let len = schema.length;
    let mut map: HashMap<String, SchemaDataValue> = HashMap::with_capacity(len);

    for schema_val in input_values.drain(0..len) {
        map.insert(schema_val.column_name.to_string(), schema_val);
    }

    assert_eq!(input_values.len(), 0 as usize);

    for column_ in schema.get_columns() {
        let val = map.remove(&column_.column_name.to_string()).expect("No such key exists");
        input_values.push(val);
    }
}

pub fn extract_byte_box_data(mut input_values: Vec<SchemaDataValue>) -> Vec<ByteBox> {
    let len = input_values.len();
    let mut output: Vec<ByteBox> = Vec::with_capacity(len);

    for val in input_values.drain(0..len) {
        output.push(val.data);
    }

    output
}

impl Tuple {
    // Values must be inserted in schema order
    // Contemplating making a function that resorts values in the order of the schema
    pub fn copy(other_tuple: &mut Tuple) -> Tuple {
        let mut tuple = Tuple {
            data: Vec::with_capacity(other_tuple.data.len()),
        };
        tuple.data.copy_from_slice(&other_tuple.data);

        tuple
    }

    pub fn build(values: &mut Vec<ByteBox>, schema: &mut Schema) -> Result<Tuple, String> {
        // First Validation check
        let error_string = String::from("Mismatch between schema and data values");
        let schema_len = schema.length;
        let mut data_ptrs: Vec<u8> = Vec::with_capacity(schema_len);

        if schema_len != values.len() {
            return Err(error_string);
        }

        // // Body Serialization
        let (mut tuple_body, num_varlen_fields) =
            Tuple::serialize_body(&values, &mut data_ptrs).expect("Error serializing body");

        // Header
        let mut header: Vec<u8> = Vec::new();

        let tuple_len = tuple_body.len() as u16;

        Tuple::serialize_header(&mut header, schema, data_ptrs, tuple_len, num_varlen_fields);

        // Tuple construction
        let mut entire_tuple: Vec<u8> = Vec::with_capacity(header.len() + tuple_body.len());

        entire_tuple.append(&mut header);
        entire_tuple.append(&mut tuple_body);

        let tuple = Tuple { data: entire_tuple };
        Ok(tuple)
    }

    // Consider making every element in the u8 data have a length prefix
    // but not now cuase im tired with section of the code base in particular
    pub fn get(&self, idx: usize, length: usize) -> Option<&[u8]> {
        let header_len = self.data[HEADER_LENGTH_OFFSET] as usize;
        let num_varlen_ptrs = self.data[VARLEN_OFFSET];
        let data_ptrs = &self.data[DATA_PTR_OFFSET..header_len];
        let field: &[u8];

        if idx > data_ptrs.len() {
            return None;
        }
        // Checks if the index is for a var length field

        if idx < data_ptrs.len() - num_varlen_ptrs as usize {
            let offset: usize = data_ptrs[idx] as usize + header_len;

            field = &self.data[offset..offset + length];
            println!("column {} {:?}", idx, field);

            return Some(field);
        } else {
            let running_count = idx - (data_ptrs.len() - num_varlen_ptrs as usize);

            let mut offset: usize = header_len + data_ptrs[idx] as usize;
            let len: usize =
                self.data[running_count + header_len + data_ptrs[idx] as usize] as usize;

            offset += 1;

            field = &self.data[running_count + offset..offset + len + running_count];

            Some(field)
        }
    }

    fn serialize_body(
        values: &Vec<ByteBox>,
        data_ptrs: &mut Vec<u8>,
    ) -> Result<(Vec<u8>, usize), String> {
        let mut tuple_body: Vec<u8> = Vec::new();
        let mut current_offset = 0;
        let mut num_varlen_fields = 0;

        for i in 0..values.len() {
            let bytebox = &values[i];
            data_ptrs.push(current_offset);

            if bytebox.datatype == data_type_string::VARCHAR {
                // Increment number of varlen fields
                num_varlen_fields += 1;
                // Append the len fields first
                let data_len = bytebox.data_length as u8;
                let len_field = data_len.to_le_bytes();
                tuple_body.extend(len_field);
            }

            let data = &mut bytebox.data.clone();
            tuple_body.append(data);

            current_offset += values[i].data_length;
        }

        Ok((tuple_body, num_varlen_fields))
    }

    fn serialize_header(
        header: &mut Vec<u8>,
        schema: &Schema,
        data_ptrs: Vec<u8>,
        tuple_len: u16,
        num_varlen_fields: usize,
    ) {
        let dat_len = if data_ptrs.is_empty() {
            1
        } else {
            data_ptrs.len() * 1
        };

        let header_len: usize =
            HEADER_LENGTH + TUPLE_LENGTH + BASE_VERSION + VARLENGTH_FIELDS + dat_len;

        let u16_header_length = header_len as u16;

        let tuple_len = u16_header_length + tuple_len as u16;

        header.reserve(header_len);

        // Tuple length
        let mut len = tuple_len.to_le_bytes();
        header.extend(len);

        // header length
        len = u16_header_length.to_le_bytes();
        header.extend(len);

        let version = schema.version.to_le_bytes();
        header.extend(version);

        // Add number of varlen fields
        let data = num_varlen_fields as u8;
        header.push(data);

        // Data pointers
        for ptr in data_ptrs {
            header.extend(ptr.to_le_bytes());
        }
    }
}
