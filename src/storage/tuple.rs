#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{ collections::HashMap, io::Cursor, mem };

use anyhow::Ok;
use bytes::BufMut;

use crate::{ catalog::schema::{ Schema, SchemaDataValue }, index::tree::byte_box::ByteBox };

pub enum Tuple {
    TupleData(Vec<SchemaDataValue>, Schema),
}

impl Tuple {
    pub fn encode(&self) -> Vec<u8> {
        let mut whole_tuple = Vec::<u8>::new();
        match self {
            Tuple::TupleData(values, schema) => {
                let schema_len = schema.columns.len();
                let mut data: Vec<u8> = Vec::with_capacity(schema_len);

                let mut schema_data_value = Vec::<SchemaDataValue>::new();

                if schema_len != values.len() {
                    panic!("Mismatch between tuple and schema");
                }

                for value in values.clone().into_iter() {
                    if value.data.datatype.to_string() == "VARCHAR" {
                        data.put_u32_le(value.data.data_length as u32);
                    }

                    schema_data_value.push(SchemaDataValue {
                        name: value.data.datatype.to_string(),
                        data: value.data.clone()
                    });

                    data.put(value.data.data);
                }

                let tuple_len = data.len() as u32;

                whole_tuple.reserve((tuple_len as usize) + mem::size_of::<usize>());

                whole_tuple.put_u32_le(tuple_len);

                whole_tuple.append(&mut data);
            }
        }

        whole_tuple
    }

    pub fn decode(schema: &Schema, tuple_data: Vec<u8>) -> Tuple {
        unimplemented!()
    }
}

pub fn schema_reorder(input: &mut Vec<SchemaDataValue>, schema: &Schema) {
    let len = schema.columns.len();
    let mut map: HashMap<String, SchemaDataValue> = HashMap::with_capacity(len);

    for schema_val in input.drain(0..len) {
        let name = schema_val.name.clone();
        map.insert(name, schema_val);
    }

    assert_eq!(input.len(), 0 as usize);

    for column in schema.columns.clone().into_iter() {
        let val = map.remove(&column.name).expect("No such key exists");
        input.push(val);
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
