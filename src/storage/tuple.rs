#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{ collections::HashMap, io::{ Read, Write }, mem };

use bytes::{ Buf, BufMut };

use crate::{
    catalog::schema::{ Schema, SchemaBuilder, SchemaData, SchemaDataValue },
    index::tree::byte_box::{ ByteBox, DataType },
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Tuple {
    TupleData(SchemaData),
}

impl Tuple {
    pub fn get(&self, idx: usize) -> &ByteBox {
        match self {
            Tuple::TupleData(data) => {
                let data_value = data.get(idx).unwrap();
                return &data_value.data;
            }
        }
    }
    pub fn encode(&self) -> Vec<u8> {
        let mut whole_tuple = Vec::<u8>::new();
        match self {
            Tuple::TupleData(values) => {
                let schema_len = values.len();
                let mut data: Vec<u8> = Vec::with_capacity(schema_len);

                let mut schema_data_value = Vec::<SchemaDataValue>::new();

                if schema_len != values.len() {
                    panic!("Mismatch between tuple and schema");
                }

                for value in values.clone().into_iter() {
                    let column_name_length = value.name.as_bytes().len();
                    data.put_u32_le(column_name_length as u32);

                    let value_name = value.name.as_bytes();
                    data.write_all(value_name).unwrap();

                    if
                        value.data.datatype.to_string() == "VARCHAR" ||
                        value.data.datatype.to_string() == "CHAR"
                    {
                        data.put_u32_le(value.data.data_length as u32);
                    }

                    schema_data_value.push(SchemaDataValue {
                        name: value.data.datatype.to_string(),
                        data: value.data.clone(),
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
        let mut entry = &tuple_data[..];
        let mut schema_data_array = Vec::<SchemaDataValue>::new();
        for column in &schema.columns {
            let column_name_len = entry.get_u32_le();
            let mut name_raw = vec![0u8; column_name_len as usize];

            entry.read_exact(&mut name_raw).unwrap();

            let column_name = String::from_utf8(name_raw).unwrap();

            let data = match column.datatype {
                DataType::BigInt => { ByteBox::big_int(entry.get_i64_le()) }
                DataType::Int => { ByteBox::int(entry.get_i32_le()) }
                DataType::SmallInt => { ByteBox::small_int(entry.get_i16_le()) }

                DataType::Decimal => { ByteBox::decimal(entry.get_f32_le()) }
                DataType::Boolean => { ByteBox::boolean(entry.get_u8() == 1) }

                DataType::Char(size) | DataType::Varchar(size) => {
                    let data_len = entry.get_u32_le() as usize;
                    let mut buf = vec![0u8; data_len];

                    entry.read_exact(&mut buf).unwrap();

                    if &column.datatype.to_string() == "VARCHAR" {
                        ByteBox::varchar(&String::from_utf8(buf).unwrap(), size)
                    } else {
                        ByteBox::char(&String::from_utf8(buf).unwrap(), size)
                    }
                }

                _ => {
                    panic!("Invalid datatype for schema");
                }
            };

            schema_data_array.push(SchemaDataValue {
                name: column_name,
                data,
            });
        }

        Tuple::TupleData(schema_data_array)
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

pub fn to_flat_schema(tuple: Tuple) -> Schema {
    match tuple {
        Tuple::TupleData(schema_data) => {
            let mut schema = SchemaBuilder::new();
            for column in schema_data {
                match column.data.datatype {
                    DataType::BigInt => {
                        schema.add_big_int(column.name);
                    }

                    DataType::Boolean => {
                        schema.add_bool(column.name);
                    }

                    DataType::Char(size) => {
                        schema.add_char(column.name, size);
                    }

                    DataType::Decimal => {
                        schema.add_decimal(column.name);
                    }
                    DataType::SmallInt => {
                        schema.add_small_int(column.name);
                    }
                    DataType::Varchar(size) => {
                        schema.add_varchar(column.name, size);
                    }
                    _ => {
                        panic!("Unsupported type");
                    }
                }
            }

            schema.build()
        }
    }
}

#[cfg(test)]
pub mod test {
    use crate::{
        catalog::schema::SchemaDataBuilder,
        index::tree::byte_box::ByteBox,
        storage::tuple::{ to_flat_schema, Tuple },
    };

    #[test]
    fn tuple_works() {
        let tuple = Tuple::TupleData(
            SchemaDataBuilder::new()
                .add_big_int(String::from("Money"), ByteBox::big_int(1_000_000))
                .add_small_int(String::from("Age"), ByteBox::small_int(25))
                .build()
        );

        let tuple_two = Tuple::TupleData(
            SchemaDataBuilder::new()
                .add_big_int(String::from("Money"), ByteBox::big_int(1_000_000))
                .add_small_int(String::from("Age"), ByteBox::small_int(25))
                .build()
        );

        assert_eq!(tuple, tuple_two);

        let schema = to_flat_schema(tuple);

        println!("{:?}", schema);
    }
}
