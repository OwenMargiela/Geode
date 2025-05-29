#![allow(unused_variables)] 
#![allow(dead_code)] 

use std::{ collections::HashMap, io::{ Read, Write }, mem };

use bytes::{ Buf, BufMut, Bytes };

use crate::{
    catalog::schema::{ Schema, SchemaBuilder, SchemaData, SchemaDataValue },
    index::tree::byte_box::{ ByteBox, DataType },
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Tuple {
    TupleData(SchemaData),
}

impl ByteBox {
    pub fn tuple(tuple: &Tuple) -> ByteBox {
        let tuple_schema = to_flat_schema(&tuple);

        let tuple_raw = tuple.encode();
        let tuple_len = tuple_raw.len();

        let tuple_bytes = Bytes::from(tuple_raw);
        ByteBox {
            data: tuple_bytes,
            datatype: DataType::Tuple(tuple_schema),
            data_size: tuple_len,
            data_length: tuple_len as u8,
        }
    }
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
                let mut data: Vec<u8> = Vec::new();

                for value in values.clone().into_iter() {
                    let column_name_length = value.name.as_bytes().to_vec().len();
                    data.put_u32_le(column_name_length as u32);

                    let value_name = value.name.as_bytes().to_vec();
                    data.write_all(&value_name).unwrap();

                    if
                        value.data.datatype.to_string() == "VARCHAR" ||
                        value.data.datatype.to_string() == "CHAR"
                    {
                        data.put_u32_le(value.data.data_length as u32);
                    }

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
        entry.get_u32_le();

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

pub fn to_flat_schema(tuple: &Tuple) -> Schema {
    match tuple {
        Tuple::TupleData(schema_data) => {
            let mut schema = SchemaBuilder::new();
            for column in schema_data {
                let name = column.name.clone();

                match column.data.datatype {
                    DataType::BigInt => {
                        schema.add_big_int(name);
                    }

                    DataType::Boolean => {
                        schema.add_bool(name);
                    }

                    DataType::Char(size) => {
                        schema.add_char(name, size);
                    }

                    DataType::Decimal => {
                        schema.add_decimal(name);
                    }
                    DataType::SmallInt => {
                        schema.add_small_int(name);
                    }
                    DataType::Varchar(size) => {
                        schema.add_varchar(name, size);
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
    use bincode::config;

    use crate::{
        catalog::schema::{ Schema, SchemaDataBuilder },
        index::tree::{
            byte_box::{ ByteBox, DataType },
            index_types::KeyValuePair,
            tree_node::{ node_type::NodeType, tree_node_inner::NodeInner },
            tree_page::codec::Codec,
        },
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

        let config = config::standard();
        let encoded: Vec<u8> = bincode::encode_to_vec(&to_flat_schema(&tuple), config).unwrap();

        let tuple_box = ByteBox::tuple(&tuple);

        let (new_schema, len): (Schema, usize) = bincode
            ::decode_from_slice(&encoded[..], config)
            .unwrap();

        let data_type = DataType::Tuple(new_schema.clone());

        let new_tuple_box = data_type.to_byte_box(&tuple_box.data.to_vec());

        assert_eq!(tuple_box.datatype, new_tuple_box.datatype);
        assert_eq!(tuple_box.data.to_vec(), new_tuple_box.data.to_vec());

        let tuple_copy = Tuple::decode(&new_schema, tuple_box.data.to_vec());
        assert_eq!(tuple, tuple_copy);
    }

    #[test]
    fn tuple_leaf() {
        let tuple = Tuple::TupleData(
            SchemaDataBuilder::new()
                .add_big_int(String::from("Money"), ByteBox::big_int(1_000_000))
                .add_small_int(String::from("Age"), ByteBox::small_int(25))
                .build()
        );

        let schema = to_flat_schema(&tuple);

        let tuple_box = ByteBox::tuple(&tuple);

        let kv = KeyValuePair {
            key: ByteBox::small_int(10),
            value: tuple_box,
        };

        let mut leaf_node = NodeInner::new(
            NodeType::Leaf(vec![], u32::default(), None),
            false,
            u32::default(),
            None
        );

        leaf_node.insert_entry(kv).unwrap();

        let codec = Codec {
            key_type: DataType::SmallInt,
            value_type: DataType::Tuple(schema),
        };

        let encode = Codec::encode(&leaf_node).unwrap();
        let leaf = codec.decode(&encode).unwrap();

        println!("Leaf {:?}", leaf);
    }
}
