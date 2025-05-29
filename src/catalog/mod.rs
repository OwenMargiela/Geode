use schema::{ Schema, SchemaDataBuilder };

use crate::{ index::tree::byte_box::ByteBox, storage::tuple::{ to_flat_schema, Tuple } };

pub mod schema;

pub fn pages_directory_schema() -> Schema {
    let allocated_pages = SchemaDataBuilder::new()
        .add_int(String::from("PAGE_ID"), ByteBox::int(0))
        .add_big_int(String::from("PAGE_OFFSET"), ByteBox::big_int(0))
        .add_bool(String::from("IS_ALLOCATED"), ByteBox::boolean(false))
        .build();

    let allocated_pages_tuple = Tuple::TupleData(allocated_pages);

    to_flat_schema(&allocated_pages_tuple)
}

pub fn table_schema() -> Schema {
    let table = SchemaDataBuilder::new()
        .add_int(String::from("ROOT_PAGE_ID"), ByteBox::int(0))
        // .add_big_int(String::from("MONO_ID"), ByteBox::big_int(0))
        .add_char(String::from("KEY_DATATYPE"), ByteBox::varchar("", 50))
        .add_char(String::from("VALUE_DATATYPE"), ByteBox::varchar("", 50))
        .build();

    let table_tuple = Tuple::TupleData(table);

    to_flat_schema(&table_tuple)
}
