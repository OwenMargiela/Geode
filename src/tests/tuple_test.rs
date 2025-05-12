#[cfg(test)]

mod tests {

    use crate::{
        catalog::schema::SchemaBuilder,
        db_types::container::{ByteBox, SchemaDataValue},
        storage::tuple::{
            extract_byte_box_data,
            header_const::{BASE_VERSION, HEADER_LENGTH, TUPLE_LENGTH, VARLENGTH_FIELDS},
            schema_reorder, Tuple,
        },
    };

    #[test]
    fn schema_reorder_test() {
        let schema = SchemaBuilder::new()
            .add_int("id")
            .add_varchar("description1", 255)
            .add_small_int("rating")
            .add_varchar("description2withakazoo", 255)
            .add_decimal("price")
            .add_varchar("description32", 255)
            .add_boolean("available")
            .build();

        let real_values_array = vec![
            SchemaDataValue {
                column_name: "id",
                data: ByteBox::int(1),
            },
            SchemaDataValue {
                column_name: "rating",
                data: ByteBox::small_int(5),
            },
            SchemaDataValue {
                column_name: "price",
                data: ByteBox::decimal(19.99),
            },
            SchemaDataValue {
                column_name: "available",
                data: ByteBox::boolean(true),
            },
            SchemaDataValue {
                column_name: "description1",
                data: ByteBox::varchar("Description 1", 255),
            },
            SchemaDataValue {
                column_name: "description2withakazoo",
                data: ByteBox::varchar("Description2withakazoo", 255),
            },
            SchemaDataValue {
                column_name: "description32",
                data: ByteBox::varchar("Description 32", 255),
            },
        ];

        let mut values_array = vec![
            SchemaDataValue {
                column_name: "price",
                data: ByteBox::decimal(19.99),
            },
            SchemaDataValue {
                column_name: "rating",
                data: ByteBox::small_int(5),
            },
            SchemaDataValue {
                column_name: "id",
                data: ByteBox::int(1),
            },
            SchemaDataValue {
                column_name: "available",
                data: ByteBox::boolean(true),
            },
            SchemaDataValue {
                column_name: "description1",
                data: ByteBox::varchar("Description 1", 255),
            },
            SchemaDataValue {
                column_name: "description32",
                data: ByteBox::varchar("Description 32", 255),
            },
            SchemaDataValue {
                column_name: "description2withakazoo",
                data: ByteBox::varchar("Description2withakazoo", 255),
            },
        ];

        schema_reorder(&mut values_array, &schema);
        for (i, value) in values_array.iter().enumerate() {
            assert_eq!(value.column_name, real_values_array[i].column_name);
        }
    }

    #[test]
    fn test_tuple_variable_length_fields() {
        let mut schema = SchemaBuilder::new()
            .add_big_int("id")
            .add_varchar("description1", 255)
            .add_small_int("rating")
            .add_varchar("description2withakazoo", 255)
            .add_decimal("price")
            .add_varchar("description32", 255)
            .add_boolean("available")
            .build();

        // Schema Order
        let mut values_array = vec![
            SchemaDataValue {
                column_name: "price",
                data: ByteBox::decimal(19.99),
            },
            SchemaDataValue {
                column_name: "rating",
                data: ByteBox::small_int(5),
            },
            SchemaDataValue {
                column_name: "id",
                data: ByteBox::big_int(5),
            },
            SchemaDataValue {
                column_name: "available",
                data: ByteBox::boolean(true),
            },
            SchemaDataValue {
                column_name: "description1",
                data: ByteBox::varchar("Description 1", 255),
            },
            SchemaDataValue {
                column_name: "description32",
                data: ByteBox::varchar("Description 32", 255),
            },
            SchemaDataValue {
                column_name: "description2withakazoo",
                data: ByteBox::varchar("Description2withakazoo", 255),
            },
        ];

        print!("Getting varlen offsets");
        schema_reorder(&mut values_array, &schema);
        let mut values = extract_byte_box_data(values_array);

        let tuple = Tuple::build(&mut values, &mut schema).expect("Faild to build tuple");
        let header_length = tuple.data[HEADER_LENGTH] as usize;

        println!("Header Len {:?}", &tuple.data[0..2]);

        println!(
            "Header Length {:?}",
            &tuple.data[HEADER_LENGTH..HEADER_LENGTH + TUPLE_LENGTH]
        );

        println!(
            "Version {:?}",
            &tuple.data[HEADER_LENGTH + TUPLE_LENGTH..HEADER_LENGTH + TUPLE_LENGTH + BASE_VERSION]
        );

        println!(
            "Data Pointers {:?}",
            &tuple.data
                [HEADER_LENGTH + TUPLE_LENGTH + BASE_VERSION + VARLENGTH_FIELDS..header_length]
        );
        println!("Header {:?}", &tuple.data[0..header_length]);

        let data_ptrs: &[u8] = &tuple.data
            [HEADER_LENGTH + TUPLE_LENGTH + BASE_VERSION + VARLENGTH_FIELDS..header_length];

        let assert_vec = vec![
            vec![5, 0, 0, 0, 0, 0, 0, 0],
            vec![5, 0],
            vec![133, 235, 159, 65],
            vec![1],
        ];

        let mut field: &[u8];
        for i in 0..assert_vec.len() {
            let offset: usize = data_ptrs[i] as usize + header_length;

            field = &tuple.data[offset..offset + assert_vec[i].len()];
            println!("column {} {:?}", i, field);

            assert_eq!(field, assert_vec[i]);
        }
        let mut running_count = 0;

        let assert_strings = vec!["Description 1", "Description2withakazoo", "Description 32"];

        for i in 4..data_ptrs.len() {
            let mut offset: usize = header_length + data_ptrs[i] as usize;
            let len: usize =
                tuple.data[running_count + header_length + data_ptrs[i] as usize] as usize;
            println!("Len {}", len);
            offset += 1;

            field = &tuple.data[running_count + offset..offset + len + running_count];

            let string = String::from_utf8_lossy(field).into_owned();

            assert_eq!(string, assert_strings[running_count]);

            running_count += 1;
        }
    }

    #[test]
    fn test_get() {
        let mut schema = SchemaBuilder::new()
            .add_big_int("id")
            .add_varchar("description1", 255)
            .add_small_int("rating")
            .add_varchar("description2withakazoo", 255)
            .add_decimal("price")
            .add_varchar("description32", 255)
            .add_boolean("available")
            .build();

        // Schema Order
        let mut values_array = vec![
            SchemaDataValue {
                column_name: "price",
                data: ByteBox::decimal(19.99),
            },
            SchemaDataValue {
                column_name: "rating",
                data: ByteBox::small_int(5),
            },
            SchemaDataValue {
                column_name: "id",
                data: ByteBox::big_int(5),
            },
            SchemaDataValue {
                column_name: "available",
                data: ByteBox::boolean(true),
            },
            SchemaDataValue {
                column_name: "description1",
                data: ByteBox::varchar("Description 1", 255),
            },
            SchemaDataValue {
                column_name: "description32",
                data: ByteBox::varchar("Description 32", 255),
            },
            SchemaDataValue {
                column_name: "description2withakazoo",
                data: ByteBox::varchar("Description2withakazoo", 255),
            },
        ];

        schema_reorder(&mut values_array, &schema);
        let mut values = extract_byte_box_data(values_array);

        let tuple = Tuple::build(&mut values, &mut schema).expect("Faild to build tuple");

        // Optimization
        // prefix the column with the length of the data type instead of having it in the header
        let data = tuple.get(0, 8).unwrap();

        assert_eq!(data, vec![5, 0, 0, 0, 0, 0, 0, 0]);

        let data = tuple.get(1, 2).unwrap();

        assert_eq!(data, vec![5, 0,])
    }
}
