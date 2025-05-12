#[cfg(test)]
mod tests {
    use crate::{
        catalog::schema::SchemaBuilder,
        db_types::container::{ByteBox, SchemaDataValue},
        storage::{
            page::page::{
                page_constants::{METADATA_SIZE, PAGE_SIZE},
                Page, SlottedPage, METADATA,
            },
            tuple::{extract_byte_box_data, schema_reorder, Tuple},
        },
    };

    #[test]
    fn new_page() {
        let prev: Option<u16> = None;
        let mut page: Page = <Page as SlottedPage>::new(prev);

        assert_eq!(page.get_metadata(METADATA::_FreespacePointer), 0);
        assert_eq!(page.get_metadata(METADATA::_PageId), 1);

        assert_eq!(
            page.get_metadata(METADATA::_FreespaceSize),
            (PAGE_SIZE - METADATA_SIZE) as u16
        );

        assert_eq!(page.get_metadata(METADATA::_NumberOfSlots), 0);

        let new_tuple_length: u16 = 80;
        page.update_freespace_values(new_tuple_length).unwrap();

        assert_eq!(
            page.get_metadata(METADATA::_FreespacePointer),
            new_tuple_length
        );

        assert_eq!(
            page.get_metadata(METADATA::_FreespaceSize),
            (PAGE_SIZE - METADATA_SIZE) as u16 - new_tuple_length
        );

        page.print_metadata();

        let new_tuple_length: u16 = 4000;

        assert!(page.update_freespace_values(new_tuple_length).is_ok());
    }

    #[test]

    fn slot_test() {
        let prev: Option<u16> = None;
        let mut page: Page = <Page as SlottedPage>::new(prev);

        let new_tuple_length: u16 = 80;
        let mut cur_offset: u16 = 0;

        for _i in 0..4 {
            page.update_freespace_values(new_tuple_length).unwrap();
            page.slot_append(cur_offset.to_le_bytes());

            cur_offset += 80;
        }

        page.print_slot();

        page.print_metadata();

        assert_eq!(page.get_slot_at_index(0).expect(""), [0, 0]);
        assert_eq!(page.get_slot_at_index(1).expect(""), [80, 0]);
        assert_eq!(page.get_slot_at_index(2).expect(""), [160, 0]);
        assert_eq!(page.get_slot_at_index(3).expect(""), [240, 0]);

        // Fragmented space is created, this will not be reflected in the meta data
        page.slot_remove_marker(3).expect("");
        assert_eq!(page.get_slot_at_index(3).expect(""), [0, 0]);
        page.print_slot();
    }

    #[test]
    fn page_operations() {
        let mut page = <Page as SlottedPage>::new(None);
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

        loop {
            let tuple = Tuple::build(&mut values, &mut schema).expect("Faild to build tuple");
            let res = page.append(tuple);
            if res.is_none() {
                break;
            }
        }

        let tuple = page.get_tuple(48).unwrap();
        let mock_tuple = Tuple::build(&mut values, &mut schema).expect("Faild to build tuple");

        assert_eq!(tuple.data, mock_tuple.data);
    }
}
