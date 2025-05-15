#[cfg(test)]
mod tests {

    use crate::{
        catalog::schema::Column,
        db_types::container::{data_type_string, ByteBox},
    };

    use crate::catalog::schema::SchemaBuilder;

    #[test]
    fn schema_test() {
        let schema = SchemaBuilder::new()
            .add_varchar("Address", 50)
            .add_big_int("Salary")
            .add_char("Name", 15)
            .add_varchar("moms_address", 50)
            .build();

        for i in 0..schema.length {
            let col = &schema.columns.get(i).unwrap();
            let col_name = col.column_name;
            let col_type = col.column_type;

            if col.column_type == data_type_string::CHAR
                || col.column_type == data_type_string::VARCHAR
            {
                print!("{} {}  \n", col_name, col_type)
            } else {
                print!("{} {} \n", col_name, col_type)
            }
        }
    }
    #[test]
    fn schema_idx() {
        let schema = SchemaBuilder::new()
            .add_big_int("Salary")
            .add_varchar("Address", 50)
            .add_char("Name", 15)
            .add_varchar("moms_address", 50)
            .build();

        let idx = schema.get_col_idx("moms_address").unwrap();

        let msg = "idx out of bounds";
        assert_eq!(
            schema.columns.get(idx as usize).expect(msg).column_name,
            "moms_address"
        );
    }

    #[test]
    fn test_create_schema() {
        // Varchar push down reorders the varchars to the back
        let schema = SchemaBuilder::new()
            .add_int("id")
            .add_varchar("description1", 255)
            .add_small_int("rating")
            .add_varchar("description2", 255)
            .add_decimal("price")
            .add_varchar("description3", 255)
            .add_boolean("available")
            .build();

        assert_eq!(schema.length, 7);
        assert_eq!(schema.columns[0].column_name, "id");
        assert_eq!(schema.columns[1].column_name, "rating");
        assert_eq!(schema.columns[2].column_name, "price");
        assert_eq!(schema.columns[3].column_name, "available");
        assert_eq!(schema.columns[4].column_name, "description1");
        assert_eq!(schema.columns[5].column_name, "description2");
        assert_eq!(schema.columns[6].column_name, "description3");
    }

    #[test]
    fn test_column_properties() {
        let column = Column::new("id", data_type_string::INT);

        assert_eq!(column.column_name, "id");
        assert_eq!(column.column_type, data_type_string::INT);
    }

    #[test]
    fn test_schema_validation() {
        let schema = SchemaBuilder::new()
            .add_int("id")
            .add_big_int("big_number")
            .add_varchar("description", 255) // Varchar push down reorders this to the back
            .add_char("initial", 1)
            .build();

        let mut values = vec![
            ByteBox::int(1),
            ByteBox::big_int(1234567890),
            ByteBox::char("A", 1),
            ByteBox::varchar("Test description", 255),
        ];

        assert!(schema.validate_fields(&values));

        values = vec![
            ByteBox::int(1),
            ByteBox::big_int(1234567890),
            ByteBox::varchar("Test description", 255), // Not in schema order
            ByteBox::char("A", 1),
        ];

        assert!(!schema.validate_fields(&values));
    }
}
