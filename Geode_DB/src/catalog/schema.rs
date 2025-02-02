// use container::DataBox;

// use  crate::db_types::container::data_type_string;

use std::cell::RefCell;

use crate::db_types::container::{data_type_string, ByteBox};

#[derive(Clone, Copy)]

// Convert fields to owned values
// TODO
// Getters and Setter
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
    fn new(name: &'a str, col_type: &'a str, offset_position: u8) -> Self {
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
    pub fn get_column_name(&self) -> &str {
        &self.column_name
    }
    pub fn get_column_type(&self) -> &str {
        &self.column_type
    }

    pub fn get_is_fixed_length(&self) -> &bool {
        &self.is_fixed_length
    }

    pub fn get_is_numeric_type(&self) -> &bool {
        &self.is_numeric_type
    }

    pub fn get_offset_position_in_tuple(&self) -> &u8 {
        &self.offset_position_in_tuple
    }

    pub fn set_offset_potion_in_tuple(&mut self, new_position: u8) {
        self.offset_position_in_tuple = new_position;
    }

    pub fn get_size(&self) -> &u8 {
        &self.size
    }

    pub fn set_size(&mut self, new_size: u8) {
        self.size = new_size;
    }
}

pub struct SchemaBuilder<'a> {
    columns: Vec<Column<'a>>,
}
impl<'a> SchemaBuilder<'a> {
    pub fn new() -> Self {
        Self {
            columns: Vec::new(),
        }
    }

    pub fn add_big_int(mut self, column_name: &'a str) -> Self {
        let schema_length = self.columns.len();

        self.columns.push(Column::new(
            column_name,
            data_type_string::BIGINT,
            schema_length as u8,
        ));
        self
    }

    pub fn add_int(mut self, column_name: &'a str) -> Self {
        let schema_length = self.columns.len();

        self.columns.push(Column::new(
            column_name,
            data_type_string::INT,
            schema_length as u8,
        ));
        self
    }

    pub fn add_small_int(mut self, column_name: &'a str) -> Self {
        let schema_length = self.columns.len();

        self.columns.push(Column::new(
            column_name,
            data_type_string::SMALLINT,
            schema_length as u8,
        ));
        self
    }

    pub fn add_decimal(mut self, column_name: &'a str) -> Self {
        let schema_length = self.columns.len();

        self.columns.push(Column::new(
            column_name,
            data_type_string::DECIMAL,
            schema_length as u8,
        ));
        self
    }

    pub fn add_boolean(mut self, column_name: &'a str) -> Self {
        let schema_length = self.columns.len();

        self.columns.push(Column::new(
            column_name,
            data_type_string::BOOLEAN,
            schema_length as u8,
        ));
        self
    }

    pub fn add_char(mut self, column_name: &'a str, size: u8) -> Self {
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

    pub fn add_varchar(mut self, column_name: &'a str, size: u8) -> Self {
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

    pub fn set_null(self) -> Self {
        let res = self.set_null_result().unwrap();
        res
    }

    fn set_null_result(mut self) -> Result<Self, String> {
        let columns = &mut self.columns;
        let len = columns.len();
        if len < 1 {
            return Err(String::from("There exist no previous column"));
        }

        columns[len - 1].is_nullable = true;

        Ok(self)
    }

    pub fn build(mut self) -> Schema<'a> {
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
    pub fn get_col_idx(&self, col_name: &str) -> Option<u8> {
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

    pub fn icrement_version(&mut self) {
        self.version += 1;
    }
    pub fn validate_fields(&self, values: &Vec<ByteBox>) -> bool {
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

    pub fn get_variable_length_offset(&self, values: &Vec<ByteBox>) -> Vec<u8> {
        let mut varlen_offsets: Vec<u8> = Vec::new();
        let mut current_offset = 0;

        for (i, column) in self.columns.iter().enumerate() {
            if column.column_type == data_type_string::VARCHAR {
                varlen_offsets.push(current_offset as u8);

                current_offset += values[i].data_length + 1;
            // account for length field
            } else {
                current_offset += values[i].data_length; // only the data length, no length field prefix
            }
        }

        varlen_offsets
    }

    pub fn get_columns(&self) -> &Vec<Column> {
        &self.columns
    }
}

/*

    pub struct Column<'a> {
    pub column_name: &'a str,
    pub column_type: &'a str,
    pub is_fixed_length: bool,
    pub is_numeric_type: bool,
    pub is_nullable: bool,
    pub size: u8,
    pub offset_position_in_tuple: u8,
    }

    pub struct Schema<'a> {
    pub length: usize,

    pub columns: Vec<Column<'a>>,
    pub version: u8,
    number_of_var_length_fields: u8,
    }
    I have mutiple type implementations

    VARCHAR Vec<u8> fixed size one element per character:
    CHAR Vec<u8> fixed size one element per character:
    BOOLEAN u8:
    DECIMAL f32:
    BIGINT i64:
    INT i32:
    SMALLINT i16:

    Variable length fields like the varchar a push to the back of vector.
    Therefore, jumping to the length of the list minus the number_of_var_length_fields
    would give us a slice of all the var length fields

    I am packing these data values into a tuple in the order it appears in the schema
    i have to serialize them. Naturally the size of these fields correspond to the size of the types in bits/bytes
    is it possible to implement this function

    take this schma for example

    schema == Schema::Build
    schema.Int( "Salary") name
    schema.addChar("Name", 15)  name + size
    schema.addVarchar("Address", 50) name + size <-- varchar fields
    schema.addVarchar("Mom's_Address", 50) name + size <--varchar fields

    the offsets of the varchar fields would be
    first varchar offset = int size + char_size
    second varchar offset = int size + char_size + first varchar size


    hypothetically if there was a third it would be

    third varchar offset = int size + char_size + first varchar size + second varchar size

    then we return an array

    [  first varchar offset,  second varchar offset , third varchar offset ]

    Remember the column array in the schema contains the corresponding sizes



    schema.get_variable_lengthoffset();



*/

// TODO;
// Implement nullable fields
// Copy constructure for schema
// schema might need to be expanded to include indexes and constraints ( we'll cross that bridge when we get there)
// Implement reordering and padding for word alignment Varchars still remaain at the back

#[cfg(test)]
mod tests {

    use crate::{
        catalog::schema::Column,
        db_types::container::{data_type_string, ByteBox},
    };

    use super::SchemaBuilder;

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
                print!("{} {} {} \n", col_name, col_type, col.get_size())
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
            .set_null()
            .build();

        let idx = schema.get_col_idx("moms_address").unwrap();

        let msg = "idx out of bounds";
        assert_eq!(
            schema.columns.get(idx as usize).expect(msg).column_name,
            "moms_address"
        );

        assert_eq!(
            *schema.columns.get(idx as usize).expect(msg).get_size(),
            50 as u8
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
            .set_null()
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
        let column = Column::new("id", data_type_string::INT, 0);

        assert_eq!(column.column_name, "id");
        assert_eq!(column.column_type, data_type_string::INT);
        assert_eq!(column.is_fixed_length, true);
        assert_eq!(column.is_numeric_type, true);
        assert_eq!(column.offset_position_in_tuple, 0);
    }

    #[test]
    fn test_bytebox_int() {
        let bytebox = ByteBox::int(123);

        assert_eq!(bytebox.datatype, data_type_string::INT);
        assert_eq!(bytebox.data_length, 4);
        assert_eq!(bytebox.data.len(), 4);
        assert_eq!(&bytebox.data, &[123, 0, 0, 0]);
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
