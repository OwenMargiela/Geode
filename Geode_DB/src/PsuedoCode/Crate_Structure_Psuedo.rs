/*
mod my_module {
    // This struct is public, so it can be accessed from other modules or crates.
    pub struct MyStruct {
        // These fields are visible to the entire crate but not outside it (protected-like).
        pub(crate) field1: i32,

        // This field is private to the module.
        field2: i32,
    }

    // This method is public within the crate.
    pub(crate) fn my_function() {
        println!("This is a crate-level function.");
    }

    impl MyStruct {
        // Public method to access the fields of the struct
        pub fn new(field1: i32, field2: i32) -> MyStruct {
            MyStruct { field1, field2 }
        }

        // Public method: Can be called outside the module.
        pub fn display_field1(&self) {
            println!("Field1: {}", self.field1);
        }

        // Private method: Only accessible within this module.
        fn private_method(&self) {
            println!("Private method");
        }
    }
}
*/
