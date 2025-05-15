// #![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
// #![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

// use crate::catalog::schema::Column;

// pub struct TreeSchema {
//     key: Column,
//     value: Column,
// }
// pub struct TreeSchemaBuilder {
//     key: Column,
//     value: Column,
// }

// impl TreeSchemaBuilder {
//     pub fn builder() -> TreeSchemaBuilder {
//         Self {
//             key: Column::new(String::new(), String::new()),
//             value: Column::new(String::new(), String::new()),
//         }
//     }

//     pub fn add_key(mut self, key: Column) -> TreeSchemaBuilder {
//         self.key = key;
//         self
//     }
//     pub fn add_value(mut self, value: Column) -> TreeSchemaBuilder {
//         self.value = value;
//         self
//     }

//     pub fn build(&self) -> TreeSchema {
//         TreeSchema {
//             key: self.key.clone(),
//             value: self.value.clone(),
//         }
//     }
// }
