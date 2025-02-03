use std::{
    ptr::copy,
    sync::atomic::{AtomicBool, AtomicI32},
};

use page_constants::{
    FREESPACE_POINTER, FREESPACE_SIZE, FREESPACE_SIZE_SIZE, PAGE_ID, PAGE_SIZE, SLOT_POINTER,
    SLOT_POINTER_SIZE, SLOT_SIZE, STARTING_SLOT_OFFSET,
};

use super::tuple::Tuple;
pub mod page_constants {
    // Offset
    pub const PAGE_SIZE: usize = 4096;
    pub const FREESPACE_POINTER: usize = PAGE_SIZE - 6;
    pub const PAGE_ID: usize = PAGE_SIZE - 5;
    pub const FREESPACE_SIZE: usize = PAGE_SIZE - 4;

    // The value contained at this index is the amount of bytes away
    // from the free slot at that particular index
    pub const SLOT_POINTER: usize = PAGE_SIZE - 2;

    // A pointer that remains fixed to the start of the slot array
    pub const STARTING_SLOT_OFFSET: usize = PAGE_SIZE - 7;

    // Size
    pub const FREESPACE_SIZE_SIZE: usize = 2;
    // Pointer to the next free slot
    pub const SLOT_POINTER_SIZE: usize = 2;

    // Actual slot
    pub const SLOT_SIZE: usize = 2;
}

// make internal interfaces
pub trait SlottedPage {
    fn new(prev_page: Option<u8>) -> Page;
    fn append(&mut self, tuple: Tuple) -> Option<usize>;
    fn next_pg_id(&self) -> u8;

    // Getters
    fn get_ptr_to_freespace(&self) -> usize;
    fn get_page_id(&self) -> u8;
    fn get_freespace_size(&self) -> u16;
    fn get_slot_ptr(&self) -> u16;
    fn get_slot_position(&self) -> usize;

    // Setters
    fn _set_ptr_to_freespace(&mut self, value: usize);
    fn _set_freespace_size(&mut self, value: u16);
    fn _set_page_id(&mut self, value: u8);
    fn set_slot_ptr(&mut self, value: u16);

    // No function below here should be user callable

    // We assume that this will not be used to compute the insertion
    // of tuples into fragmented space as the size of the fragmented space
    // with me known before insertion

    // Will be used for append only operations
    // If the data surpasses the FREESPACE_SIZE threshold,
    // it cannot be inserted
    fn insertion_offset(&self, tuple_len: usize) -> Option<usize>;
    fn push_slot(&mut self, tuple_offset: usize) -> usize;
    fn del_slot(&mut self, index: usize) -> usize;

    // Will be implement after page b-tree has been put in place
    // fn insert(tuple: Tuple, offset: usize) -> Option<usize>;

    // fn deletion_marker(slot_entry: usize) -> usize;
    // fn deletion_apply(slot_entry: usize) -> usize;
    // // Un-deletions your marker :)
    // fn rollback_delete(slot_entry: usize) -> usize;
    // writes data, any data you want baby
    fn write_data(&mut self, src: Vec<u8>, len: usize, starting_position: usize) -> usize;

    // fn update_tuple(slot_entry: usize) -> usize;
    // fn get_tuple(slot_entry: usize) -> usize;
}

impl SlottedPage for Page {
    // Make ID larger possibly u32 or 64
    fn new(prev_page: Option<u8>) -> Page {
        /*
           freespace pointer   1
           PageID              2
           Size of freespace   3
           Prev PageID         4
           Slot Pointer        5
           Footer of length:   6
        */

        let mut page_data: Vec<u8> = vec![0; PAGE_SIZE];

        // Setting Meta Data
        let mut meta_data: Vec<u8> = Vec::new();

        // freespace pointer the offset of the start of non segmented freespace
        let free_space_ptr = (0 as u8).to_le_bytes();
        meta_data.extend(&free_space_ptr);

        // PageID Might need to be u64 in the production case
        let page_id: [u8; 1];

        if prev_page.is_none() {
            page_id = (1 as u8).to_le_bytes();
        } else {
            let mut prev_value = prev_page.unwrap();
            prev_value += 1;
            page_id = prev_value.to_le_bytes();
        }

        meta_data.extend(&page_id);

        // Freespace Size
        let free_pace_size = (PAGE_SIZE as u16).to_le_bytes();
        meta_data.extend(&free_pace_size);

        // Slot Pointer ( Points to where the next slot will be placed)
        let slot_pointer = (meta_data.len() as u16 + 1).to_le_bytes();
        meta_data.extend(&slot_pointer);

        let meta_data_len = meta_data.len();
        let meta_data_offset = PAGE_SIZE - meta_data_len;

        // Memory Moving meta data to a specific offset within the data array
        unsafe {
            copy(
                meta_data.as_ptr(),
                page_data.as_mut_ptr().add(meta_data_offset),
                meta_data_len,
            );
        }

        Page {
            data: page_data,
            _pin_count: 0.into(),
            _is_dirty: false.into(),
        }
    }

    fn append(&mut self, tuple: Tuple) -> Option<usize> {
        // The get slot position is just the postion in the slot arra
        // NOT THE FREESPACE POSITION
        let ptr_to_freespace = self.get_ptr_to_freespace();
        let tuple_data_length = tuple.data.len();

        // println!("Pointer to free space {:?}", ptr_to_freespace);
        self.push_slot(ptr_to_freespace);
        let tuple_len = tuple.data.len();

        if self.insertion_offset(tuple_len).is_some() {
            self.write_data(tuple.data, tuple_len, ptr_to_freespace);
            self._set_ptr_to_freespace(tuple_data_length + ptr_to_freespace);
            return Some(tuple_len);
        } else {
            // Not enough space to append the tuple
            None
        }
    }

    fn insertion_offset(&self, tuple_len: usize) -> Option<usize> {
        let mut insertion_offset = self.get_ptr_to_freespace();
        insertion_offset += tuple_len;

        if insertion_offset > self.get_freespace_size() as usize {
            None
        } else {
            Some(0)
        }
    }

    // Is the write function overkill for the slots?
    fn push_slot(&mut self, tuple_offset: usize) -> usize {
        let slot_position = self.get_slot_ptr();
        let slot_offset = slot_position as usize + SLOT_SIZE + 1;

        println!("Tuple offset {:?}", (tuple_offset as u16).to_le_bytes());

        println!(
            "Position {:?}",
            // The + 1 to account for the offset
            PAGE_SIZE - slot_offset
        );

        // the minus 1 to account for the length of the slot
        // let slot_offset = PAGE_SIZE - ( slot_position as usize + 2 );

        self.data[PAGE_SIZE - slot_offset..PAGE_SIZE - slot_offset + SLOT_SIZE]
            .copy_from_slice(&(tuple_offset as u16).to_le_bytes());

        // println!("Slot position {} - {} ", PAGE_SIZE - slot_offset,  PAGE_SIZE - slot_offset + 1);

        self.set_slot_ptr(self.get_slot_ptr() + 2);

        tuple_offset
    }

    fn del_slot(&mut self, index: usize) -> usize {
        // jumps to the start of the slot arrays then from there,
        // the exact position of the index
        let position = STARTING_SLOT_OFFSET - index;
        let slot_value = self.data[position];

        if slot_value != 0 {
            let slot_value = (0 as u8).to_le_bytes().to_vec();

            self.write_data(slot_value, 1, position);
        }

        index
    }
    // Wrapper around a memove
    fn write_data(&mut self, src: Vec<u8>, len: usize, starting_position: usize) -> usize {
        unsafe {
            let data_dest = self.data.as_mut_ptr().add(starting_position);
            let data_src = src.as_ptr();
            let data_src_len = src.len();

            copy(data_src, data_dest, data_src_len);
        }
        starting_position + len
    }

    // Getters
    fn get_ptr_to_freespace(&self) -> usize {
        let ptr = self.data[FREESPACE_POINTER];
        let usize_ptr = ptr as usize;
        usize_ptr
    }

    fn get_freespace_size(&self) -> u16 {
        let ptr = &self.data[FREESPACE_SIZE..FREESPACE_SIZE + FREESPACE_SIZE_SIZE];
        let u16_ptr = u16::from_le_bytes([ptr[0], ptr[1]]);
        u16_ptr
    }

    fn get_page_id(&self) -> u8 {
        let ptr = self.data[PAGE_ID];
        ptr
    }

    fn get_slot_ptr(&self) -> u16 {
        let ptr = &self.data[SLOT_POINTER..SLOT_POINTER + SLOT_POINTER_SIZE];
        let u16_ptr = u16::from_le_bytes([ptr[0], ptr[1]]);
        u16_ptr
    }

    fn get_slot_position(&self) -> usize {
        let ptr = self.get_slot_ptr() as usize;
        let pos = self.data[PAGE_SIZE - ptr] as usize;
        pos
    }

    fn next_pg_id(&self) -> u8 {
        self.get_page_id() + 1
    }

    // Setters
    fn _set_ptr_to_freespace(&mut self, value: usize) {
        self.data[FREESPACE_POINTER] = value as u8;
    }

    fn _set_freespace_size(&mut self, value: u16) {
        let bytes = value.to_le_bytes();
        self.data[FREESPACE_SIZE..FREESPACE_SIZE + FREESPACE_SIZE_SIZE].copy_from_slice(&bytes);
    }

    fn _set_page_id(&mut self, value: u8) {
        self.data[PAGE_ID] = value;
    }

    fn set_slot_ptr(&mut self, value: u16) {
        let bytes = value.to_le_bytes();
        self.data[SLOT_POINTER..SLOT_POINTER + SLOT_POINTER_SIZE].copy_from_slice(&bytes);
    }
}
pub struct Page {
    data: Vec<u8>,
    _pin_count: AtomicI32,
    _is_dirty: AtomicBool,
}

#[cfg(test)]
mod tests {
    use crate::{
        catalog::schema::SchemaBuilder,
        db_types::container::{ByteBox, SchemaDataValue},
        storage::tuple::{extract_byte_box_data, schema_reorder, Tuple},
    };

    use super::{Page, SlottedPage};

    #[test]
    fn new_page() {
        let prev: Option<u8> = None;
        let mut page: Page = <Page as SlottedPage>::new(prev);

        // print!("{:?}", &page.data);

        assert_eq!(page.get_ptr_to_freespace(), 0);
        assert_eq!(page.get_page_id(), 1);
        assert_eq!(page.next_pg_id(), 2);
        assert_eq!(page.get_freespace_size(), 4096);
        assert_eq!(page.get_slot_ptr(), 5);

        page._set_ptr_to_freespace(2);
        page._set_page_id(2);
        page._set_freespace_size(4090);
        page.set_slot_ptr(page.get_slot_ptr() + 1);

        assert_eq!(page.get_ptr_to_freespace(), 2);
        assert_eq!(page.get_page_id(), 2);
        assert_eq!(page.next_pg_id(), 3);
        assert_eq!(page.get_freespace_size(), 4090);
        assert_eq!(page.get_slot_ptr(), 6);
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

        let tuple = Tuple::build(&mut values, &mut schema).expect("Faild to build tuple");
        let tuple_two = Tuple::build(&mut values, &mut schema).expect("Faild to build tuple");
        let tuple_three: Tuple =
            Tuple::build(&mut values, &mut schema).expect("Faild to build tuple");

        let tuple_four: Tuple =
            Tuple::build(&mut values, &mut schema).expect("Faild to build tuple");

        page.append(tuple);
        page.append(tuple_two);
        page.append(tuple_three);
        page.append(tuple_four);

        print!("{:?}", page.data);
    }
}
