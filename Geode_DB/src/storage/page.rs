use std::{
    env::consts,
    ops::Index,
    ptr::copy,
    sync::atomic::{AtomicBool, AtomicI32},
};

/*
    To prevent me from ripping my hair off my head, i'll  recreate this page implementation from scratch.
    I am now aware of the things i need to do on a technical bases to implement the mechanism

    Problems with the current slotted page

    Constants are hard to read
    Didn't test in-place edits of data within the byte stream
    Data isn't compartmentalized enough
    incorrent logic for insertions

    Formalize everything, try again.

*/

use page_constants::{
    BYTE_LENGTH_2, FREESPACE_POINTER, FREESPACE_SIZE, METADATA_SIZE, METADATA_STARTING_OFFSET,
    NUMBER_OF_SLOTS, PAGE_ID, PAGE_SIZE, STARTING_SLOT_OFFSET,
};

use super::tuple::Tuple;

pub struct Page {
    data: [u8; PAGE_SIZE],
    _pin_count: AtomicI32,
    _is_dirty: AtomicBool,
}
pub mod page_constants {
    // Offsets
    pub const METADATA_STARTING_OFFSET: usize = PAGE_SIZE - METADATA_SIZE;
    // Offsets within the metadata sub array
    pub const FREESPACE_POINTER: usize = 0;
    pub const PAGE_ID: usize = 2;
    pub const FREESPACE_SIZE: usize = 4;
    pub const NUMBER_OF_SLOTS: usize = 6;

    pub const STARTING_SLOT_OFFSET: usize = METADATA_STARTING_OFFSET - 1;

    // Size
    pub const PAGE_SIZE: usize = 1024 * 4;
    // Stores an element with a byte length of two
    pub const BYTE_LENGTH_2: usize = 2;
    // The metadata sub array contains 4 elements of size BYTE_LENGTH_2
    pub const METADATA_SIZE: usize = 8;
}

pub enum METADATA {
    _FreespacePointer,
    _PageId,
    _FreespaceSize,
    _NumberOfSlots,
}

// make internal interfaces
pub trait SlottedPage {
    // Param Prev Page ID
    fn new(prev_page: Option<u16>) -> Page;
    fn append(&mut self, tuple: Tuple) -> Option<usize>;
    // fn next_pg_id(&self) -> u8;

    // // Will be implement after page b-tree has been put in place
    // // fn insert(tuple: Tuple, offset: usize) -> Option<usize>;

    // // fn deletion_marker(slot_entry: usize) -> usize;
    // // fn deletion_apply(slot_entry: usize) -> usize;
    // // // Un-deletions your marker :)
    // // fn rollback_delete(slot_entry: usize) -> usize;

    // // writes data, any data you want baby
    fn write_data(&mut self, src: Vec<u8>, len: usize, starting_position: usize) -> usize;

    // fn get_tuple(slot_entry: usize) -> usize;

    fn get_metadata(&self, metadata: METADATA) -> u16;
    fn update_freespace_values(&mut self, data_length: u16) -> Result<(), i8>;

    fn get_slot_at_index(&self, index: u16) -> Result<&[u8], i8>;
    fn slot_append(&mut self, tuple_offset_len: [u8; 2]);
    fn slot_remove_marker(&mut self, index: u16) -> Result<(), i8>;

    fn print_slot(&self);
    fn print_metadata(&self);
}

impl SlottedPage for Page {
    fn new(prev_page: Option<u16>) -> Page {
        let mut page_data: [u8; PAGE_SIZE] = [0; PAGE_SIZE];

        // freespace pointer the offset of the start of non segmented freespace
        let free_space_ptr: [u8; BYTE_LENGTH_2];
        free_space_ptr = u16::MIN.to_le_bytes();

        // _PageID Might need to be u64 in the production case
        let page_id: [u8; BYTE_LENGTH_2];

        page_id = prev_page.map_or((1 as u16).to_le_bytes(), |mut prev_value| {
            prev_value += 1;
            prev_value.to_le_bytes()
        });

        // Freespace Size
        let free_pace_size: [u8; BYTE_LENGTH_2];
        free_pace_size = ((PAGE_SIZE - METADATA_SIZE) as u16).to_le_bytes();

        // Number of slots
        let number_of_slots: [u8; BYTE_LENGTH_2];
        number_of_slots = u16::MIN.to_le_bytes();

        // Setting Meta Data
        let mut meta_data: Vec<u8> = Vec::with_capacity(METADATA_SIZE);
        meta_data.extend(free_space_ptr);
        meta_data.extend(page_id);
        meta_data.extend(free_pace_size);
        meta_data.extend(number_of_slots);

        // Memory Moving meta data to a specific offset within the data array
        unsafe {
            copy(
                meta_data.as_ptr(),
                page_data.as_mut_ptr().add(METADATA_STARTING_OFFSET),
                METADATA_SIZE,
            );
        }

        Page {
            data: page_data,
            _pin_count: 0.into(),
            _is_dirty: false.into(),
        }
    }

    fn append(&mut self, tuple: Tuple) -> Option<usize> {
        let tuple_len = tuple.data.len() as u16;
        let freespace_pointer = self.get_metadata(METADATA::_FreespacePointer);

        if self.update_freespace_values(tuple_len).is_ok() {
            let tuple_len_bytes = tuple_len.to_le_bytes();
            self.write_data(tuple.data, tuple_len as usize, freespace_pointer as usize);

            self.slot_append(tuple_len_bytes);

            return Some((tuple_len + freespace_pointer) as usize);
        } else {
            // Not enough space to append the tuple
            return None;
        }
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

    fn print_metadata(&self) {
        let metadata_subarray = &self.data[METADATA_STARTING_OFFSET..];
        println!("{:?}", metadata_subarray);
    }

    fn get_metadata(&self, metadata: METADATA) -> u16 {
        let metadata_subarray = &self.data[METADATA_STARTING_OFFSET..];

        fn extract_u16(slice: &[u8], start: usize) -> u16 {
            let end = start + BYTE_LENGTH_2;
            u16::from_le_bytes(slice[start..end].try_into().expect("Slice length mismatch"))
        }

        match metadata {
            METADATA::_FreespacePointer => extract_u16(metadata_subarray, FREESPACE_POINTER),
            METADATA::_PageId => extract_u16(metadata_subarray, PAGE_ID),
            METADATA::_FreespaceSize => extract_u16(metadata_subarray, FREESPACE_SIZE),
            METADATA::_NumberOfSlots => extract_u16(metadata_subarray, NUMBER_OF_SLOTS),
        }
    }

    fn update_freespace_values(&mut self, data_length: u16) -> Result<(), i8> {
        fn write_u16(metadata_subarray: *mut u8, start: usize, data: [u8; 2]) {
            unsafe {
                copy(data.as_ptr(), metadata_subarray.add(start), BYTE_LENGTH_2);
            }
        }

        let number_of_slots = self.get_metadata(METADATA::_NumberOfSlots);
        let freespace_offset = self.get_metadata(METADATA::_FreespacePointer);
        let mut freespace_size = self.get_metadata(METADATA::_FreespaceSize);

        // BYTE_LENGTH_2 to account for the slot to be inserted
        let frspc_offset = freespace_offset + (data_length + BYTE_LENGTH_2 as u16);
        if freespace_size < data_length {
            return Err(-1);
        }
        freespace_size -= data_length;

        let metadata_subarray = &mut self.data[METADATA_STARTING_OFFSET..];

        if frspc_offset as usize
            >= PAGE_SIZE - METADATA_SIZE - (number_of_slots as usize * BYTE_LENGTH_2)
        {
            println!("insufficient size");
            return Err(-1);
        }

        let frspc_ofst_bytes = (frspc_offset - BYTE_LENGTH_2 as u16).to_le_bytes();
        let frspc_sz_bytes = (freespace_size).to_le_bytes();
        let num_of_slots = (number_of_slots + 1 as u16).to_le_bytes();

        // Write Freespace_offset
        write_u16(
            metadata_subarray.as_mut_ptr(),
            FREESPACE_POINTER,
            frspc_ofst_bytes,
        );

        // Write Freespace_size
        write_u16(
            metadata_subarray.as_mut_ptr(),
            FREESPACE_SIZE,
            frspc_sz_bytes,
        );

        // Write Number of slots
        write_u16(
            metadata_subarray.as_mut_ptr(),
            NUMBER_OF_SLOTS,
            num_of_slots,
        );

        Ok(())

    }

    fn print_slot(&self) {
        let number_of_slots = self.get_metadata(METADATA::_NumberOfSlots);

        let slot_start = STARTING_SLOT_OFFSET - (number_of_slots as usize * BYTE_LENGTH_2);
        let slot_range = slot_start + number_of_slots as usize * BYTE_LENGTH_2;

        let slots = &self.data[slot_start..slot_range];

        {
            let mut slots = slots.to_vec();
            slots.reverse();
            println!("{:?}", slots);
            println!("Length of {:?}", slots.len() / 2);
        }
    }

    fn get_slot_at_index(&self, index: u16) -> Result<&[u8], i8> {
        let index = index as usize;

        let number_of_slots = self.get_metadata(METADATA::_NumberOfSlots);
        if index > number_of_slots as usize {
            return Err(-1);
        }

        let slot_start = STARTING_SLOT_OFFSET - (number_of_slots as usize * BYTE_LENGTH_2);
        let slot_range = slot_start + number_of_slots as usize * BYTE_LENGTH_2;

        let slots = &self.data[slot_start..slot_range];

        let start = slots.len() - (index + 1) * BYTE_LENGTH_2;
        let range = start + BYTE_LENGTH_2;

        let data: &[u8] = &slots[start..range];

        Ok(data)
    }
    fn slot_append(&mut self, tuple_offset_len: [u8; 2]) {
        // Slot insertion index
        let number_of_slots = self.get_metadata(METADATA::_NumberOfSlots);
        let slot = STARTING_SLOT_OFFSET - (1 * BYTE_LENGTH_2) * number_of_slots as usize;
        self.data[slot..slot + BYTE_LENGTH_2].copy_from_slice(&tuple_offset_len[0..2]);
    }

    fn slot_remove_marker(&mut self, index: u16) -> Result<(), i8> {
        let index = index as usize;

        let number_of_slots = self.get_metadata(METADATA::_NumberOfSlots);
        if index > number_of_slots as usize {
            return Err(-1);
        }

        let slot_start = STARTING_SLOT_OFFSET - (number_of_slots as usize * BYTE_LENGTH_2);
        let slot_range = slot_start + number_of_slots as usize * BYTE_LENGTH_2;

        let slots = &mut self.data[slot_start..slot_range];

        let start = slots.len() - (index + 1) * BYTE_LENGTH_2;
        let range = start + BYTE_LENGTH_2;

        slots[start..range].copy_from_slice(&[0, 0]);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        catalog::schema::SchemaBuilder,
        db_types::container::{ByteBox, SchemaDataValue},
        storage::{
            page::{
                page_constants::{METADATA_SIZE, PAGE_SIZE},
                METADATA,
            },
            tuple::{extract_byte_box_data, schema_reorder, Tuple},
        },
    };

    use super::{Page, SlottedPage};

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
        page.update_freespace_values(new_tuple_length)
            .expect_err("Cannot insert tuple of this length");

        println!("{:?}", page.data);
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

        page.slot_remove_marker(3).expect("");

        // Fragmented space is created, this will not be reflected in the meta data
        page.slot_remove_marker(9).expect_err("");
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

        let mut i = 0;
        loop {
            let tuple = Tuple::build(&mut values, &mut schema).expect("Faild to build tuple");
            let res = page.append(tuple);
            println!("iteration {} ", i);

            i += 1;

            if res.is_none() {
                println!("Page is full");
                break;
            }
        }

        page.print_slot();
        println!("{:?}", page.data);
    }
}
