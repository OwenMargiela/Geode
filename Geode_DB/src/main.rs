mod storage;
mod catalog;
mod db_types;

fn main() {
    // The number we want to serialize
    let number: u64 = 4096;

    // Serialize the number into a byte array
    let bytes = number.to_le_bytes(); // Use to_be_bytes() for big-endian representation

    // Print out the serialized byte array
    println!("{:?}", bytes);
    
}
