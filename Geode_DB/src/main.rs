use std::{fs::File, os::unix::fs::MetadataExt};


mod utils;
mod storage;
mod db_types;
mod catalog;
fn main() {
    let file = File::open("Path");
    let number = file.unwrap().metadata().unwrap().ino();
}
