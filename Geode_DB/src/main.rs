use std::{fs::File, os::unix::fs::MetadataExt};

use utils::fdpool::FdPool;


mod utils;
mod storage;
mod db_types;
mod catalog;

fn main() {
    let file = File::open("Path");

    
    
}
