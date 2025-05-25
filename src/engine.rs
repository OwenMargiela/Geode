use std::sync::Arc;

use crate::{ index::tree::db::btree_obj::BPTree, wal::Wal };

pub struct StorageOptions {
    // Block size in bytes
    pub page_size: usize,
    // The approximate leaf node max capacity
    pub target_sst_size: usize,
    pub enable_wal: bool,
}
pub struct RootCatalougue {
    tables: Arc<BPTree>,
    page_directory: Arc<BPTree>,
}

pub struct StorageEngine {
    catalougue: RootCatalougue,
    wal: Option<Wal>,
    state: Arc<BPTree>,
    options: StorageOptions,
}

/*






Storage Engine with the following Btree Schema


Table
Key    | Value
String | Tuple_typle
Name   | Root Page ID | Key Type | Data Type

Page
Key     | Value
Int     | Tuple_typle
Page_id | Is_Alocated | Offset in File



pub enum Commands(){
    PUT
    GET
    SET
    DEL
}
pub struct Wal {
    create() -> Self
    
    Traverses down the write-ahead log and returns a Vector of commands
    build_log



    Accepts a build log a computes commands
    recover
    put(Command, Table, Data
    )
    put_bach
    sync

}



*/
