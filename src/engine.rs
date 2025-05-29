#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{ cell::RefCell, path::Path, sync::{ Arc, Mutex } };

use bincode::config;

use crate::{
    buffer::{ buffer_pool_manager::BufferPoolManager, flusher::Flusher },
    catalog::{ pages_directory_schema, schema::Schema, table_schema },
    index::tree::{
        byte_box::DataType,
        db::btree_obj::{ BPTree, BTreeBuilder },
        tree_page::codec::Codec,
    },
    storage::disk::manager::Manager,
    wal::Wal,
};

pub struct StorageOptions {
    // Block size in bytes
    pub page_size: usize,
    // The approximate leaf node max capacity
    pub target_sst_size: usize,
    pub enable_wal: bool,
}
pub struct TableGenerator;

impl TableGenerator {
    pub fn start_table(
        manager: Arc<Mutex<Manager>>,
        flusher: Arc<Flusher>,
        file_id: u64,
        pages_directory_schema: Schema,
        table_name: String
    ) -> Arc<BPTree> {
        let config = config::standard();

        let page_directory = BTreeBuilder::new()
            .b_parameter(4)
            .tree_schema(Codec {
                key_type: DataType::Varchar(25),
                value_type: DataType::Tuple(pages_directory_schema),
            })
            .build_table(table_name, manager, flusher, file_id);

        unimplemented!()
    }
}

// Single Threaded Storage engine That holds a pointer to a singular table at a time
pub struct StorageEngine {
    pub(crate) options: StorageOptions,

    pub(crate) pages_directory: Arc<BPTree>,
    pub(crate) table_directory: Arc<BPTree>,

    pub(crate) state: Arc<BPTree>,

    pub(crate) bpm: Arc<BufferPoolManager>,

    // Experimental
    pub(crate) wal: Option<Wal>,
}

impl StorageEngine {
    pub fn open() -> StorageEngine {
        let (log_file, log_file_path) = Manager::open_log();

        let path_string = format!("geodeData/base/{}.bin", 0);
        let path = Path::new(&path_string);

        let manager = Arc::new(Mutex::new(Manager::new(log_file, log_file_path)));
        let (file_id, _) = manager.lock().unwrap().create_db_file().expect("File made");

        let bpm = Arc::new(BufferPoolManager::new_with_arc(10, manager.clone(), 2));
        bpm.open_file(path);
        let flusher = Arc::new(Flusher::new(bpm, file_id));

        // Root PageID of 0
        let pages_directory = Arc::new(BPTree {
            b: 2,
            codec: Codec {
                key_type: DataType::Varchar(25),
                value_type: DataType::Tuple(pages_directory_schema()),
            },
            flusher: flusher.clone(),
            index_id: 0,
            root_page_id: RefCell::new(0),
        });

        // Root PagSeID of 1
        let table_directory = Arc::new(BPTree {
            b: 2,
            codec: Codec {
                key_type: DataType::Varchar(25),
                value_type: DataType::Tuple(table_schema()),
            },
            flusher: flusher.clone(),
            index_id: 0,
            root_page_id: RefCell::new(1),
        });
        unimplemented!()
    }

    pub fn start() -> StorageEngine {
        let (log_file, log_file_path) = Manager::open_log();

        let manager = Arc::new(Mutex::new(Manager::new(log_file, log_file_path)));
        let (file_id, _) = manager.lock().unwrap().create_db_file().expect("File made");

        let bpm = Arc::new(BufferPoolManager::new_with_arc(10, manager.clone(), 2));
        let flusher = Arc::new(Flusher::new(bpm, file_id));

        // Root PageID of 0
        let pages_directory = TableGenerator::start_table(
            manager.clone(),
            flusher.clone(),
            file_id,
            pages_directory_schema(),
            String::from("PAGES_DIRECTORY")
        );

        // Root PageID of 1
        let table_directory = TableGenerator::start_table(
            manager.clone(),
            flusher.clone(),
            file_id,
            table_schema(),
            String::from("TABLE_DIRECTORY")
        );

        unimplemented!()
    }
}

impl Manager {
    fn shutdown(page_directory: Arc<BPTree>) -> anyhow::Result<()> {
        unimplemented!()
    }
}
