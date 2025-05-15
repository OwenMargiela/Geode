# Tree Object

BtreeCode {
    schema: TreeSchema
}

TreeObjectInner {
    // Maintains the Tree
    
    bpm: BufferPoolManager
    
    schema: TreeSchema
    codec: BtreeCodec

    index_name: String,
    file_id: FileId,
    
    d: usize,
    root_page: RefCell<PageId>,
}

BTree {

    // Foward facing API
    inner: TreeObjectInner

}

impl BTree {
    BUILD ( TREE SCHEMA )
    GET
    PUT
    DEL
    SCAN
}