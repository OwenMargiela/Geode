

pub struct _TableId{
    value: u64
}
pub struct _PageId {
    table_id: _TableId,
    value: [u8; 2],
}

pub struct _RowID {
    value: [u8; 6],
}