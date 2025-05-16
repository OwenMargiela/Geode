pub struct LockData {
    lock_type: String,
    txn_num: AtomicU32,
    ticker: AtomicU32,
}

pub enum TxnTrac {
    page
    guard
    status
}

enum Lock {
    EXLOCK
    SHLOCK
}


pub struct Flusher {
    inner: Arc<BufferPoolManager>,

    txn_table:   DashMap<u32, TxnTrace>
    lock_table:  DashMap<PagePointer, LockData>,
    guard_table: DashMap<PagePointe, PageGuard>
}


impl Flusher {
    
    aqquire_ex( Page u32 ) -> Result<WriteGuard>

        lock_table.get( Page ).is_none {
            bpm.write_page( Page )
            
            // Clears shared lock and replace it with exclusive one
            update_table( Page, lock::EXLOCK  )
        } else {
            log_transaction( Page )
            err
        }
    
    aqquire_sh( Page u32 ) -> Result<ReadGuard>

        lock_table.get( Page ).lock_data == EXLOCK {
            log_transaction( Page )
            err
        } else {
            bpm.write_page( Page )
            update_table( Page, lock::SHLOCK   )
        }
    
    is_lock_ex( Page u32 ) -> bool
        lock_table.get( Page ).lock_data == EXLOCK

    
    aqquire_context_ex( [Page u32] ) -> Result<ContextEx>
        context 
        for page in [Page u32]
            guard = aqquire_ex( page )
            guard.is_ok {
                context.push_front( guard )
            } else {
                print_stack(context)
                err
            }

    aqquire_context_sh( [Page u32] ) -> ContextSh
        context 
        for page in [Page u32]
            guard = aqquire_sh( page )
            guard.is_ok {
                context.push_front( guard )
            } else {
                print_stack(context)
                err
            }

    read( Page u32 ) -> TreePage

    // Removes the guard from the map and flushes data
    write_all( Page u32, Page Data TreePage )

    log_transaction( Page ) 

    update_table( Page, lock::EXLOCK  )
    

      
}
