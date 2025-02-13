/*
    The replacer trait for replacement policy implementations ( clock, lru, lru-k )
    This API details all the necessary methods that all replacers should implemente


    Replacer
        Removes a frame as defined bythe replacement policy
        Victim ( frame_id ) -> bool

        Pins a frame. This indicates that this framer should not be victimized
        Pin ( frame_id )

        Unpins a frame, indicating that it can be victimized
        Unpin ( frame_id )

        Returns the number of elements in the replacer that can be victimized
        Size () -> usize;



    This will be the replacement policy used in the database unless I find something easier to implements
    pub struct ClockReplacer
    imple Replacer for ClockReplacer {}
*/

// A config file might be useful

/*
    FrameHeader
        Index of the fram this header represents
        frame_id

        Read Write latch
        rwlatch

        The number of pins on this frame keeping the page in memory.
        Multiple transations may hold a reference to a frame_id
        pin_count


        Is dirty flag
        is_dirty

        A pointer to the data this frame holds
        Vec<u8> data

        The page that the data within the frame belongs to
        page_id

*/

/*

    BufferPoolManager
    const size_t num_frames_;

    brief The next page ID to be allocated.
    std::atomic<page_id_t> next_page_id_;


    The latch protecting the buffer pool's inner data structure
    bpm_latch_;

    The frame headers of the frames that this buffer pool manages.
    frames_;

    The page table that keeps track of the mapping between pages and buffer pool frames
    Map<page_id_t, frame_id_t> page_table_;

    A list of free frames that do not hold any page's data.
    free_frames_;

    brief The replacer to find unpinned / candidate pages for eviction.
    replacer_;

    A pointer to the disk scheduler
    disk_scheduler_;


    Ignore for now
    Log Manager




*/

/*

    Buffer::build(  num_frames,
                   &disk_manager,
                   replacer,
                   Option <&log_manager> )

    Size() -> size_t;
    NewPage() -> page_id_t;
    DeletePage(page_id_t page_id) -> bool;
    
    CheckedWritePage( page_id_t page_id, 
                      AccessType access_type = AccessType::Unknown) -> std::optional<WritePageGuard>;
    
    CheckedReadPage( page_id_t page_id, 
                     AccessType access_type = AccessType::Unknown) -> std::optional<ReadPageGuard>;
    
    WritePage(  page_id_t page_id, 
                AccessType access_type = AccessType::Unknown) -> WritePageGuard;
    
    ReadPage (  page_id_t page_id, 
                AccessType access_type = AccessType::Unknown) -> ReadPageGuard;
    
    FlushPage(page_id_t page_id) -> bool;
    
    FlushAllPages();
    
    GetPinCount(page_id_t page_id) -> Option<size_t>;

*/
