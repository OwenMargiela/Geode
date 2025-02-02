// API run through

/*

    There is no way of knowing the maximum number of slots or tuples since the structure is implemented
    with variable sized tuples.The actual byte array of tuple data will grow to the right while the slots
    grow to the left.Other all other aspects of the footer will be of fixed length.

    Slotted Page
    [ tuple ] [ tuple ] [ tuple ] -->|
    free space  <--|[slot][slot][slot]
    [ meta data ][ freespace pointer ]

    I dont know what the data will be since I'm in the design process :)
        PageID
        Page Size
        Size of freespace
        Next PageID ( but the next page Id should just be the current page's id plus 1)
            Therefore Next Page should be a boolean field

    A system must be implemented to minimize the non-usage of fragmented space within a Page.
    Linearly scaning the offsets stores in within the slots is an idea that can be used.However, a table level free list
    might be a possible implementation strategy. For the creation of a MVP we will use the linear scan.
    The size of fragment space and pointers to the start of fragmented space would be useful.


    enum Data Pages(){
        SlottedPage(Page <>)
        PageDirectory(Page <>)
    }

    struct Page {
        data: Vec<u8>,
    }

*/

/*
    impl SlottedPage for Page {

        param - previous page
        new ( optional < page_id >  )
            self.data = Vec::with_capacity(PAGE_SIZE)

            if ( LOGGING_ENAGLE ) {
                Log helper logic
            }

            Set page meta data

        param - the length of the tuple to be insert
        insertion_offset( data_offset, tuple_len )
            offsets the current free space pointer to the tuple length given
            returns the validity of the offset

        next_pgID ()
            If there is a next page return the Id by increasing the current one by 1

        prev_pgID ()
            Vice-versa

        It makes sense to break up the logic into a seeking method and a writing method
        param - tuple to be inserted and row ID
        insert ( tuple, row ID )

            {
                if there is a fragmented space that
                can hold the inserted tuple we insert

                ptr = frag_spc_ptr_search()
                write(ptr, tuple)
                return

            }{

                if let val insertion_offset( freespace_ptr, tuple.length ) != true
                return Err("Not enough space")

                write(freespace_ptr, tuple)
                return

            }

            Logging Mechanism

        deletion_marker(row_ID){

            adds a field within the meta data of a tuple indicating that it has been deleted
            usefull when needed to rollback inserts
        }

        deletion_apply(row_ID){

            "Physically deletes data" really it just allows either the database to write over these fragmented
            spaces or the compaction processs to...compact.
        }

        rollback_delete(){
            un-deletions your marker :)
        }

        update_tuple(){
            A lgocial combination of the insert and deletion methods
            In the case where the updated tuple cannot fit within its original space cell,
            append it to the beginning of free space then apply an imediate delete of the old value.
        }

        get_tuple(){
            I wonder what this does
        }

    }
*/

/*

    Page Directory: DBMS maintains special pages that track locations of data pages along with the
    amount of free space on each page.
    
    A data page that data only containes the freespace associated with slotted pages
    Implemented as a b-tree map witha pointer to the next page if you just have a 
    god awful amount of pages I gues

    [ key: page_id, value: amount of freespace ]

    Leave implementation to when I'm creating the index
    impl FreeSpaceMap for Page {}

*/
