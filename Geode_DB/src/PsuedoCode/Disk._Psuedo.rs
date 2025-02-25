/*

    Protoype
    Disk Scheduler
        Bus Tub's  scheduler utalized asynchrous shared queues
        to implement an async i/o mechanism. The usage of io_uring
        mitigates the need for our system to do the same. However,
        they are things to be done to optimize the scheduler in ways
        that suit us.

        A fixed sized pool of open file descriptions will be maintained
        to minimize the overhead of the usage of system calls.Eviction
        strategies must be implemented to prevent fd exhaustion.

        A mechanism needs to be instantiated to ensure a correct mapping
        between page to file

        table_ID - unique table identifier
        page_ID  - unique page identifier
                   comprised of the page offset (monotomically increasing ) and the table_ID

        row_ID   - unique tuple identifier
                   comprised of the page_ID and the slot number
*/

/*
    The General idea is to implement a structure in which multiple request can be produced
    asynchronously. A method that allows for us as the programmer to know when one a given disk
    operation was successful is should also be put in place. Any mechanism used to manage
    amd execute task efficiently, concurrently and thread safe is good enough.


    Non blocking operation

    Blocking when conformation that the task has bin commited sucessfully is needed


*/


/*

    Disk Request
        is_write
        data
        page_id
        table_unique_identifier

    FdPool
        acknowledge that the os can handle only so much open file descriptors at a time
        file_descriptors : Vec< FileDescriptors >


*/

/*

    Where am I putting these files, surely not within the code base?
    In another directory perhaps?
    
    // Attributes
    
    If anything, a shared queue can be implemented if the IO is filled to capacity to aid with retries.
    This is more of an optimization.

    Disk scheduler
        state: IoState // monotomically increasing ID
        fd_pool: FdPool

    
    // Methods

    Disk scheduler
        schedule( DiskRequest ) -> IoFuture


*/
