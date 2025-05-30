## The Buffer Pool Manager

The buffer pool is responsible for moving database pages to and from memory and persistent storage.
It caches the most frequently accessed pages for speedy retrievals, evicting unused or 
cold pages whenever needed. More on eviction strategies in the LRU's readme file. Every database page
consists of no more than 4096 bytes or 4KB of data. These fixed-size buffers are loaded into a logical
unit known as a frame.

Normally, page caching is normally overseen by, the OS page cache. However, rolling our own buffer pool 
manager provides us with the unique benefit of having the database engine gain total control of disk
page management. The DBMS almost always wants to control things itself and can do a better job than the OS:


* Flushing dirty pages to disk in the correct order.
* Better buffer replacement policies.
* Specialized fetching algorithms to optimize query execution.

This is a thread-safe implementation of a buffer pool. Perfect for sharing between several processes at once

## Flusher

The Flusher object wraps around an instance of the buffer pool manager. Its main purpose is to logically 
implement the crabbing/ coupling algorithm for index concurrency control. Future implementation may
do away with such a large grain locking mechanism.
