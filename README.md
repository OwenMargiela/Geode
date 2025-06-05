## Geode DB  
A minimal embedded key-value store built in Rust, powered by a custom B+Tree storage engine.  
Designed for educational and practical systems-level understanding of storage engine implementations.

### Main features:

* **[B+Tree Indexing]**  
  Disk-based n-ary tree with a variable but often large number of children per node.  
  Efficient O(log n) retrieval, deletion, and scans.

* **[Crabbing/Coupling]**  
  Lock crabbing/coupling to allow multiple threads to access and modify the B+Tree at the same time.

* **[Buffer Pool Manager]**  
  Asynchronous page loading and eviction with the LRU-K replacement policy.

---

This project was written as a means to understand database internals.  
Researching and eventually implementing an on-disk B+Tree, in particular, had been something I'd been looking forward to for months.  
Apart from being my very first systems-level project, it is also my first-ever foray into the world of Rust development!

I'm open to any and all criticism. Don’t be afraid to rip my code to shreds — I can take it. 🙏

---

### 📌 TODO / Future (might be implemented in future projects)

* [ ] Write-ahead logging and crash recovery  
* [ ] MVCC transactions to replace coarse-grained locking strategies  
* [ ] SQL support

---

## References

Amazing references I used to compose Geode DB:

Andy Pavlo's CMU lectures are an absolutely fantastic introduction to database internals.  
A must-read for anyone looking to make a database.

- 🌐 [CMU 15-445 Intro to Database Systems](https://15445.courses.cs.cmu.edu/fall2024/) (A. Pavlo, 2024)  
- 🎥 [YouTube Lecture Series](https://www.youtube.com/watch?v=otE2WvX3XdQ&list=PLSE8ODhjZXjYDBpQnSymaectKjxCy6BYq&index=1)

Mini-LSM introduced me to so many Rust coding conventions that I hope will eventually become second nature to
me.  (Also, I'm most likely going to copy their transaction and WAL system.)

- 🌐 [Building an LSM in a Week](https://skyzh.github.io/mini-lsm/) (Alex Chi Z, 2023–2025)

---

⚠️ Project Status: This project has not yet reached the stage of a Minimum Viable Product (MVP).

While the core components — including the buffer pool, B+Tree index, and asynchronous I/O — are functional, the engine 
lacks the higher-level scaffolding needed to integrate these parts into a complete system. Development is actively ongoing.

