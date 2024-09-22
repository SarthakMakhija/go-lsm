# go-lsm
[![Build](https://github.com/SarthakMakhija/go-lsm/actions/workflows/build.yml/badge.svg)](https://github.com/SarthakMakhija/go-lsm/actions/workflows/build.yml)

Implementation of LSM tree in Golang, mainly for educational purposes.

Rewrite of the existing [workshop](https://github.com/SarthakMakhija/storage-engine-workshop) code.

Inspired by [LSM in a Week](https://skyzh.github.io/mini-lsm/00-preface.html)

### Building blocks of LSM-based key-value storage engine

1. **Memtable** is an in-memory data structure which holds versioned [key](https://github.com/SarthakMakhija/go-lsm/blob/main/kv/key.go) and [value](https://github.com/SarthakMakhija/go-lsm/blob/main/kv/value.go) pairs.
Memtable uses [Skiplist](https://tech-lessons.in/en/blog/serializable_snapshot_isolation/#skiplist-and-mvcc) as its storage data structure.
The [Skiplist](https://github.com/SarthakMakhija/go-lsm/blob/main/memory/external/skiplist.go) implementation in this repository is shamelessly take from [Badger](https://github.com/dgraph-io/badger).
It is a lock-free implementation of Skiplist. It is important to have a lock-free implementation, otherwise scan operation will take lock(s) (/read-locks) and it will start interfering with write operations.
Check [Memtable](https://github.com/SarthakMakhija/go-lsm/blob/main/memory/memtable.go).

3. **WAL** is a write-ahead log. Every transactional write is stored in a memtable which is backed by a WAL. Every write to memtable (typically a [TimestampedBatch](https://github.com/SarthakMakhija/go-lsm/blob/main/kv/timestamped_batch.go)) involves writing every key/value pair from the batch to WAL.
This implementation writes every key/value pair from the batch to WAL individually. An alternate would be to serialize the entire [TimestampedBatch](https://github.com/SarthakMakhija/go-lsm/blob/main/kv/timestamped_batch.go) and write to WAL. Check [WAL](https://github.com/SarthakMakhija/go-lsm/blob/main/log/wal.go).

4. **Recovery of Memtable from WAL** involves the following:
    1) Reading the file in READONLY mode.
    2) Reading the whole file in one go.
    3) Iterating through the file buffer (/bytes) and decoding each the bytes to get [key](https://github.com/SarthakMakhija/go-lsm/blob/main/kv/key.go) and [value](https://github.com/SarthakMakhija/go-lsm/blob/main/kv/value.go) pairs.
    4) Storing the key/value pairs in the Memtable.
    
    Check [recovery of Memtable from WAL](https://github.com/SarthakMakhija/go-lsm/blob/main/log/wal.go#L41).
   
5. **Manifest** records different events in the system. This implementation supports `MemtableCreatedEventType`, `SSTableFlushedEventType` and `CompactionDoneEventType` event types. This concept is used in recovering the state of the
LSM ([StorageState](https://github.com/SarthakMakhija/go-lsm/blob/main/state/storage_state.go)) when it restarts. Check [Manifest](https://github.com/SarthakMakhija/go-lsm/blob/main/manifest/manifest.go).

6. **Bloom filter** is a probabilistic data structure used to test whether an element maybe present in the dataset. A bloom filter can query against large amounts of data and return either “possibly in the add” or “definitely not in the add”. It depends on M-sized bit vector and K-hash functions. It is used to check if the application should read an [SSTable](https://github.com/SarthakMakhija/go-lsm/blob/main/table/table.go#L173) during a get operation.
Check [Bloom filter](https://github.com/SarthakMakhija/go-lsm/blob/main/table/bloom/filter.go).
   
8. **Transaction** represents an atomic unit of work. This repository implements concepts to implement ACID properties:
 - [Batch](https://github.com/SarthakMakhija/go-lsm/blob/main/kv/timestamped_batch.go) and [TimestampedBatch](https://github.com/SarthakMakhija/go-lsm/blob/main/kv/timestamped_batch.go) for atomicity.
 - [Serialized-snapshot-isolation](https://github.com/SarthakMakhija/go-lsm/blob/main/txn/transaction.go) for isolation
 - [WAL](https://github.com/SarthakMakhija/go-lsm/blob/main/log/wal.go) for durability.
 
A brief over of serialized-snapshot-isolation:

  1) Every transaction is given a begin-timestamp. Timestamp is represented as a logical clock.
  2) A transaction can read a key with a commit-timestamp < begin-timestamp. This guarantees that the transaction always reads committed data.
  3) When a transaction is ready to commit, and there are no conflicts, it is given a commit-timestamp.
  4) ReadWrite transactions keep a track of the keys read by them.
     Implementations like [Badger](https://github.com/dgraph-io/badger) keep track of key-hashes inside ReadWrite transactions.
  5) Two transactions conflict if there is a read-write conflict. A transaction T2 conflicts with another transaction T1, if, T1 has committed to any of the keys read by T2 with a commit-timestamp greater
  than the begin-timestamp of T2.
  7) Readonly transactions never abort.
  8) Serialized-snapshot-isolation prevents: dirty-read, fuzzy-read, phantom-read, write-skew and lost-update.

More details are available [here](https://tech-lessons.in/en/blog/serializable_snapshot_isolation/). Start understanding [Transaction](https://github.com/SarthakMakhija/go-lsm/blob/main/txn/transaction.go).

7. **Compaction** implementation in this repository is a simpled-leveled compaction. Simple-leveled compaction considers two options for deciding if compaction needs to run.

    - **Option1**: `Level0FilesCompactionTrigger`. Consider `Level0FilesCompactionTrigger` = 2, and number of SSTable files at level0 = 3. This means all SSTable files present at level0 are eligible for undergoing compaction with all the SSTable files at level1.
    
    - **Option2:** `NumberOfSSTablesRatioPercentage`. This defines the ratio between the number of SSTable files present in two adjacent levels: number of files at lower level / number of files at upper level.
    Consider `NumberOfSSTablesRatioPercentage` = 200, and number of SSTable files at level1 = 2, and at level2 = 1. Ratio = (1/2)*100 = 50%. This is less than the configured NumberOfSSTablesRatioPercentage. Hence, table.SSTable files will undergo compaction betweenlevel1 and level2.

In the actual Simple-leveled compaction, we consider the file size instead of number of files. Check [Compaction](https://github.com/SarthakMakhija/go-lsm/blob/main/compact/compaction.go).

8. **Iterators** form one of the core building blocks of LSM based key/value storage operations. Iterators are used in operations like [Scan](https://github.com/SarthakMakhija/go-lsm/blob/main/state/storage_state.go#L184) and [Compaction](https://github.com/SarthakMakhija/go-lsm/blob/main/compact/compaction.go#L75). This repository provides various iterators, (listing a few here): [MergeIterator](https://github.com/SarthakMakhija/go-lsm/blob/main/iterator/merge_iterator.go), [SSTableIterator](https://github.com/SarthakMakhija/go-lsm/blob/main/table/iterator.go) and [InclusiveBoundedIterator](https://github.com/SarthakMakhija/go-lsm/blob/main/iterator/iterator.go).

### Development plan
![LSM development items](https://github.com/user-attachments/assets/47731c33-a642-432e-8a02-1d3146d88e8d)
