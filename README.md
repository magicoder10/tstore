# tStore

The next generation data store optimized for

- time to market
- scalability
- consistency
- reliability
- speed

## Features

- [x] Create, delete and list database
- [x] Queue & commit incoming transactions
- [x] Generate transaction undo log
- [x] Fetch entities with custom queries
- [x] Query the latest committed transaction ID at a given time
- [x] Query the entities at a given commit
- [x] Query the change of entities between 2 given commits
- [ ] Query schemas for a give DB
- [ ] Notify client when the transaction is committed
- [x] Abort uncommitted transaction
- [x] Persist versioned entities & schema
- [ ] Design data transformation language & APIs
- [ ] User management & access control
- [ ] Real time query subscription
- [ ] Indexing (B+ tree)
- [ ] Distributed storage backend
    - [ ] Partitioning
    - [ ] Consistent hashing
    - [ ] Data replication
- [ ] Distributed query processing
- [ ] Distributed transaction processing

## Prerequisites

- [go 1.18 rc1(Generics)](https://go.dev/dl/#go1.18rc1)

## Getting Started

1. Navigate to `example` dir
    ```bash
    cd example
    ```

2. Start server
    ```bash
    go run server.go
    ```

3. Run client
    ```bash
    go run client.go
    ```

4. Here is the sample output
   ```txt
    has latest commit: {3 2022-03-01 02:08:56.661596 +0000 UTC}
    Transaction ID: 0
    []
    
    Transaction ID: 1
    [{1 user map[firstName:Harry lastName:Potter]}]
    
    Transaction ID: 2
    [{1 user map[firstName:Harry lastName:What]} {2 user map[firstName:Tony lastName:Stark]}]
    
    Transaction ID: 3
    [{2 user map[firstName:Tony lastName:Stark]} {3 user map[firstName:Princess lastName:Leia]} {1 user map[firstName:Harry lastName:What]}]
   ```
