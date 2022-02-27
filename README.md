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
- [ ] Notify client when the transaction is committed
- [ ] Query the latest committed transaction IDs at a given time
- [ ] Query the entities at a given commit
- [ ] Query the change of entities between 2 given commits
- [ ] Query schemas for a give DB
- [ ] Abort uncommitted transaction
- [ ] User management & access control
- [ ] Real time query subscription
- [ ] B+ tree based indexing
- [ ] Multi instance data store with partitioning

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
   ```
   has latest commit: {1 2022-02-27 06:55:12.108527 +0000 UTC}
   [{1 user map[firstName:Harry lastName:Potter]} {2 user map[firstName:Tony lastName:Stark]} {3 user map[firstName:Princess lastName:Leia]}]
   ```