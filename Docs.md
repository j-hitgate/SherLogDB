# Documentation

### Start & run
When a `Service` is created (`New()`), it registers API handlers. When it starts (`Run()`):
1. It executes transactions from the "*transactions/*" folder that were not completed during the previous run (this folder also stores file backups);
2. It reads storages and chunk metadata from the storages/ folder, while:
    - Older versions of chunks are deleted, keeping only the latest ones;
    - Storages marked as deleted (i.e., containing a "*\_deleted\_*" file) and their chunks are also removed;
3. It starts the log writers, readers, deleters, and the scheduler;
4. It launches the server.

### Directories
- **Storages** are directories located inside the "*storages/*" folder. When a storage is deleted, it is first marked as deleted by adding an empty "*\_deleted\_*" file to its folder. Physical deletion may happen later.
- **Chunks** are folders located inside storages.
    - The name contains an ID and a version of chunk, separated by a symbol '_'. For example, a chunk with ID 5 and version 11 would be named "*5_11/*";
    - Each chunk contains column files and a metadata file ("*meta*");
    - A chunk is considered "raw" if it is still being written to, and its metadata contains offsets for each column;
    - Readers of raw chunks read only the available values from column files, while writers can append new values. This allows for parallel read/write operations without conflicts;
    - Non-raw chunks are sorted by the `timestamp` column.
**Transactions** are instruction/command files located in the "*transactions/*" folder. Transactions are idempotent, meaning they can be safely re-executed without causing errors. Possible commands include:
    - **Cut** - truncate a file to a specified size;
    - **Remove** - delete a file/directory;
    - **Rename** - rename a file/directory.
- **Delete tasks** are files containing user deletion requests (`DeleteQuery` model), stored in the "*delete_tasks/*" folder. A task is removed only after it has been successfully completed.

### Constants
Constants located in the "*models/consts.go*" file:
- `BLOCK_MAX_SIZE` – the maximum number of chunks in a `MetasMap` block (recommended: `100`);
- `MAX_LOGS_IN_CHUNK` – the maximum number of logs per chunk (recommended: `2000`);
- `DIR_STORAGES` – directory for storages;
- `DIR_TRANSACTIONS` – directory for transactions;
- `DIR_DELETE_TASKS` – directory for user log deletion tasks;
- `C_*` (column) – all constants with this prefix represent log column names;
- `AG_*` (aggregator) – all constants with this prefix represent aggregator names.

### Metas map & version control
`MetasMap` is an agent that stores the current state of the database along with its version - i.e., metadata about storages and their chunks. It serves as the **single source of truth** and version controller.

Example of storage structure:
```
- storage 1:
    - Chunk 1 meta
    - Chunk 2 meta
- storage 2:
    - Chunk 1 meta
```

- Each time the agent's state is updated, its version is incremented by one, meaning **every state has its own version**.
- Access to the agent is **exclusive (only one goroutine can access it at a time)**, but it can return a **read-only** snapshot of the current state, which can then be safely used for parallel processing. Keep in mind that this is just a snapshot of a specific version - it does not guarantee the latest state.
- To block the state from further modifications, you can:
    - first retrieve the required chunks from the current version;
    - lock those chunks;
    - then retrieve the chunks again, now with confidence that this snapshot reflects a consistent and up-to-date state, since locked chunks cannot be modified by anyone else;
    - after making the necessary changes, it's important **to update the state first, and only then release the locks** (the necessary operations can be placed in the callback function of the `UpdateStateTask`, which is executed after the state update), because releasing the lock first may allow someone else to lock the chunk and operate on a non-relevant DB state:
    ```js
    - Goroutine A: Chunk version 5 changes started...
    - Goroutine A: Unlocked chunk version 5   // first released the lock
    - Goroutine B: Got chunk version 5
    - Goroutine B: Locked chunk version 5
    - Goroutine A: Updated chunk to version 6 // then updated the version, but goroutine B already caught chunk version 5
    - Goroutine B: Chunk version 5 changes started...
    - Goroutine B: Chunk change error, version 5 is not latest version! // error
    ```
- You can reserve a state version using the `ReserveVersion()` method to prevent chunks/storages from being physically deleted by the garbage collector while they are still being read. In simple terms, it's a way to say: "don't delete what I'm still working with." The `ReserveVersion()` method returns a callback to release the reservation.
- If the new state no longer includes certain chunks or storages (i.e., they've been virtually deleted), their files are added to a deletion queue (picked up by the garbage collector). They are physically deleted only if none of the versions where these chunks/storages still exist are currently reserved.
- State updates can be performed asynchronously by calling the `Update()` method and passing it a task with instructions for updating the state (the `UpdateStateTask` model). These tasks are executed by a single goroutine to ensure the consistency of changes. Since the state must be duplicated during updates to avoid conflicts, the metadata (`Meta` models) has been partitioned into blocks. When an update occurs, only the affected blocks are duplicated, not the entire list. However, if the number of virtually deleted metadata entries exceeds a certain threshold, the entire metadata list is rebuilt to eliminate "holes."

### Condition analizer
Let's take the condition as an example: `level >= ?0 & (entity == ?1 & ?2 => labels) | !(level == ?3)`

1. First, the condition is broken down into pairs of blocks separated by logical operators AND (`&`) and OR (`|`), where the second block is always a simple condition, and the first one is an accumulation of the previous blocks. For example, in the expression `A & B | C`, the first pair will be `A & B` (let's call it condition `D`), and the second pair will be `D | C`. Parentheses are evaluated first, then outer expressions. So, the expression above can be represented as a tree:
```
       entity == ?1     ?2 => labels
             \               /
              \____[ & ]____/
                    /
level >= ?0        /
      \           /
       \__[ & ]__/
             \        !(level == ?3)
              \             /
               \___[ | ]___/
```
(*In this tree, the leaves are **simple conditions**, and the combined nodes are **compound conditions***)

To negate a condition, wrap it in parentheses and add a `!` in front. For example, `!(level == 4)` will evaluate to true if level is any value other than 4.

Each condition pair, along with its logical operator, is stored in the `Comparator` agent.

2. Next, simple conditions are analyzed. The system parses the operator (`==`, `!=` for equality, `>`, `<`, `>=`, `<=` for comparison, and `=>` for array inclusion) and two operands. Operands can be:
    - A literal value like `?i`, where `i` is the index of a value in the `where_values`, `having_values`, or `aggreg_values` arrays;
    - A log column (`level`, `entity`, `labels`, etc.), whose value will be substituted by the column's actual data;
    - An aggregation function (`avg`, `count`, `max`, etc.), whose result will be used in place of the function itself.

In a `where` condition, aggregations are not allowed as operands, since it is used for filtering individual records from the database. Conversely, in a `having` clause, log columns (except for the grouping column) are not allowed, as this clause is meant for filtering groups, not individual records.

Operand types are also taken into account:
- For `==` and `!=`, both operands must be of the same type;
- For `>`, `<`, `>=`, and `<=`, both operands must be numbers;
- For the `=>` operator, the second operand must be an array, and its elements must be of the same type as the first operand.

The available types for aggregations and log columns can be found in the file [models/consts.go](models/consts.go).

### Aggergators
Aggregators are agents responsible for aggregating log values. In query conditions, aggregators are represented as functions with parameters in the form `aggr_name[arg1, arg2, ...]`. The DBMS supports the following aggregators:
- `avg[column, condition]` - calculates the average of the `column` values that match the `condition`;
- `count[condition]` - counts the number of records that match the `condition`;
- `max[column, condition]` and `min[column, condition]` - find the maximum and minimum values of the `column` for records that match the `condition`;
- `sum[column, condition]` - calculates the sum of `column` values for records that match the `condition`.

(*The `condition` parameter is optional in all aggregators. If omitted, the aggregation will apply to all records*)

### Writer
`Writer` is an agent responsible for writing logs into chunks within a specified storage. To start a writer worker and send logs to it via a channel, call the `RunWriter()` method.

Upon initialization, the agent automatically launches goroutines for writing to individual columns (`columnWriter`), allowing simultaneous writing to all columns.
Each `columnWriter` performs the following steps:
- retrieves the value from the log based on its assigned column;
- converts the value to bytes using the `msgpack` package;
- gets the length of the encoded byte array and converts it into a 2-byte array (i.e., the length is equivalent to a `uint16`);
- Then appends both the length and value byte arrays to a slice of byte arrays to be written to a file.

Example:
```go
ptrToValue, _ := log.Get("some_column")
data, _ := msgpack.Marshal(ptrToValue)
lenBytes := lenToByteArray(len(data))
bytesArrayForWrite = append(bytesArrayForWrite, lenBytes, data)

// Example of what will be in a column-file (x - some byte):
// 0 3 x x x 0 2 x x 0 4 x x x x
```
This format allows the reader to decode each value by reading the first two bytes to determine the length of the encoded value, reading the exact number of bytes needed, and then moving on to the next value.

If the number of logs to be written fits into the current chunk, they are simply appended. For example, if a chunk has a maximum size of 2000 logs and currently contains 1000, another 100 logs can be easy added.

If the number of logs exceeds the chunk capacity, the following algorithm is used:
- Read the existing logs from the chunk;
- Calculate how many new logs can still be added to the chunk and take that slice;
- Merge the existing logs and the slice of new logs into a single array and sort it;
- Write the sorted logs to a new version of the current chunk;
- Move on to the next chunk.

Before writing, the chunk is backed up to allow rollback in case of a failure during the write. Only after a successful write is the backup discarded, effectively confirming the changes. After that, the state in `MetasMap` is updated.

To determine the next chunk ID for writing, the writer adds the total number of writers to the current chunk ID. For instance, if 3 writers are writing to chunks with IDs 1, 2, 3, their next chunk IDs will be 4, 5, 6, respectively. This mechanism allows writers to work in parallel without interfering with each other.

### Reader
`Reader` is an agent designed to read logs from chunks in a specified storage. To start a reader worker and send it log reading tasks via a channel, call the `RunReader()` method.

Upon initialization, the agent automatically launches goroutines for reading individual columns (`columnReader`), allowing simultaneous reading all columns.
Each `columnReader` reads a column-file, and then, in a loop, reads the first 2 bytes to determine the length of the encoded value, it then reads the corresponding byte slice and decodes the value using the `msgpack` package, placing it into the log entry field.

The number of values read matches the number of logs available for reading (as defined in the chunk's metadata). For example, if a chunk physically contains 6 complete logs and 1 incomplete (corrupted) one, but only 5 are marked as available, only these 5 values will be read - other data will be ignored.

### Deleter
`Deleter` is an agent responsible for **virtually deleting** logs and chunks from the specified storage. To start a deleter worker and send it log deletion requests via a channel, call the `RunDeleter()` method.

When a user requests log deletion, a task is created that will be processed incrementally. In other words, **deletion does not happen immediately, but it is guaranteed to happen**.

Log deletion works in two ways:
- **By time range.** If the deletion request includes only a time range, the deletion is based solely on it:
    - Logs within the range are removed from chunks;
    - Entire chunks are deleted if all their logs fall within the range.
- **By condition.** If a condition and time range (optionally) is specified, logs are deleted based on that condition and time range, i.e. chunks are read and logs matching the condition (and time range, if specified) are filtered:
    - If no logs match, nothing happens;
    - If some logs match, a new version of the chunk is created without the deleted logs;
    - If all logs match, the entire chunk is deleted.

### Scheduler
`Scheduler` is an agent responsible for launching background workers that perform scheduled tasks:
- `Aligner` - retrieves non-raw chunks with overlapping time ranges from `MetasMap`, performs "alignment" (i.e., reorganizes them), and writes new versions of the chunks to disk. The time range of a chunk is defined by the `timestamp` of its newest and oldest log. For example, given 3 chunks: `[1 5 6], [2 3 7], [4 8 9]`, the "aligned" version would be: `[1 2 3], [4 5 6], [7 8 9]`.
- `ExpiredDeleter` - retrieves chunks from `MetasMap` whose logs are all expired and **virtually deletes** them.
- `Remover` - retrieves files from `MetasMap` for **physical deletion**.

To run these workers, call the methods `RunAligner()`, `RunExpiredDeleter()`, and `RunRemover()`, respectively.

### Log processor
`LogProcessor` is an agent that processes logs passed to it (filters, groups, aggregates, etc.) based on a specified query (`SearchQuery` model), and returns the result as a matrix of rows and columns.

1. First, the user query is passed to the agent for:
    - Analyzing filter conditions;
    - Analyzing aggregations;
    - Parsing the time range for log search;
    - Determining which data to load (`LogLoadData` model);
    - Validating the query as a whole.
2. After analyzing the query, logs can be provided to the agent either via the `PutLogs()` method or the `PutLogsFromChannel()` method (if logs are coming through a channel). During log ingestion:
    - Logs are filtered according to the `where` condition;
    - Grouping logs by the `group_by` column (if it was specified in the request);
    - Logs are passed through aggregators.
3. Then, the results can be retrieved by calling `GetResult()`, which performs:
    - For logs:
        - Merging and sorting the logs by `order_by` column;
        - Slicing logs using `offset` and `limit`;
        - Generate results according to what is specified in `select`.
    - For groups:
        - Retrieving aggregator results for each group;
        - Sorting the groups by `order_by`;
        - Slicing groups using `offset` and `limit`;
        - Generate results according to what is specified in `select`.