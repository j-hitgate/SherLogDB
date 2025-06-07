# SherLogDB

<img src="logo.svg" alt="logo" width="300"/>

### Description
SherlogDB is a high-performance columnar DBMS for logs, providing parallel processing, ACID guarantees, an SQL-like query language, TTL-based log deletion, indexing, automatic time-based sorting and more.

### Futures
- **ACID compliance**, achieved through change versioning and a data backup system;
- **Parallel execution** of read, write, and delete requests for logs and storage;
- **SQL-like query language** with support for filtering, sorting, grouping, group filtering, aggregation, and more;
- Capable of writing **400,000 logs per second** (~3 KB each, totaling ~1.2 GB/s) with 200 concurrent writers, each pushing 2,000 logs — measured on a typical SSD with ~600 MB/s write speed;
- Fast retrieval of logs within a specific time range using **indexing**;
- Querying of specific columns without scanning the entire dataset;
- Scheduled log **structuring** and **sorting**;
- **Garbage collector**: scheduled deletion of unused files (**without breaking consistency**);
- Deletion of old logs based on **TTL**;
- Resource usage can be limited and managed through **worker pool** configuration.

(*More details about the features and more can be found in the documentation [Docs.md](Docs.md)*)

### Technologies

Golang v1.23.1

Libs:
- github.com/google/**uuid** - v1.6.0
- github.com/j-hitgate/**sherlog** - v1.0.0
- github.com/joho/**godotenv** - v1.5.1
- github.com/labstack/**echo**/v4 - v4.13.3
- github.com/stretchr/**testify** - v1.10.0
- github.com/vmihailenco/**msgpack**/v5 - v5.4.1

### Build & run
The command to build and run the executable file:
```bash
go build -o sherlogdb
./sherlogdb
```

Configuration of `.env`:
- `PORT` - the port on which the DBMS will run (default `8070`);
- `WRITERS` - the number of launched writers (default `10`);
- `READERS` - the number of launched readers (default `10`);
- `DELETERS` - the number of launched deleters (default `1`);
- `PASSWORD` - admin access password to the DBMS;
- `DB_LOG_LEVEL` - DBMS logging level (default `0`);
- `DB_LOGS_DIR` - folder for storing DBMS logs (if not specified, logs will not be written to disk);
- `LOGS_TTL` - logs time-to-live (default 30 days);
- `ALIGNING_CHUNKS_PERIOD` - chunk alignment frequency (default every 1 minute);
- `DELETING_EXPIRED_CHUNKS_PERIOD` - frequency of checking and deleting expired logs (by default, every 1 hour);
- `REMOVING_FILES_PERIOD` - frequency of removing unused files (by default, every 1 minute).

To gracefully shut down the database — aside from just “pulling the plug” — you can send the following request (the password is specified in the configuration under the `PASSWORD` key):
```bash
curl -X POST http://127.0.0.1:8070/shutdown \
    -H 'Content-Type: application/json'
    -d '{"password": "your_password"}'
```

### APIs
**Logs:**
- **POST /logs** - adding logs
- **POST /logs/search** - searching and getting logs
- **DELETE /logs** - deleting logs

**Storages:**
- **GET /storages** - getting a list of storages
- **POST /storage** - creating a storage
- **DELETE /storage** - deleting a storage

**Admin panel:**
- **POST /shutdown** - shut down the DBMS

(*More details about the API in the file [APIs.md](APIs.md)*)

### Load testing
Command to run the test via [k6](https://k6.io/open-source/): `k6 run vus_test.js`.

The test simulates requests to add (**POST /logs**) and search (**POST /logs/search**) logs from multiple users. Logs generated during the test will be saved at path "*storages/storage/*" which needs to be **pre-created**.

Test configuration in the script `vus_test.js`:
```js
const config = {
    randLogs      // generate random logs (true) or use prepared ones (false)
    minLogsToSend // minimum number of logs that a virtual user sends
    maxLogsToSend // maximum number of logs that a virtual user sends
    writersRatio  // ratio of writers to readers
}
export let options = {
    vus      // number of virtual users
    duration // duration of testing
}
```

### Examples
Let's imagine that we need to store logs of user actions. Let's create a `users` storage for these logs (analogous to a table in SQL):
```bash
curl -X POST http://127.0.0.1:8070/storage \
    -H 'Content-Type: application/json'
    -d '{"storage": "users"}'
```

Now we can add logs to this storage. Let's add a couple:
```bash
curl -X POST http://127.0.0.1:8070/logs \
    -H 'Content-Type: application/json'
    -d '{"storage": "users", "logs": [
        {
            "timestamp": 1748354137,
            "level":     4,
            "traces":    ["req_1f5a4ff6e8"],
            "entity":    "customer",
            "entity_id": "26",
            "message":   "Ordered products",
            "modules":   ["_OrderMicroservice3", "orderAPI"],
            "labels":    ["VIP", "regular"],
            "fields":    {"Product": "Pen", "Price": "8.00", "Number": "5"}
        },
        {
            "timestamp": 1748423691,
            "level":     4,
            "traces":    ["req_6a53f4e387"],
            "entity":    "admin",
            "entity_id": "7",
            "message":   "Changed price of product",
            "modules":   ["_PanelMicroservice2", "changePriceAPI"],
            "fields":    {"Product": "Pen", "NewPrice": "10.00"}
        }
    ]}'
```

If we want to find out what input errors were made by **customers (excluding regulars)** and **admins** over the past 30 days, and also count how many of those errors were made by admins - that is, to find log messages with level 3 (`level == 3`) related to **customers (`entity == "customer"`) whose labels do not include "regular" (`!("regular" => labels")`)** and **admins (`entity == "admin"`)** - we can run the following query:
```bash
curl -X POST http://127.0.0.1:8070/logs/search \
    -H 'Content-Type: application/json'
    -d '{
        "storage":       "users",
        "select":        ["timestamp", "entity", "message", "count[entity == ?0]"],
        "aggreg_values": ["admin"],
        "time_range":    "last 30d",
        "where":         "level == ?0 & ((entity == ?1 & !(?2 => labels)) | entity == ?3)",
        "where_values":  [3, "customer", "regular", "admin"]
    }'
```

As a result, we will get a list with 4 columns specified in `select`, for example:
```json
[
    [1777112345, "customer", "Incorrect password", 2],
    [1777212345, "customer", "Incorrect email",    2],
    [1777312345, "admin",    "Product not exists", 2],
    [1777412345, "admin",    "Invalid new price",  2],
    [1777512345, "customer", "Incorrect password", 2]
]
```
Please note that this query only targets logs that fall approximately within the specified time range (30 days) and only the values of the columns used in the query (`message`, `level`, `entity`, `labels`), along with `timestamp`, which is always loaded. In other words, the database does not scan the entire dataset and all columns, but only what's necessary to execute the query.
Additionally, the values used in the conditions are passed separately via `aggreg_values` and `where_values` to prevent SQL injection.
The results are sorted by date and time (i.e., by `timestamp`) by default, since the logs are already stored in sorted order.