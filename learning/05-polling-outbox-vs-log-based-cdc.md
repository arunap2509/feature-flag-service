# Polling Outbox vs. True Log-Based CDC (Change Data Capture)

## 1. The Core Distinction

Many engineering discussions conflate "Transactional Outbox" with "CDC". They are distinct concepts:

* **Transactional Outbox**: An architectural pattern where business state and event records are written into the same database within a single ACID transaction (`BEGIN ... COMMIT`).
* **Change Data Capture (CDC)**: A data integration mechanism that observes and captures changes occurring in a database without using standard application-level SQL queries.

Executing `SELECT ... FOR UPDATE SKIP LOCKED` on a timer is **Polling-based Outbox** (a database queue pattern). It is **not** true CDC.

---

## 2. Architectural Comparison

| Dimension | Polling-Based Outbox | True Log-Based CDC *(The Real Deal)* |
| :--- | :--- | :--- |
| **Data Source** | Queries the database table via SQL (`SELECT`). | Tails the internal binary **Write-Ahead Log (WAL)**. |
| **Database Engine Overhead** | Consumes SQL parser, query planner, buffer pool, and table lock manager. | **Zero SQL execution**. Streams raw binary WAL bytes over a streaming replication TCP connection. |
| **Propagation Latency** | Bounded by the sleep/poll interval (e.g. 100ms – 1s). | **Sub-millisecond**: The instant `COMMIT` flushes to the WAL, the engine pushes the record over the replication socket. |
| **Database Write Bloat** | Requires `UPDATE outbox SET sent = true` or `DELETE`, triggering PostgreSQL MVCC dead-tuple accumulation and vacuuming. | The publisher merely advances its **Log Sequence Number (LSN)**. No writes or updates are needed on the database. |
| **Connection Type** | Normal client connection pool (`database/sql`). | Native PostgreSQL **Logical Replication Protocol** connection (`wal_level = logical`). |

---

## 3. How PostgreSQL Logical Replication Works

PostgreSQL implements logical replication via a built-in streaming protocol:

```
[ Application Write ] ──► [ Local Transaction Commit ]
                                     │
                                     ▼
                        [ PostgreSQL Write-Ahead Log (WAL) ]
                                     │ (Logical Decoding Plugin: "pgoutput")
                                     ▼
                        [ Logical Replication Slot ]
                                     │ (Continuous TCP Streaming Socket)
                                     ▼
                        [ Pure Go CDC Worker (pglogrepl) ]
                                     │
                                     ▼
                           [ Kafka / Redpanda ]
```

1. **Write-Ahead Log (WAL)**: Every `INSERT`, `UPDATE`, or `DELETE` is written sequentially to the WAL for crash-recovery and durability.
2. **Logical Decoding (`pgoutput`)**: A built-in PostgreSQL plugin decodes binary WAL records into logical change events (e.g., `INSERT into outbox values (...)`).
3. **Replication Slots**: A server-side cursor that tracks the client's progress using a **Log Sequence Number (LSN)**. PostgreSQL guarantees that WAL segments will not be deleted until the client acknowledges that LSN.
4. **Publications (`CREATE PUBLICATION outbox_pub FOR TABLE outbox`)**: Allows filtering replication strictly to the `outbox` table, completely ignoring other database operations.

---

## 4. Pure Go Log-Based CDC vs. Debezium

Historically, log-based CDC was synonymous with **Debezium** (running on Apache Kafka Connect):

* **Debezium**: Requires a Java Virtual Machine (JVM), Apache Kafka Connect cluster, ZooKeeper or KRaft metadata, and extensive configuration. It typically requires **1.5 GB to 2.5 GB of RAM** just to idle.
* **Pure Go CDC (`jackc/pglogrepl`)**: Implements the PostgreSQL logical replication wire protocol directly in Go. It connects as a replication client, decodes tuples natively, and forwards them to Kafka using `franz-go`. It requires **< 20 MB of RAM** and starts in under 10 milliseconds.

This enables a cloud-native, true log-based CDC pipeline without Java or heavy orchestration infrastructure.
