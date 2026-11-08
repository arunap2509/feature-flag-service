# PostgreSQL Logical Replication Internals: Parameters Explained

## Overview

When building log-based Change Data Capture (CDC) or streaming data out of PostgreSQL, three critical engine parameters must be configured:

```yaml
command: ["postgres", "-c", "wal_level=logical", "-c", "max_replication_slots=5", "-c", "max_wal_senders=5"]
```

This document explains the internal database mechanics behind each setting and key production trade-offs.

---

## 1. `wal_level = logical`

PostgreSQL records all changes sequentially in its **Write-Ahead Log (WAL)** for durability and crash recovery. The `wal_level` setting controls **how much detail** is written into those logs:

| Level | What It Stores | Use Case |
| :--- | :--- | :--- |
| `minimal` | Only logs what is strictly necessary to recover from a crash. Skips logging tables during bulk copies/index creates. | High-throughput initial data ingestion; no standby replicas allowed. |
| `replica` *(Default)* | Adds physical page changes required to support **physical read replicas** (streaming raw block deltas). | Standard PostgreSQL replication; not readable by logical decoders. |
| `logical` | Adds **logical decoding metadata**: table names, column names, old/new tuple values, and transaction boundaries. | Required for **logical replication, CDC (pglogrepl/Debezium), and zero-downtime major version upgrades**. |

*Without `wal_level=logical`, PostgreSQL will reject any attempt to create a logical replication slot with `ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE`.*

---

## 2. `max_replication_slots = 5`

### What is a Replication Slot?
A replication slot is a **durable, named cursor on the PostgreSQL server** that tracks how far a consumer (like `outbox_cdc_slot`) has progressed in reading the WAL stream:
* Every time our Go worker acknowledges a **Log Sequence Number (LSN)**, PostgreSQL advances the slot.
* **The Invariant Guarantee**: PostgreSQL **guarantees** it will never delete or recycle WAL files from disk that contain unacknowledged changes needed by that slot.

### Why Limit Slots?
`max_replication_slots` defines the maximum number of concurrent replication cursors that can exist simultaneously.

> [!CAUTION] Production Pitfall: The WAL Disk Filling Trap
> Because a replication slot forbids PostgreSQL from deleting WAL files until acknowledged, if a CDC worker crashes and never reconnects, PostgreSQL will continue retaining WAL files on disk indefinitely until the server runs out of disk space. In production, always set `max_slot_wal_keep_size` to cap the maximum disk space a lagging slot can retain.

---

## 3. `max_wal_senders = 5`

### What is a WAL Sender?
In PostgreSQL, active replication connections do not use standard query worker threads. Instead, PostgreSQL spawns a dedicated background process called a **`walsender`**:
* When our Go CDC daemon connects over TCP with `replication=database`, PostgreSQL forks a `walsender` process.
* The `walsender` reads binary WAL buffers and pipes them directly to the client socket using the streaming replication protocol.

### Why Match or Exceed `max_replication_slots`?
* `max_wal_senders` controls the maximum number of active replication processes that can stream simultaneously.
* Each connected replication slot requires **at least 1 WAL sender process**.
* If `max_wal_senders` is lower than the number of active streaming consumers, new replication connections will fail with `FATAL: number of requested standby connections exceeds max_wal_senders`.
