# Component Deep Dive: Outbox CDC Publisher

## 1. Executive Summary & Architectural Role

The **Outbox CDC Publisher** ([`cmd/outbox-pub/main.go`](file:///Users/arunap/Project/Lab/feature-flag-service/cmd/outbox-pub/main.go)) bridges PostgreSQL and Kafka without introducing the dual-write problem.

Instead of running recurring polling queries (`SELECT * FROM outbox WHERE processed = false FOR UPDATE SKIP LOCKED`), which creates database CPU spikes, table bloat, and seconds of latency, this service uses **Pure Go Log-Based Change Data Capture (CDC)** via PostgreSQL's streaming logical replication protocol.

```mermaid
flowchart LR
    subgraph PostgreSQL Primary
        TX[Management API Commit] -->|Write to WAL| WAL[(WAL Engine)]
        WAL -->|Logical Decoding pgoutput| Slot[Replication Slot: outbox_cdc_slot]
    end

    subgraph Outbox CDC Publisher (Pure Go)
        Slot -->|Raw WAL Stream pglogrepl| Parser[Tuple Parser]
        Parser -->|Commit LSN Update| FlushTracker[confirmed_flush_lsn Tracker]
        FlushTracker -->|StandbyStatusUpdate| Slot
    end

    subgraph Kafka Control Plane
        Parser -->|Produce Record Key: env_id| Topic["Kafka: flag-changes"]
    end
```

---

## 2. Why Log-Based CDC Over Polling?

| Metric / Characteristic | Polling Outbox (`SELECT ... SKIP LOCKED`) | Pure Go Log-Based CDC (`pglogrepl`) |
| :--- | :--- | :--- |
| **End-to-End Latency** | 500ms – 5,000ms (governed by poll interval) | **< 5ms** (immediate push from WAL) |
| **Database Overhead** | High IOPS, table index thrashing, VACUUM pressure | Zero query overhead; reads sequential disk log |
| **CPU Impact at Idle** | Constant polling queries 24/7 | 0% database CPU when no flag changes occur |
| **External Dependencies** | Requires Debezium / Kafka Connect cluster | **Zero JVM dependencies**; single static Go binary |

---

## 3. Implementation Details ([`cmd/outbox-pub/main.go`](file:///Users/arunap/Project/Lab/feature-flag-service/cmd/outbox-pub/main.go))

### A. Replication Connection & Slot Setup
The publisher establishes a low-level replication connection to PostgreSQL by setting `replication=database` in runtime connection params ([line 95](file:///Users/arunap/Project/Lab/feature-flag-service/cmd/outbox-pub/main.go#L95)):

```go
pgConfig.RuntimeParams["replication"] = "database"
conn, err := pgconn.ConnectConfig(ctx, pgConfig)
```

It ensures the physical replication slot (`outbox_cdc_slot`) exists:
```go
_, err = pglogrepl.CreateReplicationSlot(ctx, conn, slotName, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
    SnapshotAction: "NOEXPORT_SNAPSHOT",
})
```

### B. Streaming WAL Replication Protocol
It invokes `pglogrepl.StartReplication` tied to the PostgreSQL publication [`PublicationOutbox`](file:///Users/arunap/Project/Lab/feature-flag-service/internal/constants/constants.go#L68) (`outbox_pub`):
```go
pluginArgs := []string{
    "proto_version '1'",
    fmt.Sprintf("publication_names '%s'", constants.PublicationOutbox),
}
err = pglogrepl.StartReplication(ctx, conn, slotName, 0, pglogrepl.StartReplicationOptions{
    PluginArgs: pluginArgs,
})
```

### C. Message Parsing & LSN Flush Tracking
The CDC loop processes logical replication messages emitted by PostgreSQL's `pgoutput` plugin:
1. `*pglogrepl.RelationMessage`: Tracks schema metadata and column mappings.
2. `*pglogrepl.InsertMessage`: Fires when a new outbox entry is written. Extracts `environment_id` and the JSON `payload`.
3. `*pglogrepl.CommitMessage`: Marks the end of a transaction.
4. **LSN Advancement (`confirmed_flush_lsn`)**:
   After producing the event to Kafka, the service immediately transmits a [`StandbyStatusUpdate`](file:///Users/arunap/Project/Lab/feature-flag-service/cmd/outbox-pub/main.go#L45):
   ```go
   pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
       WALWritePosition: lsn,
       WALFlushPosition: lsn, // Crucial: advances confirmed_flush_lsn
       WALApplyPosition: lsn,
       ClientTime:       time.Now(),
   })
   ```

> [!IMPORTANT]
> Advancing `WALFlushPosition` informs PostgreSQL that this WAL segment has been durably handled. If this position is not acknowledged, PostgreSQL will retain WAL logs indefinitely on disk until storage fills up (`wal_keep_size` exhaustion).

### D. Kafka Partitioning & Ordering
The publisher sends records to the Kafka topic [`TopicFlagChanges`](file:///Users/arunap/Project/Lab/feature-flag-service/internal/constants/constants.go#L86) (`flag-changes`).
- **Partition Key**: `environment_id`
- **Result**: All flag, segment, and kill-switch events for a given environment are guaranteed to arrive in strict sequential order on the same Kafka partition.

---

## 4. Failure Modes & Resiliency

1. **At-Least-Once Delivery**:
   If the publisher crashes after emitting to Kafka but before sending the `StandbyStatusUpdate` back to Postgres, it will re-read the last unacknowledged transaction on restart. Downstream evaluators handle this idempotently by checking `environment_version`.
2. **Replication Connection Drop**:
   PostgreSQL terminates idle replication streams if keepalives fail. The publisher handles `PrimaryKeepaliveMessage` with `ReplyRequested=true` and runs an internal 10-second heartbeat timer.
3. **Kafka Backpressure**:
   The Kafka producer client (`twmb/franz-go`) synchronously or asynchronously buffers records. If Kafka is unavailable, the publisher halts LSN acknowledgment, freezing replication until Kafka recovers without losing state.
