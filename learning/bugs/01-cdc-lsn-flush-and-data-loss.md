# Bug Analysis: CDC LSN Acknowledgment & Zero-Loss Error Handling

## Bug 1: Stuck `confirmed_flush_lsn` and WAL Retention Bloat

### Problem
In `cmd/outbox-pub/main.go`, the standby status update sent to PostgreSQL was:
```go
pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
    WALWritePosition: lastWrittenLSN,
})
```
`WALFlushPosition` and `WALApplyPosition` were left at default `0`.
In the PostgreSQL streaming logical replication protocol, PostgreSQL tracks two different LSN cursors:
1. `write_lsn`: Bytes received over network by client.
2. `flush_lsn`: Bytes physically committed/flushed by client to durable downstream storage.

PostgreSQL explicitly advances `confirmed_flush_lsn` **only** when `WALFlushPosition` advances. Because `WALFlushPosition` was 0, `confirmed_flush_lsn` was frozen at the initial slot creation snapshot LSN.

### Empirical Evidence
Inspecting `pg_replication_slots`:
```text
slot_name       | restart_lsn | confirmed_flush_lsn
outbox_cdc_slot | 0/19F85F0   | 0/19F969E  (never advanced)
```
Whenever `outbox-pub` restarted, PostgreSQL replayed all previous WAL events, publishing duplicates to Kafka. Over time, PostgreSQL would refuse to recycle WAL files, eventually running the disk out of space.

### Fix
Created `sendStatusUpdate()` which sets all positions synchronously:
```go
pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
    WALWritePosition: lsn,
    WALFlushPosition: lsn, // Signals PostgreSQL that event is committed to Kafka!
    WALApplyPosition: lsn,
    ClientTime:       time.Now(),
})
```
And immediately flush the LSN upon successful Kafka publish, as well as upon `pkm.ReplyRequested`.

---

## Bug 2: Silent Event Dropping on Kafka Produce Failures

### Problem
When publishing to Kafka in `cmd/outbox-pub/main.go`, if retries failed after 2.5 seconds:
```go
if produceErr != nil {
    log.Printf("Dropping or halting on unpublishable event: %v", produceErr)
    continue // permanently skipped!
}
```
If Kafka had a 3-second network blip or partition leader election, the event was silently dropped forever while the worker continued.

### Fix
A CDC publisher must enforce strict **At-Least-Once Delivery**:
1. Implemented exponential backoff with up to 10 retries.
2. If retries are exhausted, the process calls `log.Fatalf()` to halt immediately.
3. Because the LSN was **not** acknowledged to PostgreSQL, the container orchestrator restarts the daemon, which reconnects to the replication slot and retries the exact unacknowledged WAL message.
