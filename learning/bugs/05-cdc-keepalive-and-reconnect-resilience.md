# Bug Analysis: CDC Keepalive Deadlock & Reconnection Resilience

## Bug 9: Infinite Dead-Loop on PostgreSQL Replication Connection Loss

### Problem
In `cmd/outbox-pub/main.go`, during the CDC streaming loop:
```go
rawMsg, err := conn.ReceiveMessage(receiveCtx)
if err != nil {
    if pgconn.Timeout(err) {
        continue
    }
    log.Printf("Error receiving replication message: %v", err)
    time.Sleep(1 * time.Second)
    continue
}
```
If PostgreSQL restarted or the TCP connection dropped, `conn.ReceiveMessage` failed with a broken connection error (e.g. `read: connection reset by peer`). The loop logged the error, slept 1s, and retried calling `ReceiveMessage` on the identical broken connection forever. It never exited or reconnected, silently halting the entire feature flag event streaming pipeline.

### Fix
Replaced the infinite loop with an immediate fatal exit:
```go
if err != nil {
    if pgconn.Timeout(err) {
        continue
    }
    if ctx.Err() != nil {
        break
    }
    log.Fatalf("[%s] FATAL: Replication connection lost: %v. Exiting for orchestrator restart.", constants.ServiceNameOutboxPub, err)
}
```
Because the Docker Compose container is configured with `restart: unless-stopped`, the container orchestrator restarts the daemon immediately, triggering its startup retry loop that waits for PostgreSQL to become healthy and establishes a fresh logical replication connection.

---

## Bug 10: Zero-LSN Deadlock on Primary Keepalive Messages

### Problem
At daemon startup, `lastWrittenLSN` begins at `0`.
In `sendStatusUpdate`:
```go
func sendStatusUpdate(ctx context.Context, conn *pgconn.PgConn, lsn pglogrepl.LSN) error {
    if lsn == 0 {
        return nil
    }
    return pglogrepl.SendStandbyStatusUpdate(...)
}
```
When `outbox-pub` starts up and no flag changes have occurred yet, PostgreSQL sends a `PrimaryKeepaliveMessage` with `pkm.ReplyRequested = true`. Because `lastWrittenLSN == 0`, `sendStatusUpdate` returned `nil` without sending any packet over the wire.
After 60 seconds (`wal_sender_timeout`), PostgreSQL assumed the client was dead and forcibly severed the TCP replication connection.

### Fix
When `pkm.ReplyRequested` is received, if `lastWrittenLSN == 0`, the publisher now responds with `pkm.ServerWALEnd`:
```go
if pkm.ReplyRequested {
    targetLSN := lastWrittenLSN
    if targetLSN == 0 {
        targetLSN = pkm.ServerWALEnd
    }
    if err := sendStatusUpdate(ctx, conn, targetLSN); err != nil {
        log.Printf("[%s] Failed to send requested status update: %v", constants.ServiceNameOutboxPub, err)
    }
    nextStandbyMessageDeadline = time.Now().Add(10 * time.Second)
}
```
PostgreSQL receives the heartbeat confirmation and resets its `wal_sender_timeout` timer.

---

## Bug 11: Non-Outbox WAL LSN Stalling

### Problem
Previously, `lastWrittenLSN` was updated only on `*pglogrepl.InsertMessage`. If other database operations occurred (tenant creation, SDK key generation, vacuuming), PostgreSQL's WAL advanced, but `confirmed_flush_lsn` remained behind because no outbox inserts had arrived to acknowledge the commit LSN.

### Fix
Added handler for `*pglogrepl.CommitMessage`:
```go
case *pglogrepl.CommitMessage:
    if msg.CommitLSN > lastWrittenLSN {
        lastWrittenLSN = msg.CommitLSN
    }
    if err := sendStatusUpdate(ctx, conn, lastWrittenLSN); err != nil {
        log.Printf("[%s] Failed to flush commit LSN to Postgres: %v", constants.ServiceNameOutboxPub, err)
    }
```
This guarantees the replication slot continuously advances with the database WAL, preventing replication lag and disk bloat even during periods of non-flag activity.
