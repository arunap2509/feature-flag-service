# Production Safeguards for PostgreSQL `wal_level=logical`

## Overview

A common question among backend and database engineers is:
> *"Does setting `wal_level=logical` cause the database disk to explode with too much storage in production?"*

This document breaks down the actual storage overhead, the real failure mode (abandoned replication slots), and the production circuit breakers used to make logical replication safe at scale.

---

## 1. Storage Overhead: The Minor Factor

Setting `wal_level=logical` adds logical decoding metadata (relation identifiers, column names, old/new row tuples) to the Write-Ahead Log.

* **Typical Write Amplification**: ~10% to 25% increase in WAL volume depending on row size and number of columns.
* **Why it doesn't crash disks by itself**: PostgreSQL writes WAL files in fixed 16 MB segments (e.g. `000000010000000000000001`). During routine checkpoints (typically every 5 to 15 minutes), PostgreSQL recycles or deletes segments that are no longer needed. Active WAL directory size normally hovers stably between 1 GB and 5 GB.

---

## 2. The Real Threat: The Replication Slot Trap

The true cause of database disk exhaustion with logical replication is **lagging or abandoned replication slots**:

```
[ Application Writes ] ──► [ Appends to WAL ]
                                  │
                   Replication Slot Check:
                   "Has slot 'outbox_cdc_slot' acknowledged this LSN?"
                                  │
                  ┌───────────────┴───────────────┐
                 YES                             NO
                  │                               │
        [ Delete/Recycle WAL ]        [ RETAIN WAL ON DISK ]
                                                  │
                                   Consumer is down for 12 hours...
                                                  │
                                   10 GB -> 50 GB -> 250 GB...
                                                  │
                                   💥 DISK HITS 100% CAPACITY
                                   PostgreSQL Enters Panic Mode & Halts
```

If a CDC consumer process (like our Go daemon or Debezium) is terminated and never reconnects, PostgreSQL honors the replication slot contract by **refusing to delete any WAL file** the slot hasn't consumed. The disk will inevitably hit 100% capacity and crash the database.

---

## 3. Production Defense Mechanisms

### A. The Engine Circuit Breaker: `max_slot_wal_keep_size` (PostgreSQL 13+)
Introduced specifically to solve this issue, `max_slot_wal_keep_size` puts a hard ceiling on the maximum volume of WAL that replication slots are permitted to retain:

```ini
# postgresql.conf
max_slot_wal_keep_size = 20GB
```

* **Behavior**: If a slot falls behind by more than 20 GB, PostgreSQL automatically invalidates the slot's `restart_lsn`, allows old WAL segments to be deleted, and preserves database availability.
* **The Trade-off**: The consumer receives `ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE` ("replication slot has been invalidated") and must re-synchronize from a fresh snapshot, but the production database **never crashes**.

---

### B. Scoped Publications (Avoid `FOR ALL TABLES`)
Never use wildcards for logical replication in high-throughput production databases:
* **Bad**: `CREATE PUBLICATION all_pub FOR ALL TABLES;` (replicates high-churn logging or analytics tables).
* **Best Practice**: `CREATE PUBLICATION outbox_pub FOR TABLE outbox WITH (publish = 'insert');` (strictly tracks outbox event inserts).

---

### C. Automated Lag Monitoring & Alerts
In production monitoring (Datadog, Prometheus, CloudWatch), alert on replication slot lag:

```sql
SELECT 
    slot_name,
    active,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS replication_lag
FROM pg_replication_slots;
```

* **Warning Alert**: `replication_lag > 5 GB` for > 10 minutes.
* **Critical Alert**: `active = false` for > 15 minutes.
* **Remediation**: If an abandoned slot is detected, drop it manually via `SELECT pg_drop_replication_slot('slot_name');`.
