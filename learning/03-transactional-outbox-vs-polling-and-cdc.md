# The Transactional Outbox Pattern: Dual-Write Prevention and Event Streaming

## 1. The Core Problem: The Dual-Write Trap

In distributed microservices, a common requirement is to persist state to a database and notify other services via a message broker (e.g. Kafka):

```go
// ANTI-PATTERN: The Dual-Write Problem
func UpdateFlag(...) error {
    db.Exec("UPDATE flags SET ...")      // Step 1: Database mutation
    kafkaProducer.Send("flag-updated")    // Step 2: Message broker publish
}
```

This simple flow has a fatal flaw: **there is no distributed atomic transaction between PostgreSQL and Kafka**.

### Failure Scenarios:
1. **DB succeeds, Kafka fails**:
   The database commits, but the network drops, Kafka is temporarily unreachable, or the application container crashes before `kafkaProducer.Send` completes. The database is updated, but downstream evaluators never learn about the change.
2. **Kafka succeeds, DB fails**:
   If you flip the order and publish to Kafka first, but the database transaction fails a constraint or times out, downstream consumers receive an event for a state change that was never committed.

Two-Phase Commit (2PC) is notoriously complex, slow, and not supported by Kafka.

---

## 2. Why Evaluators Cannot Poll the Database Directly

An alternative naive approach is having downstream evaluators poll the database:
`SELECT * FROM flags WHERE updated_at > :last_poll;`

In a production feature flag service, this causes immediate failure:

1. **Database Connection & CPU Starvation**:
   A typical feature flag service operates dozens of evaluator instances serving tens of thousands of client SSE connections. If 20 evaluators poll the database every 500ms, PostgreSQL spends all its CPU answering identical queries where 99.9% of the time nothing changed.
2. **High Latency & Missed SLAs**:
   Feature flag architectures demand **sub-100ms flag propagation**. Polling introduces an artificial delay bounded by the polling interval (1 to 5 seconds). If an emergency kill switch is pressed, waiting 5 seconds for evaluators to discover it is unacceptable.

---

## 3. The Solution: Transactional Outbox Pattern

The Transactional Outbox pattern leverages **PostgreSQL's local ACID transaction guarantee**:

```
[ Client Request ]
       │
       ▼
┌────────────────────────────────────────────────────────┐
│ PostgreSQL                                             │
│                                                        │
│  BEGIN TRANSACTION;                                    │
│    UPDATE flags SET ...;                               │
│    INSERT INTO outbox (env_id, env_version, payload);  │
│  COMMIT;  ◄── Guaranteed atomic (all-or-nothing)       │
└──────────────────────────┬─────────────────────────────┘
                           │
                           ▼
              ┌──────────────────────────┐
              │ Outbox Publisher Daemon  │
              └────────────┬─────────────┘
                           │
                           ▼
                 [ Kafka / Redpanda ]
                           │
                           ▼
                  [ Evaluator Nodes ]
```

Because `flags` and `outbox` reside in the same relational database, writing to both in a single transaction guarantees that:
* If the flag change commits, the outbox event is **guaranteed** to be stored.
* If the flag change aborts, the outbox event is **guaranteed** to disappear.

---

## 4. Why Outbox is a Table vs. CDC (Change Data Capture)

A common point of confusion is whether the Outbox pattern is a table or an application, and how it relates to CDC.

### Outbox Table + Publisher = The Complete Pattern
1. **Outbox Table**: The physical PostgreSQL table that stores pending messages.
2. **Outbox Publisher**: The daemon or worker process that reads rows from the outbox table and delivers them to the message broker (Kafka).

### How CDC Relates to Outbox
**CDC is not an alternative to the outbox pattern; CDC is one way to implement the outbox publisher.**

There are two primary methods to extract events from the outbox table:

| Strategy | How it Works | Pros | Cons |
| :--- | :--- | :--- | :--- |
| **Log-Based CDC**<br>*(e.g. Debezium)* | Tails the PostgreSQL Write-Ahead Log (WAL) directly using replication slots (`pgoutput`). | Zero query load on Postgres; sub-millisecond WAL tailing. | Heavy operational footprint: requires Kafka Connect, JVM, ZooKeeper/KRaft, and replication slots (~1.5 GB RAM). |
| **Polling Publisher Daemon**<br>*(Lightweight Go worker)* | Polls `SELECT * FROM outbox WHERE sent = false LIMIT 100 FOR UPDATE SKIP LOCKED` and publishes to Kafka. | Ultra-lightweight (~15 MB RAM in Go); zero external infrastructure; perfect for local dev & moderate scale. | Small periodic query overhead on Postgres. |

---

## 5. Why Not Attach CDC Directly to the `flags` Table?

Why even have an `outbox` table if Debezium can read the `flags` table directly?

1. **Domain Events vs. Relational Rows**:
   The `flags` table stores current state. An outbox event captures a **domain event** (e.g., `FlagKilled`, `SegmentMembershipChanged`, `RollbackExecuted`), including metadata like `changed_by`, `trace_id`, and `environment_version`.
2. **Schema Decoupling**:
   If you alter or split columns in the `flags` table, direct CDC on `flags` immediately breaks downstream consumers. An `outbox` table preserves a stable, versioned event contract.
3. **Multi-Table Atomic Aggregation**:
   When a single action affects multiple tables (e.g. updating a flag and advancing an environment version), an outbox table allows publishing a single combined atomic event rather than fragmented table-level diffs.
