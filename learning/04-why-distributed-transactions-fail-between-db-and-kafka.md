# Why Distributed Transactions (2PC) Fail Between Relational Databases and Kafka

## 1. The Common Question

A natural initial question in distributed system design is:
> *"Why do we need an Outbox table? Can't we just open a transaction that writes to PostgreSQL and publishes to Kafka, and if either fails, roll back both?"*

The short answer is **no**. PostgreSQL and Kafka cannot share an atomic transaction boundary.

---

## 2. Anatomy of the Failure: Simulating Dual-Write in Code

Let's examine why writing custom coordination logic in application code always fails.

### Failure Scenario A: Kafka First, Then DB Commit

```go
tx, _ := db.Begin()
tx.Exec("UPDATE flags SET state = 'off' WHERE key = 'search_v2'")

// 1. Publish to Kafka first
err := kafkaProducer.Send(msg)
if err != nil {
    tx.Rollback() // Looks safe if Kafka fails...
    return err
}

// 2. Commit PostgreSQL
err = tx.Commit()
if err != nil {
    // 💥 CATASTROPHIC STATE:
    // Kafka already accepted the message and delivered it to consumers.
    // But PostgreSQL failed to commit (disk full, lock timeout, connection dropped).
}
```

* **The Invariant Violation**: In Kafka, messages cannot be "un-sent". Once accepted by the broker partition, consumers (evaluators) read the message immediately.
* **The Result**: Downstream systems believe `search_v2` is `off`, but the database still says `search_v2` is `on`.

---

### Failure Scenario B: DB Commit First, Then Kafka

```go
tx, _ := db.Begin()
tx.Exec("UPDATE flags SET state = 'off' WHERE key = 'search_v2'")

// 1. Commit PostgreSQL first
err := tx.Commit() // Committed to disk!

// 2. Publish to Kafka
err = kafkaProducer.Send(msg)
if err != nil {
    // 💥 CATASTROPHIC STATE:
    // PostgreSQL is already committed. You CANNOT rollback a committed transaction.
}
```

* **The Crash Window**: Even if you add retries to `kafkaProducer.Send`, if the container process crashes (`kill -9`, Out Of Memory, hardware failure) between lines 5 and 8, the message is permanently lost.
* **The Result**: PostgreSQL has the state change, but Kafka never received it. Downstream evaluators remain out of sync indefinitely.

---

## 3. Why Not Two-Phase Commit (2PC / XA)?

In classical enterprise architectures, the **Two-Phase Commit (2PC)** protocol using the **XA standard** attempted to coordinate transactions across disparate systems:

1. **Phase 1 (Prepare)**: The coordinator asks all participants (DB 1, DB 2): *"Can you commit?"*
2. **Phase 2 (Commit)**: If all respond *"Ready"*, the coordinator issues *"Commit"*.

### Why 2PC Is Not Used Between PostgreSQL and Kafka:

1. **Kafka is NOT an XA Participant**: Kafka does not implement the XA protocol and does not accept external 2PC coordinators. Kafka’s internal transactional API only supports Kafka-to-Kafka stream processing (`read-process-write` within Kafka topics).
2. **The Blocking Coordinator Problem**: If the coordinator crashes between Phase 1 and Phase 2, participants hold locks indefinitely. In a database, held locks cause cascading request timeouts and system outages.
3. **Throughput Destruction**: 2PC requires synchronous round-trips over the network while holding database row locks, which reduces system throughput by orders of magnitude.
4. **The Two Generals' Problem**: In distributed computing, reaching 100% guaranteed consensus across an unreliable network is provably impossible without either blocking or risking inconsistency.

---

## 4. The Outbox Pattern Solution

The Transactional Outbox pattern avoids the distributed consensus trap by reducing the problem:

> **Instead of attempting a distributed transaction across two systems, it utilizes a local ACID transaction inside ONE system (PostgreSQL).**

```
┌────────────────────────────────────────────────────────┐
│ PostgreSQL Transaction Boundary                        │
│                                                        │
│  BEGIN;                                                │
│    UPDATE flags SET ...;                               │
│    INSERT INTO outbox (env_id, payload) VALUES (...);  │
│  COMMIT; ◄── Atomic Guarantee (Local WAL)              │
└────────────────────────────────────────────────────────┘
```

Because both tables reside in PostgreSQL, the engine guarantees that:
* Either **both** the flag mutation and the event are persisted to disk.
* Or **neither** is persisted.

Once persisted, an asynchronous background publisher guarantees delivery to Kafka via **At-Least-Once** semantics and retries.
