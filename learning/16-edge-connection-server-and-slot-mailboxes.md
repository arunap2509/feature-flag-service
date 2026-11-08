# Edge Connection Server Fleet & Slot Mailbox Pipeline

## 1. Architectural Motivation: Decoupling Edge from Compute (Tenet #3)

In high-scale real-time push systems (such as LaunchDarkly or our 2:00 AM production architecture), separating **Edge Connection Gateways** from **Rule Evaluation Engines** is essential:

```
[50 Million Clients]
         │ (Persistent HTTP SSE Streams)
         ▼
┌────────────────────────┐
│ Connection Server Fleet│ ── Sized by Blast Radius & OS File Descriptors (~200k sockets)
│  (Stateless Edge SSE)  │ ── Zero business rules, zero flag definitions, minimal RAM
└───────────┬────────────┘
            │
            │ (Kafka Deliveries Mailbox Topic: Partition = Slot ID)
            ▼
┌────────────────────────┐
│   Evaluator Cluster    │ ── Sized by RAM & In-Memory Context (Rules cache + user states)
│ (Consistent Hash Ring) │ ── Pure computational engine (< 1µs evaluation)
└────────────────────────┘
```

### Why Decoupling Matters:
1. **Rolling Deployments Without Connection Drops**:
   When targeting rules or evaluation code is redeployed, Evaluators can be rolled or restarted without terminating millions of open client mobile/browser TCP sockets.
2. **Distinct Scaling Dimensions**:
   - Connection Servers scale with **concurrent socket connections** (OS file descriptors, socket buffer memory).
   - Evaluators scale with **in-memory rule volume, user contexts, and evaluation CPU**.

---

## 2. Slot Mailboxes in Kafka (`deliveries` Topic)

### The $N \times M$ Fan-Out Problem:
If 20 Evaluator nodes pushed diff patches directly to 250 Connection Servers via HTTP or gRPC:
- $20 \times 250 = \mathbf{5{,}000}$ persistent, fragile network connections.
- If Connection Server 5 undergoes a garbage collection pause, Evaluator A hangs trying to deliver diffs, blocking updates to healthy connection servers.

### The Solution: Kafka Topic Partitions as Mailboxes
```
Evaluator 1 ──► [ Partition 0 ] ──► Connection Server (Slot 1) ──► SSE Clients
Evaluator 2 ──► [ Partition 1 ] ──► Connection Server (Slot 2) ──► SSE Clients
```
- Each Connection Server claims a unique **Slot ID** (Slot 1, Slot 2...) in `etcd` (`/members/conn-servers/slot-<id>`).
- The `deliveries` Kafka topic is provisioned with matching partitions (`Partition = (SlotID - 1) % NumPartitions`).
- The Connection Server consumes **specifically its assigned partition** (`kgo.ConsumePartitions(...)`).
- Evaluators batch diff patches by `ServerSlot` and publish directly to that slot's partition (`kgo.ManualPartitioner()`).
- Kafka absorbs bursts, provides backpressure, and decouples Evaluators completely from Connection Server network IPs.

---

## 3. Shard Routing and Initial Handshake (`put`)

```
[ Client App ]
      │ 1. GET /api/v1/stream?env_id=env_prod&user_id=usr_alice
      ▼
[ Connection Server (Slot 1) ]
      │ 2. ShardID = ComputeShard("usr_alice", ShardCount) -> Shard 2
      │ 3. ring.GetOwner("env_prod:shard:2") -> evaluator-1
      │ 4. HTTP POST http://evaluator:9001/api/v1/sessions/register
      ▼
[ Evaluator 1 ]
      │ 5. Evaluates all flags in memory (< 1µs)
      │ 6. Registers session in Shard 2 registry with ServerSlot = 1
      │ 7. Returns: { flags: { "show_animation": true }, environment_version: 31 }
      ▼
[ Connection Server ]
      │ 8. Emits initial SSE frame:
      │    event: put
      │    id: 31
      │    data: {"type":"put", "flags": {...}}
      ▼
[ Client App ]
```

---

## 4. End-to-End Real-Time Change Flow (`patch`)

```
[ PM / Engineer ]
      │ 1. POST /api/v1/environments/env_prod/flags/show_animation/kill
      ▼
[ Management API ]
      │ 2. Single DB Transaction: Update flag to 'off' + Insert outbox event
      ▼
[ Outbox CDC Tailer ]
      │ 3. Reads WAL logical replication -> Publishes to Kafka `flag-changes`
      ▼
[ Evaluators (Broadcast) ]
      │ 4. Re-evaluate Shard 2: show_animation flipped true -> false
      │ 5. Group patches for ServerSlot: 1
      │ 6. Publish to Kafka `deliveries` (Partition 0)
      ▼
[ Connection Server (Slot 1) ]
      │ 7. Reads batch from Partition 0
      │ 8. Matches connection_id in local registry
      │ 9. Emits SSE frame:
      │    event: patch
      │    data: {"type":"patch", "changes": {"show_animation": false}}
      ▼
[ Client App ] (UI toggles in < 100ms with zero polling)
```
