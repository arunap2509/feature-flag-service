# Kafka Deliveries (Slot Mailbox) and Context Store Architecture

## 1. Why Do We Need Kafka `deliveries` (Slot Mailboxes)?

### The Problem It Solves: The N × M Point-to-Point Mesh
Suppose we have:
- **20 Evaluator nodes** (computing rules)
- **250 Connection Servers** (holding persistent client SSE streams)
- Clients connect to Connection Servers blindly via a Layer-4 Load Balancer.

```
[Evaluator 1] -----\  /----- [Connection Server 1 (Slot 1)]
[Evaluator 2] ------><------ [Connection Server 2 (Slot 2)]
[Evaluator 3] -----/  \----- [Connection Server 3 (Slot 3)]
...                           ...
(20 Evaluators)   5,000 Mesh  (250 Connection Servers)
```

If an admin flips a kill switch:
1. Evaluator 1 re-evaluates Shard 17.
2. It finds 80 clients need their flag toggled from `true` to `false`.
3. Those 80 clients are spread across Connection Servers: 40 on Slot 1, 25 on Slot 2, 15 on Slot 3.

#### Why NOT direct HTTP/gRPC from Evaluators to Connection Servers?
- **5,000 Fragile Sockets**: $20 \times 250 = 5,000$ persistent internal RPC channels.
- **Cascading Blockages**: If Connection Server 2 slows down or suffers a GC pause, Evaluator 1 hangs trying to push RPCs, delaying kill switches for users on other healthy connection servers.
- **Service Discovery Overhead**: Every Evaluator must track the dynamic IP addresses and health of all 250 connection servers.

---

### The Solution: Kafka `deliveries` Topic as "Slot Mailboxes"

```
[ Evaluator A ]
   │ Re-evaluates Shard 17:
   │ 40 diffs for Slot 1, 25 diffs for Slot 2
   │
   ├─► Kafka `deliveries` [ Partition 1 ] ──► [ Connection Server 1 ] ──► 40 Phones (SSE)
   │
   └─► Kafka `deliveries` [ Partition 2 ] ──► [ Connection Server 2 ] ──► 25 Phones (SSE)
```

1. **Partition Number = Slot ID**:
   - In `etcd`, each Connection Server claims an ephemeral Slot ID (e.g. Slot 1, 2, 3...).
   - The Kafka topic `deliveries` is created with dedicated partitions matching the slots.
   - Connection Server 1 **only consumes Partition 1**.
   - Connection Server 2 **only consumes Partition 2**.
2. **Evaluator Batching**:
   - Evaluator groups patches by `slot_id`.
   - Evaluator sends **ONE single batch Kafka record** into Partition 1 with all 40 client patches.
3. **Total Decoupling**:
   - Evaluators do not know or care about Connection Server IPs.
   - Kafka provides elastic buffering and backpressure. If a connection server is busy, Kafka queues the patches safely.

---

## 2. What Exactly is the Context Store?

The **Context Store** is a fast, durable key-value store (e.g., Redis, DynamoDB, or PostgreSQL unindexed kv table) that stores client session metadata.

### The Schema:
Keyed by `(shard_id, connection_id)`:
```json
{
  "shard_id": 17,
  "connection_id": "conn_c9f1",
  "user_id": "usr_42",
  "slot_id": 1,
  "attributes": {
    "country": "US",
    "app_version": "2.4.0",
    "beta_user": true
  }
}
```

### When is it written and deleted?
1. **On Connect**: When a phone opens an SSE stream, the Evaluator writes the context to the Context Store **before** replying with initial answers.
2. **On Disconnect**: When the phone closes the app or disconnects, the connection is deregistered and its record is purged.

---

### Why is it needed? The "Zero-Reconnect Evaluator Failover"

Consider what happens if **Evaluator 2 crashes at 2:00 AM**:

```
[Phones / Apps]
      │
      ▼ (Persistent TCP Sockets remain OPEN!)
[Connection Servers]
      │
      X  (Evaluator 2 died!)
      │
[Surviving Evaluator 1]
      │ Adopts Shard 17 via consistent hash ring...
      ▼
   Reads Shard 17 contexts from [Context Store] (takes ~2s)
   Rebuilds active user contexts in RAM
   Ready to evaluate flags!
```

1. **The Clients Did Not Disconnect**:
   - Clients are connected to **Connection Servers**, not Evaluator 2.
   - When Evaluator 2 dies, the phones' TCP streams are completely unaffected!
2. **Without Context Store (Catastrophe)**:
   - Surviving Evaluator 1 adopts Shard 17, but it has no idea who is connected, what their `user_id`s are, or what targeting attributes (`country`, `app_version`) they have.
   - Evaluator 1 would have to force all 2.5 million phones to disconnect and reconnect to resend their context.
   - Result: A **massive thundering herd reconnect stampede** that crushes the load balancer and gateway.
3. **With Context Store (Zero Reconnects)**:
   - Evaluator 1 simply queries: `SELECT * FROM context_store WHERE shard_id = 17`.
   - In ~2 seconds, Evaluator 1 reads the contexts and reconstructs Shard 17 in RAM.
   - When a kill switch is flipped 10 seconds later, Evaluator 1 evaluates those users and delivers the switch via the `deliveries` topic.
   - **Zero phones reconnected. Zero dropped streams.**
