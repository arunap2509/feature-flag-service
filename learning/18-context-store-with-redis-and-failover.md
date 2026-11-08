# Context Store: Redis Architecture & Zero-Reconnect Evaluator Failover

## 1. Why Redis is Ideal for the Context Store

The **Context Store** is a specialized distributed state store designed for one critical mission: **Zero-Reconnect Failover**.

When evaluating options (Redis vs. DynamoDB vs. PostgreSQL):

| Dimension | Redis | PostgreSQL / Relational DB | DynamoDB / Cassandra |
| :--- | :--- | :--- | :--- |
| **Write Latency on Connect** | **< 1ms** (In-Memory) | 5–15ms (Disk WAL & index overhead) | 5–10ms (Network round-trip) |
| **Shard Fetch on Crash** | **O(N) in RAM** (`HGETALL` in ~15ms for 50k rows) | Table scan or B-tree index lookup (100–300ms) | Batch query scan |
| **High Deletion Volume** | **Zero fragmentation penalty** (`HDEL`) | Table bloat & VACUUM pressure | TTL tombstone cleanup |
| **Data Footprint** | ~50 MB for 50,000 active sessions | ~100 MB | Cloud billed per WCU |

Redis is the industry-standard choice because saving context is directly in the **client connection handshake path**, and fast mass-retrieval is required during an emergency failover.

---

## 2. Redis Data Structures & Key Schema

Instead of flat keys (`shard:1:conn:123`), we use **Redis Hashes**:

```
Key:   shard:{env_id}:{shard_id}
Field: {connection_id}
Value: JSON session metadata:
       {
         "connection_id": "conn_c9f1",
         "user_id": "usr_42",
         "shard_id": 1,
         "env_id": "env_prod",
         "server_slot": 1,
         "attributes": {"country": "US", "tier": "premium"}
       }
```

### Key Operations:
1. **Client Connects (`POST /api/v1/sessions/register`)**:
   ```redis
   HSET shard:env_prod:1 conn_c9f1 '{"user_id":"usr_42", ...}'
   ```
   *Written before the HTTP response is returned to the Connection Server.*

2. **Client Disconnects (`POST /api/v1/sessions/deregister`)**:
   ```redis
   HDEL shard:env_prod:1 conn_c9f1
   ```
   *Sub-millisecond single field deletion.*

3. **Node Crash Recovery (`Evaluator 2 dies`)**:
   Surviving Evaluator 1 adopts Shard 1:
   ```redis
   HGETALL shard:env_prod:1
   ```
   *Returns all active connections in Shard 1 in a single round-trip.*

---

## 3. The Failover Sequence

```
[Evaluator 2 (Crashes)]           [etcd]           [Surviving Evaluator 1]           [Redis Context Store]
         X                          │                         │                                │
         │ (3s TTL lease expires)   │                         │                                │
         └─────────────────────────►│ Watch Event: DELETE     │                                │
                                    └────────────────────────►│ Recompute Ring                 │
                                                              │ Adopts Shard 1                 │
                                                              │ HGETALL shard:env_prod:1       │
                                                              ├───────────────────────────────►│
                                                              │◄───────────────────────────────┤
                                                              │ Rebuilds sessions in RAM       │
                                                              │ (Zero client reconnects!)      │
```
