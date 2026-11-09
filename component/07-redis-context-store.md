# Infrastructure Deep Dive: Redis Context Store

## 1. Executive Summary & Architectural Role

The **Context Store** ([`internal/evaluator/contextstore/store.go`](file:///Users/arunap/Project/Lab/feature-flag-service/internal/evaluator/contextstore/store.go)) is a high-speed, durable key-value store backed by Redis.

Its sole purpose is to solve the **Zero-Reconnect Failover Problem**:
> *If an Evaluator node crashes, how can surviving Evaluators continue calculating real-time flag updates for millions of active client connections without forcing those clients to disconnect and reconnect?*

By maintaining a durable, fast-delete index of active user contexts in Redis, surviving evaluators can adopt orphaned shards, recover user attributes, and resume streaming flag diffs without dropping a single edge TCP socket.

```mermaid
flowchart TD
    subgraph Edge Connection
        Client([Mobile App]) -->|Persistent SSE| Conn[Edge Connection Server]
        Conn -->|POST /sessions/register| Eval1[Evaluator 1]
    end

    subgraph Redis Context Store (shard:prod:17)
        Eval1 -->|1. HSET conn_42 ClientSession JSON| Redis[(Redis Hash)]
        Eval1 -->>|2. Success| Conn
    end

    subgraph Crash & Zero-Reconnect Failover
        Eval1 -.->|Crash / Die| Dead([Dead])
        etcd[(etcd)] -->|Watch: Member Deleted| Eval2[Evaluator 2]
        Eval2 -->|3. Adopt Shard 17: HGETALL shard:prod:17| Redis
        Redis -->>|4. Return all ClientSession objects| Eval2
        Eval2 -->|5. Resume ReevaluateShard| Eval2
        Eval2 -->|6. Emit DiffPatches to Kafka Deliveries| Conn
    end
```

---

## 2. The Disaster of Dropping Edge Connections (Reconnect Storms)

In naive architectures, client session state is held only in the evaluator's local memory.

If an evaluator node holding 250,000 active sessions crashes:
1. The edge connection servers discover that the evaluator is unreachable.
2. The connection servers are forced to close all 250,000 client SSE streams.
3. 250,000 mobile apps and web browsers immediately execute reconnect loops.
4. The load balancer and remaining evaluators are hit with a **250,000 request/sec reconnect storm (thundering herd)**.
5. Databases lock up, CPU spikes to 100%, and an outage cascades across the entire platform.

**With the Redis Context Store**:
Edge connection servers **never drop their client streams**. The surviving evaluator transparently adopts the orphaned sessions from Redis in $< 500\text{ms}$.

---

## 3. Data Model & Schema Design

The store organizes sessions into Redis Hashes partitioned by **environment** and **shard**:

### Key Structure
```
shard:{env_id}:{shard_id}
```
*Example*: `shard:prod:17`

### Hash Fields and Values
| Hash Field (`KEY`) | Hash Value (`VALUE`) |
| :--- | :--- |
| `connection_id` (e.g. `conn_98234a`) | JSON-serialized [`ClientSession`](file:///Users/arunap/Project/Lab/feature-flag-service/internal/evaluator/session/session.go#L10) payload |

### Serialized Payload Example:
```json
{
  "connection_id": "conn_98234a",
  "user_id": "usr_42",
  "shard_id": 17,
  "env_id": "prod",
  "server_slot": 1,
  "attributes": {
    "country": "US",
    "tier": "enterprise",
    "app_version": "3.4.1"
  },
  "last_answers": {
    "dark_mode": true,
    "checkout_v2": false
  }
}
```

---

## 4. Code Walkthrough ([`internal/evaluator/contextstore/store.go`](file:///Users/arunap/Project/Lab/feature-flag-service/internal/evaluator/contextstore/store.go))

### A. The Strict Save-Before-Respond Contract
When a connection server registers a new client, the evaluator **must** write to Redis before replying:
```go
func (s *Store) SaveSession(ctx context.Context, sess *session.ClientSession) error {
    key := ShardKey(sess.EnvID, sess.ShardID) // "shard:prod:17"
    data, err := json.Marshal(sess)
    if err != nil {
        return fmt.Errorf("failed to marshal session context: %w", err)
    }

    return s.client.HSet(ctx, key, sess.ConnectionID, data).Err()
}
```

### B. Fast Delete on Disconnect
When a client closes the SSE stream, the connection server notifies the evaluator, which immediately purges the hash field:
```go
func (s *Store) DeleteSession(ctx context.Context, envID string, shardID int, connID string) error {
    key := ShardKey(envID, shardID)
    return s.client.HDel(ctx, key, connID).Err()
}
```
Because `HDEL` operates on an existing hash key in $O(1)$, disconnect cleanup is instantaneous and does not leave orphaned records.

### C. Bulk Shard Recovery During Failover
When an evaluator's [`onRingChange`](file:///Users/arunap/Project/Lab/feature-flag-service/cmd/evaluator/main.go#L148) callback detects that it has adopted Shard 17:
```go
func (s *Store) GetShardSessions(ctx context.Context, envID string, shardID int) ([]*session.ClientSession, error) {
    key := ShardKey(envID, shardID)
    fields, err := s.client.HGetAll(ctx, key).Result()
    if err != nil {
        return nil, err
    }

    sessions := make([]*session.ClientSession, 0, len(fields))
    for _, val := range fields {
        var sess session.ClientSession
        if err := json.Unmarshal([]byte(val), &sess); err == nil {
            sessions = append(sessions, &sess)
        }
    }
    return sessions, nil
}
```
The adopting node iterates the returned slice, re-runs [`engine.EvaluateAll`](file:///Users/arunap/Project/Lab/feature-flag-service/internal/evaluator/engine/engine.go#L140) to refresh `LastAnswers`, and registers them in its local memory registry.

---

## 5. Performance & Operational Considerations

1. **Memory Sizing**:
   - Each active session JSON takes ~250–500 bytes.
   - 200,000 active sessions in Redis occupy only $\approx 100\text{ MB}$ of RAM.
2. **Pipelining & Bulk Deletes**:
   - If an entire environment is deleted, Redis can execute `DEL shard:env_id:*` in one command.
3. **Redis Persistence Settings**:
   - Redis RDB snapshots / AOF can be configured with loose fsync (`appendfsync everysec`). Since Redis acts as a high-speed failover buffer for transient sessions, sub-second durability is sufficient.
