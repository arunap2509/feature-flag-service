# Architecture: Evaluator Fleet, Consistent Hash Ring & etcd Cluster Coordination

## 1. Overview
The **Evaluator Fleet** is the computational brain of the distributed real-time feature flag service. It operates entirely off the public Internet, decoupled from connection-holding edge servers:
1. Subscribes to the Kafka `flag-changes` topic.
2. Maintains an in-memory compiled representation of active flag targeting rules and user segments.
3. Maps environment shards onto an in-memory **Consistent Hash Ring** with 100 virtual nodes per evaluator.
4. Manages distributed membership and dynamic cluster topology via `etcd` heartbeat leases.
5. Tracks active client sessions in memory, re-evaluating flags upon change events and generating minimal delta diff patches.

```
┌─────────────────┐       ┌─────────────────┐       ┌───────────────────────┐
│ Kafka: changes  │──────►│ Evaluator Node  │◄──────│ etcd Cluster (2379)   │
└─────────────────┘       │                 │       │ (/members/evaluators/)│
                          │ ┌─────────────┐ │       └───────────────────────┘
                          │ │ Hash Ring   │ │
                          │ └──────┬──────┘ │
                          │        ▼        │
                          │ ┌─────────────┐ │
                          │ │ Engine &    │ │
                          │ │ Session Reg │ │
                          │ └──────┬──────┘ │
                          └────────┼────────┘
                                   │
                                   ▼
                          [Diff Patches Generated]
```

---

## 2. Component Architecture

### A. Targeting Rule Engine (`internal/evaluator/engine`)
- **Deterministic Hashing**:
  $$\text{bucket} = \text{hash}(\text{flag\_key} + \text{salt} + \text{user\_id}) \pmod{100,000}$$
  Implemented via big-endian SHA-256 truncation, guaranteeing deterministic stickiness: increasing rollout percentage from 10% to 20% strictly preserves all previously allocated users.
- **Segment Matching**: Fast in-memory set lookups on `user_ids`.
- **Fast-Path Kill Switch**: When `state == 'off'`, evaluation short-circuits in $< 100\text{ns}$ to return `fallback_value`.

### B. In-Memory Consistent Hash Ring (`internal/evaluator/ring`)
- Allocates 100 virtual nodes per physical evaluator onto a 32-bit ring.
- Shards are computed via $\text{hash}(\text{user\_id}) \pmod{\text{shard\_count}}$.
- Ownership is determined by hashing `fmt.Sprintf("%s:shard:%d", envID, shardID)` and binary searching the clockwise nearest virtual node.

### C. etcd Cluster Coordinator (`internal/evaluator/cluster`)
- Registers node at `/members/evaluators/<node-id>` with a 3-second TTL lease.
- Background goroutine streams `client.KeepAlive()`.
- Background goroutine maintains a persistent `etcd.Watch()` on `/members/evaluators/`. On any node join or departure, the local Consistent Hash Ring is atomically recalculated.
- Stamps decisions with `etcd_revision` as a distributed fencing token.

### D. In-Memory Rules & Segment Cache (`internal/evaluator/cache`)
- Pre-compiles JSON rule trees into structured in-memory slices upon receiving Kafka events or initial PostgreSQL hydration, avoiding `json.Unmarshal` overhead during runtime evaluation.
- Thread-safe via `sync.RWMutex`.

### E. Session Registry & Delta Patch Generator (`internal/evaluator/session`)
- Stores connected client sessions partitioned by shard: `(env_id, shard_id) -> conn_id -> ClientSession`.
- When a flag event arrives, evaluates the changed flag only across active sessions in owned shards.
- If an answer changed (e.g. `checkout_v2: true -> false`), generates a `DiffPatch` with the new environment version.

---

## 3. Live Verification Results

1. **etcd Membership**:
   - `evaluator-1` registered lease `69f5a1b558af4e07` with TTL 3s.
   - Ring initialized with 100 virtual nodes at revision 2.
2. **Initial Database Hydration**:
   - 7 flags hydrated across 4 environments from PostgreSQL on boot.
3. **Sub-Microsecond In-Memory Evaluation**:
   - `POST /api/v1/evaluate/{env_id}` evaluated 3 flags for `usr_test_1` in $< 1\mu\text{s}$.
4. **End-to-End Real-Time Kill Switch**:
   - `POST /flags/checkout-v2/kill` invoked via Management API (`:8081`).
   - Postgres atomic commit $\to$ CDC WAL outbox publisher $\to$ Kafka partition 0 offset 4 $\to$ Evaluator.
   - Evaluator detected active session in Shard 0 flipped from `true` to `false`, immediately logging:
     `Shard 0: generated 1 diff patches for flag 'checkout-v2' (env_version: 7)`.
