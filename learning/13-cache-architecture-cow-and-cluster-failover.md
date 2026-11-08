# Cache Architecture, Copy-on-Write (CoW), and Cluster Failover

## 1. Why Every Evaluator Caches All Flags (Not Sharded Flags)

A common intuition is: *"If we shard evaluators, shouldn't each evaluator only load the flags for its shard?"*

In a distributed feature flag architecture:
- **What is sharded?** **Traffic and Active Connections** (users and their persistent SSE/WebSocket TCP sockets).
- **What is NOT sharded?** **Flag & Rule Definitions** (metadata).

### Why Flag Metadata is Fully Replicated:
1. **Flag Rules are Tiny**:
   - A single flag rule is pure text configuration (JSON/struct: key, status, percentage, rules, salt).
   - Even in an enterprise with **50,000 active flags**, 50,000 flags $\times \approx 1\text{ KB} = \mathbf{\approx 50\text{ MB}}$ of RAM.
   - 50 MB is negligible for any modern server (which typically has 4 GB to 64 GB RAM).

2. **Zero-RPC Fast Path (< 1µs SLA)**:
   - When a user (`user_123`) loads an app, the client evaluates dozens of flags (e.g. `dark_mode`, `new_checkout`, `v2_search`, `promo_banner`).
   - If Flag A lived on Evaluator 1 and Flag B lived on Evaluator 2, evaluating 20 flags would require **distributed RPC fan-outs across 20 nodes** over the network.
   - Evaluation latency would jump from **$< 1\mu\text{s}$ (RAM read)** to **$15-50\text{ms}$ (distributed network hops)**, destroying performance.
   - By replicating flag definitions across all evaluators (via Kafka CDC broadcast), any evaluator can evaluate **any flag for any user** locally in RAM with zero network I/O.

3. **What is Sharded**:
   - **Open TCP Sockets**: A single node can hold ~50,000 open connections before running into OS file descriptor or memory limits. Sharding distributes 500,000 connections across 10 nodes.
   - **Re-evaluation Compute**: When a flag changes, we only re-evaluate the connected users in the shards assigned to that node, spreading the CPU work evenly.

---

## 2. What is Copy-on-Write (CoW)?

In Go, when hundreds of concurrent HTTP requests and SSE streams are reading flag data simultaneously, updating that data safely is critical.

### The Problem: In-Place Mutation Causes Data Races
If goroutine A is reading `flag.State` or iterating over `flag.Rules` while goroutine B (the Kafka consumer) writes `flag.State = "off"`, the Go runtime detects a **data race** and panics or corrupts memory:
```go
// DANGEROUS: In-place mutation during live evaluation
existingFlag.State = "off"  // Goroutines reading this will crash or race!
```

### The Naive Solution: Global Mutex Locking
Taking a write lock on the entire cache for every read and write creates massive lock contention. Under 50,000 req/sec, CPUs spend their time waiting on mutex locks rather than evaluating flags.

### The Solution: Copy-on-Write (CoW)
Instead of modifying an existing struct in place:
1. Allocate a **new immutable struct copy** in private memory.
2. Populate it with the updated values.
3. Atomically swap the map pointer under a sub-microsecond lock:
   ```go
   // Allocate brand-new immutable struct
   s.flags[envID][payload.FlagKey] = &engine.ParsedFlag{
       Key:           payload.FlagKey,
       State:         constants.FlagStateOff,
       Version:       payload.FlagVersion,
       FallbackValue: payload.FallbackValue,
       Salt:          existing.Salt,
       Rules:         existing.Rules,
   }
   ```
- Goroutines currently evaluating continue reading the old struct reference safely (garbage collector reclaims it when done).
- New incoming evaluations immediately see the new struct pointer.
- **Result**: Zero data races, zero lock contention during evaluation.

---

## 3. When Does the Deterministic Evaluation Engine Run?

The **Deterministic Evaluation Engine** is a pure CPU, zero-I/O function. It runs in two distinct moments:

### Moment A: Synchronous API Request (`POST /api/v1/evaluate/{env_id}`)
When a backend microservice asks: *"Should user_123 see this flag?"*
- Runs inline in the HTTP handler.
- Completes in $< 1\mu\text{s}$.
- Returns the computed variation (`true`/`false`/string).

### Moment B: Push/Streaming Lifecycle (`GET /api/v1/sessions/stream`)
1. **At Initial Connection Handshake (`event: put`)**:
   - The moment a client opens an SSE stream, the engine evaluates **all flags** in that environment for `user_id`.
   - Sends the full initial state map down the stream so the client is immediately ready.
2. **On Kafka Change Event (`event: patch`)**:
   - When Kafka broadcasts an update (e.g. kill switch toggled or percentage increased from 20% to 50%):
   - The evaluator iterates over all active sessions in the shards it owns.
   - Runs the engine on the updated flag for each user.
   - If the new result differs from `session.LastAnswers[flagKey]`, it emits an `event: patch` with the diff.

---

## 4. What Happens When an Evaluator Node Dies? (Failover Lifecycle)

```
[Evaluator 1]              [Evaluator 2 (CRASHES)]             [Evaluator 3]
      |                               X                             |
      |                               |                             |
      |                        3s TTL expires                       |
      |                        in etcd registry                     |
      |<---------- etcd emits Watch event: DELETE node-2 ---------->|
      |                                                             |
Recalculates Hash Ring                                    Recalculates Hash Ring
Adopts Shard 1                                            Adopts Shard 2
(Data ALREADY in RAM)                                     (Data ALREADY in RAM)
      |                                                             |
      |<-- Disconnected clients reconnect to LB / surviving nodes --|
      |                                                             |
Immediate evaluation                                      Immediate evaluation
(0ms warm-up time)                                        (0ms warm-up time)
```

### Step-by-Step Failover Walkthrough:

1. **Failure Occurs**:
   - `evaluator-2` crashes or gets OOM-killed.
   - Its open SSE connections to clients drop immediately.

2. **Decentralized Lease Expiry**:
   - `evaluator-2` stops sending heartbeats to `etcd`.
   - Because its lease has a **3-second TTL**, etcd automatically removes `/members/evaluators/evaluator-2` after 3 seconds.

3. **Event-Driven Topology Update**:
   - etcd's streaming watcher notifies `evaluator-1` and `evaluator-3`.
   - Both nodes run `ring.SetMembers(["evaluator-1", "evaluator-3"])`.
   - The 100 virtual tokens for `evaluator-2` are purged from the ring.
   - The shards previously owned by `evaluator-2` are automatically reassigned to `evaluator-1` and `evaluator-3`.

4. **Why Having Full Data in RAM Makes Failover Instant**:
   - Because `evaluator-1` and `evaluator-3` **already had all flags and rules loaded in memory from boot and kept fresh via Kafka CDC**, there is **zero cache warming, zero DB querying, and zero delay**.

5. **Client Reconnection**:
   - The disconnected client SDKs automatically reconnect (standard SSE/WebSocket auto-reconnect with exponential backoff).
   - The load balancer routes them to `evaluator-1` or `evaluator-3`.
   - The surviving evaluator immediately runs the initial evaluation from RAM and streams the flags.
