# Evaluator Internals & etcd Cluster Coordination Guide

## 1. What is the Evaluator and What Happens Inside It?

The **Evaluator** is the high-throughput, low-latency computational engine of the distributed feature flag service. Its primary purpose is to **evaluate flag targeting rules in $< 1\mu\text{s}$ in memory** without querying PostgreSQL during the request path, and **push real-time state changes** down streaming connections (SSE / WebSockets).

```
                      +---------------------------------------+
                      |           Evaluator Service           |
                      |                                       |
PostgreSQL (Boot) --->| 1. Startup Hydration                  |
                      |    - Flags, Rules, Fallbacks, Salts   |
                      |    - In-Memory Cache (RAM)            |
                      |                                       |
Kafka (flag-changes) >| 2. Broadcast CDC Listener             |
                      |    - Cache Invalidation / CoW Update  |
                      |                                       |
Client HTTP Request ->| 3. Rule Evaluation Engine (< 1µs)     |
                      |    - Fast-path Status check           |
                      |    - Segment Set Lookup (O(1))        |
                      |    - SHA-256 Percentage Rollout       |
                      |                                       |
                      | 4. Shard Ownership & Hash Ring        |
                      |    - 100 Virtual Nodes per Member     |
                      |                                       |
Client SSE Stream <---| 5. Session Registry & Push Diffs      |
                      |    - ReevaluateShard on Event         |
                      |    - Emit 'event: patch' delta        |
                      +---------------------------------------+
```

### The 5 Internal Stages of the Evaluator

1. **Startup Hydration (Warm Cache Boot)**:
   - On startup, the evaluator reads all active flags, rules, fallback values, rollout salts, and segment target user IDs from PostgreSQL into an in-memory thread-safe cache (`cache.RulesCache`).
   - From this point on, **PostgreSQL is never queried** during flag evaluations.

2. **Broadcast Event Listener (Kafka CDC Consumer)**:
   - Evaluator subscribes to the `flag-changes` Kafka topic using a **unique broadcast consumer group** (`evaluator-<node_id>`).
   - Every evaluator instance receives every flag update, segment addition, or kill switch trigger within $< 10\text{ms}$ of it being committed to Postgres by the Management API.
   - Updates use **Copy-on-Write (CoW)** immutable allocations so active evaluations never experience race conditions or lock contention.

3. **In-Memory Rule Evaluation Engine**:
   - When `/api/v1/evaluate` is called or an active session is re-evaluated:
     1. **Kill Switch / Status**: If flag `status == "off"`, immediately return the off variation.
     2. **Segment Targeting**: Checks if the `user_id` belongs to targeted segments ($O(1)$ set lookup).
     3. **Context Attribute Rules**: Matches user context (e.g. `country == "US"`, `app_version >= "2.0"`).
     4. **Deterministic Percentage Rollouts**: Computes `SHA256(flag_key + salt + user_id) % 100,000`. If hash bucket $< \text{percentage} \times 1,000$, flag evaluates to true.
     5. **Fallback**: If no rule matches, returns default baseline.

4. **Consistent Hash Ring & Shard Ownership**:
   - The evaluator participates in a consistent hash ring with virtual nodes (100 tokens per node).
   - In distributed deployments, user sessions are mapped to shards (`Hash(tenant, env, user) % ShardCount`). The node only accepts and manages SSE connections for shards that fall in its ring sector.

5. **Session Registry & Push Diffs (Real-Time SSE)**:
   - When a client opens `/api/v1/stream`, the evaluator registers the connection in `session.Registry`.
   - When a flag or segment event arrives via Kafka, the evaluator triggers `ReevaluateShard`.
   - It re-evaluates the flags in memory for all active sessions in that shard. If a flag state flipped ($false \to true$, or $true \to false$), it immediately writes a diff JSON (`event: patch`) directly down the open HTTP connection.

---

## 2. What is etcd, What Does It Store, and How Does It Help?

### What is etcd?
`etcd` is an open-source, strongly consistent, distributed key-value store built on the **Raft consensus algorithm**. It is the same distributed coordination engine used by Kubernetes to manage cluster state.

### What are we storing in etcd?

| Key | Value | Metadata |
|---|---|---|
| `/members/evaluators/evaluator-1` | `evaluator-1` | Bound to active 3s TTL Lease |
| `/members/evaluators/evaluator-2` | `evaluator-2` | Bound to active 3s TTL Lease |
| `/members/evaluators/evaluator-3` | `evaluator-3` | Bound to active 3s TTL Lease |

Each evaluator node registers a single key under the prefix `/members/evaluators/` attached to an **ephemeral Lease with a 3-second Time-To-Live (TTL)**.

### How does etcd help our architecture?

1. **Decentralized Service Discovery & Health Checks**:
   - There is no central master node. Evaluator nodes periodically send keepalive heartbeats to etcd (`c.client.KeepAlive`).
   - If an evaluator process crashes, gets killed, or gets network-partitioned, it stops heartbeating. After **3 seconds**, etcd automatically expires its lease and deletes `/members/evaluators/<node-id>`.

2. **Event-Driven Cluster Topology Watches**:
   - Every surviving evaluator maintains a real-time streaming watch on `/members/evaluators/` (`c.client.Watch`).
   - The moment a node joins, leaves, or dies, etcd immediately pushes a watch event to all nodes.
   - Evaluators instantly update their consistent hash ring (`ring.SetMembers(nodeIDs)`). Shards are automatically reassigned across the surviving nodes in $< 3\text{s}$ with zero operator intervention.

3. **Linearizable Consistency & Split-Brain Prevention**:
   - Unlike Redis, which uses asynchronous replication and can lose data or suffer split-brain during network partitions, etcd requires a Raft quorum ($N/2 + 1$). All cluster members see an identical, linearizable view of active ring membership.

---

## 3. How to Inspect etcd Data via CLI

Since `etcd` is running in Docker as `ff-etcd`, you can run `etcdctl` directly using `docker exec`.

### 1. View all evaluator keys and values
```bash
docker exec ff-etcd etcdctl get "" --prefix
```
**Output:**
```
/members/evaluators/evaluator-1
evaluator-1
/members/evaluators/evaluator-2
evaluator-2
/members/evaluators/evaluator-3
evaluator-3
```

---

### 2. View keys with metadata (Version, Lease ID, Revision)
```bash
docker exec ff-etcd etcdctl get /members/evaluators/ --prefix -w fields
```
**Output:**
```
"ClusterID" : 17810356679686510411
"MemberID" : 17456590584391395829
"Revision" : 19
"RaftTerm" : 2
"Key" : "/members/evaluators/evaluator-1"
"CreateRevision" : 6
"ModRevision" : 16
"Version" : 4
"Value" : "evaluator-1"
"Lease" : 7635186543525582502
...
```

---

### 3. Check active lease TTL and attached keys
etcd assigns a 64-bit lease ID to each node. You can inspect its remaining TTL and attached keys using `lease timetolive <hex-lease-id> --keys`:

```bash
# Get the hex lease ID of evaluator-1 and inspect its TTL
LEASE_HEX=$(docker exec ff-etcd etcdctl get /members/evaluators/evaluator-1 -w json | python3 -c "import sys, json; print(hex(json.load(sys.stdin)['kvs'][0]['lease']))")
docker exec ff-etcd etcdctl lease timetolive $LEASE_HEX --keys
```
**Output:**
```
lease 69f5a1b558af56a6 granted with TTL(3s), remaining(2s), attached keys([/members/evaluators/evaluator-1])
```
*(Notice how the remaining TTL counts down from 3s and refreshes continuously as keepalive pings arrive).*

---

### 4. Check etcd cluster health and raft leadership
```bash
docker exec ff-etcd etcdctl endpoint status -w table
docker exec ff-etcd etcdctl endpoint health
```
**Output:**
```
+----------------+------------------+---------+---------+-----------+------------+-----------+------------+--------------------+--------+
|    ENDPOINT    |        ID        | VERSION | DB SIZE | IS LEADER | IS LEARNER | RAFT TERM | RAFT INDEX | RAFT APPLIED INDEX | ERRORS |
+----------------+------------------+---------+---------+-----------+------------+-----------+------------+--------------------+--------+
| 127.0.0.1:2379 | f24244a5c413e9f5 |  3.5.11 |   29 kB |      true |      false |         2 |       3904 |               3904 |        |
+----------------+------------------+---------+---------+-----------+------------+-----------+------------+--------------------+--------+
127.0.0.1:2379 is healthy: successfully committed proposal: took = 1.054375ms
```

---

### 5. Watch live membership changes (Real-Time Stream)
Open a terminal and run:
```bash
docker exec -it ff-etcd etcdctl watch /members/evaluators/ --prefix
```
Now in another terminal, restart an evaluator:
```bash
docker stop ff-evaluator-3
```
Within 3 seconds, etcd emits a `DELETE` event on your watch terminal:
```
DELETE
/members/evaluators/evaluator-3
```
When you start it back up:
```bash
docker start ff-evaluator-3
```
etcd immediately emits a `PUT` event:
```
PUT
/members/evaluators/evaluator-3
evaluator-3
```
