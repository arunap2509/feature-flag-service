# Kafka Broadcast Ingestion vs. Sharded Execution

## The Core Question
> *"If evaluator 2 ran for 3 hours and then dies, won't evaluator 1 only have data from bootup time? Or do all evaluators update their cache when they receive input from Kafka, but only serve what they are supposed to by shard?"*

**The second statement is 100% correct.**

Every evaluator continuously updates its in-memory cache for **100% of the flags** from Kafka, but only does the heavy re-evaluation and SSE streaming for the **shards it currently owns**.

---

## 1. How Kafka Broadcast Works (`group.id` per node)

In standard Kafka queueing, consumers sharing the same `group.id` divide partitions among themselves (competing consumers).

In our feature flag architecture, we use **Broadcast Pub/Sub** by assigning each evaluator a **unique consumer group**:

```go
kgo.ConsumerGroup(fmt.Sprintf("evaluator-%s", cfg.NodeID))
```

- `evaluator-1` listens with group: `evaluator-evaluator-1`
- `evaluator-2` listens with group: `evaluator-evaluator-2`
- `evaluator-3` listens with group: `evaluator-evaluator-3`

When Kafka receives a message on `flag-changes`:
**Kafka replicates and delivers that message to ALL three consumer groups simultaneously.**

```
                           Kafka Topic: flag-changes
                                      |
         +----------------------------+----------------------------+
         |                                                         |
         v (delivered to group-1)    v (delivered to group-2)      v (delivered to group-3)
   [Evaluator 1]               [Evaluator 2]                 [Evaluator 3]
   Apply to RAM                Apply to RAM                  Apply to RAM
   (Has 100% Latest Rules)     (Has 100% Latest Rules)       (Has 100% Latest Rules)
```

Within $< 10\text{ms}$ of an admin updating a flag, **all 3 evaluators update their in-memory rules map**. Their RAM state is completely identical.

---

## 2. Where Sharding Comes In: Sharded Re-evaluation Workload

While **cache updates** happen on every node, **client re-evaluations** are strictly partitioned:

```go
// 1. ALL nodes update their RAM cache
rulesStore.ApplyEvent(payload)

// 2. ONLY re-evaluate clients in shards THIS node owns
ownedShards := hashRing.GetOwnedShards(cfg.NodeID, payload.EnvironmentID, cfg.ShardCount)
for shardID := range ownedShards {
    sessionRegistry.ReevaluateShard(...) // only compute diffs for my shards!
}
```

- **Evaluator 1** only burns CPU re-evaluating clients in **Shard 0**.
- **Evaluator 2** only burns CPU re-evaluating clients in **Shard 1**.
- **Evaluator 3** only burns CPU re-evaluating clients in **Shard 2**.

This prevents redundant CPU work across the cluster while ensuring every node's cache stays in perfect sync.

---

## 3. What Happens During Failover After 3 Hours?

If `evaluator-2` crashes after 3 hours:

1. `evaluator-1` has been running for those same 3 hours, receiving **every single Kafka update** in real-time.
2. Its RAM cache is already at the exact same version ($V_{\text{latest}}$) as `evaluator-2` was at the moment of failure.
3. When `evaluator-2`'s 3s etcd lease expires:
   - `evaluator-1` adopts Shard 1.
   - When Shard 1's clients reconnect to `evaluator-1`, `evaluator-1` **already has all flag rules at $V_{\text{latest}}$**.
   - **Zero stale data. Zero DB queries. 0ms warm-up latency.**

---

## 4. What If a Brand-New Node (e.g. Evaluator 4) Boots Up After 3 Hours?

If an auto-scaler or operator spins up a brand new node (`evaluator-4`):
1. **Bootup Hydration**: Evaluator 4 queries PostgreSQL, which has all committed changes from the past 3 hours.
2. **Kafka Catch-up**: It starts consuming Kafka from the end offset (`AtEnd()`).
3. **etcd Registration**: It registers `/members/evaluators/evaluator-4` with etcd.
4. The ring rebalances, and Evaluator 4 starts taking its share of shards immediately with 100% fresh data.
