# Infrastructure Deep Dive: etcd Distributed Coordination

## 1. Executive Summary & Architectural Role

**etcd** is the distributed consensus backbone of the system. Built on the **Raft consensus algorithm**, etcd provides a strongly consistent, linearizable hierarchical key-value store used for:
1. **Dynamic Service Discovery**: Tracking live Evaluators and Connection Servers.
2. **Ephemeral Lease Heartbeating**: Detecting crashed instances in $< 3\text{ seconds}$ without health-check polling loops.
3. **Event-Driven Topology Watches**: Instantly notifying surviving fleet members when topology changes, triggering automatic shard rebalancing.

```mermaid
flowchart TD
    subgraph etcd Raft Cluster (ff-etcd:2379)
        LeaseEngine["3-Second TTL Lease Manager"]
        KeyStore["Key Store (/members/...)"]
        WatchHub["Event Watch Hub (gRPC Streams)"]
    end

    subgraph Evaluator Fleet
        Eval1["Evaluator 1"] -->|KeepAlive Heartbeat| LeaseEngine
        Eval2["Evaluator 2"] -->|KeepAlive Heartbeat| LeaseEngine
        WatchHub -->|Push Membership Changes| Eval1
        WatchHub -->|Push Membership Changes| Eval2
    end

    subgraph Edge Tier
        Conn1["Conn-Server Slot 1"] -->|Register Slot Lease| KeyStore
        LB["Load Balancer"] -->|Watch /members/conn-servers/| WatchHub
    end
```

---

## 2. Why etcd Over Redis or ZooKeeper?

| Requirement | etcd (Raft) | Redis Sentinel / Cluster | ZooKeeper (Zab) |
| :--- | :--- | :--- | :--- |
| **Consistency Model** | **Linearizable (Strict Raft Quorum)** | Eventual (Async replica drops on failover) | Sequential Consistency |
| **Split-Brain Immunity** | Complete; requires $(N/2 + 1)$ majority | Vulnerable during master-failover network splits | Complete |
| **Streaming Watch Mechanism** | Multiplexed gRPC event streams | Redis Pub/Sub (Fire-and-forget; lossy) | Watcher one-shot triggers (must re-arm) |
| **Language & Ecosystem** | Native Go client library (`go.etcd.io/etcd`) | Redis Go drivers | Heavy JVM/JNI footprint |

---

## 3. Keyspace Schema & Semantics

All cluster coordination keys live under structured prefixes:

| Key Format | Sample Key | Value | Lease Attached | Description |
| :--- | :--- | :--- | :--- | :--- |
| `/members/evaluators/<node-id>` | `/members/evaluators/evaluator-9001` | `http://evaluator-1:9001` | **3-Second Ephemeral Lease** | Advertises physical evaluator network address and ring participation. |
| `/members/conn-servers/<slot-id>` | `/members/conn-servers/1` | `http://conn-server-1:8091` | **3-Second Ephemeral Lease** | Claims edge slot ownership and advertises load balancer upstream address. |

---

## 4. In-Depth Coordination Mechanisms

### A. The 3-Second Ephemeral Lease Contract ([`internal/evaluator/cluster/cluster.go`](file:///Users/arunap/Project/Lab/feature-flag-service/internal/evaluator/cluster/cluster.go))
Every service registers its identity attached to an ephemeral lease:

```go
// 1. Grant 3s lease
grantResp, err := c.client.Grant(ctx, 3)

// 2. Put key with lease
nodeKey := fmt.Sprintf("%s%s", constants.EtcdPrefixEvaluators, c.nodeID)
_, err = c.client.Put(ctx, nodeKey, c.nodeAddr, clientv3.WithLease(grantResp.ID))
```

### B. Resilient Background Keepalive Loop
The node launches a dedicated keepalive goroutine ([`cluster.go:108`](file:///Users/arunap/Project/Lab/feature-flag-service/internal/evaluator/cluster/cluster.go#L108)):
```go
keepAliveCh, err := c.client.KeepAlive(ctx, leaseID)
for {
    select {
    case _, ok := <-keepAliveCh:
        if !ok {
            // Keepalive dropped (e.g. temporary network blip)
            // Re-acquire lease and re-register immediately
            c.registerOnce(ctx)
        }
    }
}
```

### C. Event-Driven Watch Loops
Surviving nodes do not poll etcd. Instead, they maintain persistent gRPC streaming watchers ([`cluster.go:209`](file:///Users/arunap/Project/Lab/feature-flag-service/internal/evaluator/cluster/cluster.go#L209)):

```go
rch := c.client.Watch(ctx, constants.EtcdPrefixEvaluators, clientv3.WithPrefix())
for wresp := range rch {
    // Fired immediately when any evaluator joins, leaves, or expires!
    c.refreshMembers(ctx)
}
```
When `refreshMembers` runs:
1. It reads the full list of active node IDs.
2. It updates the in-memory [`ring.Ring`](file:///Users/arunap/Project/Lab/feature-flag-service/internal/evaluator/ring/ring.go#L41) with virtual nodes.
3. It triggers `onRingChange(activeNodes)`, initiating automatic shard failover.

---

## 5. Inspection & Troubleshooting via `etcdctl`

Since etcd runs in Docker (`ff-etcd`), you can inspect cluster state directly from terminal:

```bash
# 1. List all active cluster registrations
docker exec ff-etcd etcdctl get "" --prefix

# 2. View keys with lease IDs and revision numbers
docker exec ff-etcd etcdctl get /members/evaluators/ --prefix -w fields

# 3. Check remaining TTL on an active lease
LEASE_HEX=$(docker exec ff-etcd etcdctl get /members/evaluators/evaluator-1 -w json | python3 -c "import sys, json; print(hex(json.load(sys.stdin)['kvs'][0]['lease']))")
docker exec ff-etcd etcdctl lease timetolive $LEASE_HEX --keys

# 4. Check cluster health and endpoint status
docker exec ff-etcd etcdctl endpoint health
docker exec ff-etcd etcdctl endpoint status -w table
```
