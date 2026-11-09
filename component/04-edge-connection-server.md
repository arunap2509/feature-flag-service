# Component Deep Dive: Edge Connection Server

## 1. Executive Summary & Architectural Role

The **Edge Connection Server** ([`cmd/conn-server/main.go`](file:///Users/arunap/Project/Lab/feature-flag-service/cmd/conn-server/main.go)) forms the internet-facing streaming tier. Its sole responsibility is to maintain long-lived **Server-Sent Events (SSE)** connections to hundreds of thousands of client devices (mobile apps, single-page apps, microservices).

It is intentionally designed to be **stateless with respect to business rules**:
- **Zero Rules in Memory**: The connection server holds **no** flag definitions, segment sets, or rollout percentages.
- **Zero Evaluation Logic**: It does not compute whether a user qualifies for a flag.
- **Sized by Failure Blast Radius**: Sized to hold ~200,000 idle TCP connections per machine without running out of file descriptors or kernel buffer memory.

```mermaid
flowchart TD
    subgraph Edge Clients
        C1[Client Device 1] -->|GET /stream| Conn[Connection Server Slot 1]
        C2[Client Device 2] -->|GET /stream| Conn
    end

    subgraph Internal RPC & Hash Ring
        Conn -->|Resolve Shard Owner| Ring[Hash Ring via etcd]
        Conn -->|POST /sessions/register| Eval[Evaluator Cluster]
        Eval -->>|Return initial answers| Conn
        Conn -->>|event: put| C1
    end

    subgraph Slot Mailbox Kafka Consumer
        Eval -->|Produce to Deliveries| KDeliv["Kafka: deliveries (Partition 0)"]
        KDeliv -->|Consume Partition 0| DelivCons[Deliveries Consumer]
        DelivCons -->|Lookup connection_id| Reg[Local Connection Registry]
        Reg -->|event: patch| C1
    end
```

---

## 2. Why Decouple Edge Connection Servers from Evaluators?

1. **Deploying Rule Changes Must Not Terminate Client Streams**:
   Engineers frequently deploy updates to flag evaluation rules, SDK parsers, or context operators. If the streaming sockets were held by the evaluators, every rolling deployment would sever millions of client connections simultaneously, creating massive reconnect storms (thundering herds).
2. **Blast Radius Containment**:
   If a connection server node crashes due to an out-of-memory or network interface error, only the connections on that specific machine drop. The business state and user contexts remain intact in the Evaluator fleet and Redis.
3. **Resource Profile Specialization**:
   - **Connection Server**: High network file descriptors, low CPU, low memory (raw socket I/O).
   - **Evaluator**: High CPU (cryptographic hashing, rule iteration), high RAM (in-memory user contexts, segment sets).

---

## 3. Code Structure & Key Packages

```
internal/connserver/
├── cluster/      # etcd registration (/members/conn-servers/<slot_id>) and Evaluator ring tracking
├── server/       # HTTP SSE endpoint (/api/v1/stream) and registration proxy
├── registry/     # Thread-safe in-memory map of active local connection channels
└── deliveries/   # Partition-specific Kafka consumer reading diff patch batches
```

- **Entry Point**: [`cmd/conn-server/main.go`](file:///Users/arunap/Project/Lab/feature-flag-service/cmd/conn-server/main.go)
- **SSE Stream Handler**: [`internal/connserver/server/server.go`](file:///Users/arunap/Project/Lab/feature-flag-service/internal/connserver/server/server.go#L71)
- **Cluster Coordinator**: [`internal/connserver/cluster/coordinator.go`](file:///Users/arunap/Project/Lab/feature-flag-service/internal/connserver/cluster/coordinator.go)
- **Deliveries Consumer**: [`internal/connserver/deliveries/consumer.go`](file:///Users/arunap/Project/Lab/feature-flag-service/internal/connserver/deliveries/consumer.go)

---

## 4. Operational Lifecycle & Mechanics

### A. Slot ID & Mailbox Partitioning
Each connection server instance is assigned a unique `SLOT_ID` (e.g. `1`, `2`, `3`):
1. **etcd Registration**: Registers `/members/conn-servers/<slot_id>` with a 3-second heartbeat lease.
2. **Kafka Partition Assignment**: The instance consumes exclusively from the `deliveries` topic at `partition = (slot_id - 1) % DefaultDeliveriesPartitions` ([`deliveries/consumer.go:34`](file:///Users/arunap/Project/Lab/feature-flag-service/internal/connserver/deliveries/consumer.go#L34)).
3. **Direct Delivery**: Evaluators partition their patch batches directly to this partition, turning Kafka into a partitioned mailbox broker.

### B. Handling Client Connect (`GET /api/v1/stream`)
When a client connects ([`server.handleStream`](file:///Users/arunap/Project/Lab/feature-flag-service/internal/connserver/server/server.go#L71)):
1. **Response Controller Setup**: Disables write timeouts for indefinite streaming:
   ```go
   rc := http.NewResponseController(w)
   _ = rc.SetWriteDeadline(time.Time{})
   ```
2. **Shard Resolution**: Computes `shardID = engine.ComputeShard(userID, shardCount)`.
3. **Evaluator Lookup**: Asks `coordinator.GetEvaluatorForShard(envID, shardID)` to find which Evaluator instance currently owns this shard via the local consistent hash ring.
4. **Registration RPC**: Calls `POST http://<evaluatorAddr>/api/v1/sessions/register` with the client's context, receiving initial flag evaluations.
5. **Initial Snapshot (`event: put`)**: Immediately flushes full answers to the client:
   ```http
   event: put
   data: {"connection_id":"conn_123","environment_version":42,"flags":{"dark_mode":true}}
   ```
6. **Local Registry**: Saves a Go channel `chan session.DiffPatch` in [`registry.Registry`](file:///Users/arunap/Project/Lab/feature-flag-service/internal/connserver/registry/registry.go).

### C. Consuming & Flushing Delta Diffs (`event: patch`)
When the Kafka deliveries consumer receives a [`DeliveryBatch`](file:///Users/arunap/Project/Lab/feature-flag-service/internal/connserver/deliveries/consumer.go#L17):
1. It unmarshals the batch of patches for that slot.
2. For each patch, it looks up `patch.ConnectionID` in the local connection registry.
3. If the connection exists on this server, it pushes the patch to the connection's writer channel.
4. The HTTP connection loop serializes and flushes:
   ```http
   event: patch
   data: {"type":"patch","connection_id":"conn_123","environment_version":43,"changes":{"dark_mode":false}}
   ```

### D. Keepalive Heartbeats
Mobile cellular networks and stateful firewalls drop idle TCP sockets after 30–60 seconds of inactivity. The connection server maintains a 15-second heartbeat ticker:
```go
ticker := time.NewTicker(15 * time.Second)
for {
    select {
    case <-ticker.C:
        fmt.Fprintf(w, ": keepalive\n\n")
        flusher.Flush()
    ...
```

---

## 5. Failure Modes & Graceful Disconnect

1. **Client Disconnect**:
   When the client terminates the socket, `r.Context().Done()` fires. The connection server removes the connection from its local registry and notifies the Evaluator via `POST /api/v1/sessions/deregister`.
2. **Evaluator Re-assignment During Stream**:
   If an evaluator dies, the connection server does **not** close client connections. The surviving evaluator adopts the shard from Redis. When the next flag change occurs, the new evaluator sends updates to the same slot partition.
