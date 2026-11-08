# ⚡ Distributed Real-Time Feature Flag Service

A production-grade, distributed, real-time feature flag and configuration delivery engine built in **Go**, designed for ultra-low latency targeting, deterministic percentage rollouts, emergency kill-switches, and closed-loop SSE push propagation.

![Real-Time Feature Flag Service Architecture](docs/assets/architecture-diagram.png)

---

## 💡 Inspiration

This distributed system was designed and implemented based on the architectural deep-dive:
📺 **[Feature Flag Service System Design (YouTube)](https://youtu.be/GsRR__fz854?si=FHMwpxzHJds9GkGP)**

---

## 🏗️ Architecture Overview

The platform decouples the **Control Plane** (rule configuration & persistence) from the **Data Plane** (real-time edge streaming & evaluation) to guarantee high throughput, zero lost updates, and sub-50ms push delivery.

```
┌────────────────────────────────────────────────────────────────────────┐
│                          CONTROL PLANE PIPELINE                        │
│                                                                        │
│  Operator ──► Management API (:8081) ──► PostgreSQL 16 (Durable WAL)   │
│                 (OCC 412 Precondition)         │                       │
│                                                ▼                       │
│                                        Outbox CDC Tailer               │
│                                         (Logical pgoutput)             │
│                                                │                       │
│                                                ▼                       │
│                                      Control Redpanda (:9092)          │
│                                      Topic: 'flag-changes'             │
└────────────────────────────────────┬───────────────────────────────────┘
                                     │
                                     ▼ Broadcast
┌────────────────────────────────────────────────────────────────────────┐
│                        EVALUATOR FLEET & STATE                         │
│                                                                        │
│   etcd v3.5 Cluster ◄── leases ──► Evaluator Cluster (Consistent Ring) │
│                                            │                           │
│                                            ▼                           │
│                                   Redis Context Store                  │
│                               (shard:{env_id}:{shard_id})              │
│                                            │                           │
│                                            ▼ batch deliveries          │
│                                    Deliveries Redpanda                 │
│                                    Topic: 'deliveries' (P0 - P7)       │
└────────────────────────────────────┬───────────────────────────────────┘
                                     │
                                     ▼ consume slot mailbox
┌────────────────────────────────────────────────────────────────────────┐
│                       EDGE STREAMING & INGRESS                         │
│                                                                        │
│    Deliveries Bus ──► Edge Connection Servers (:8091, :8092...)        │
│                                    ▲                                   │
│                                    │ SSE Upstreams                     │
│                        Layer-7 Load Balancer (:8080)                   │
│                                    ▲                                   │
│               ┌────────────────────┴────────────────────┐              │
│               ▼                                         ▼              │
│      Mobile App Clients                        Web SPA Clients         │
│     (Pre-evaluated SSE)                     (Persistent Stream)        │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 🔑 Key Engineering Invariants

1. **Zero Dual-Write Race Conditions (Transactional Outbox & CDC)**:
   - Operators modify flag rules, rollouts, and targeting segments via the Management API using **Optimistic Concurrency Control (OCC `If-Match`)** returning HTTP `412 Precondition Failed` on version mismatch.
   - Outbox events are committed atomically inside the database transaction.
   - The CDC publisher (`outbox-pub`) tails PostgreSQL's Write-Ahead Log (WAL) directly using logical decoding (`pgoutput`). If the message broker is temporarily down, the PostgreSQL replication slot LSN is held, guaranteeing **zero dropped events**.
2. **Consistent Hash Ring with Virtual Nodes (`etcd`)**:
   - Evaluator pods register with 100 virtual nodes each in `etcd` (`/members/evaluators/*`).
   - Tenant environments are partitioned into deterministic shards (`shardID = Hash(userID) % shardCount`).
   - Evaluator failures trigger automated lease expiration in $< 3\text{s}$, seamlessly remapping keyspace to surviving nodes without service disruption.
3. **Cryptographically Salted Deterministic Rollouts**:
   - Percentage rollouts use **FNV-1a 64-bit hashing** combining `salt + flag_key + user_id`.
   - Modulo 100,000 provides $0.001\%$ precision, ensuring users maintain deterministic flag values across rollouts while preventing user-correlation bias across flags.
4. **Dual-Topic Kafka Isolation**:
   - **Control Topic (`flag-changes`)**: Low-frequency broadcast of rule modifications.
   - **Deliveries Topic (`deliveries`)**: High-throughput partitioned mailbox. Each Edge Connection Server consumes exclusively from its mapped partition: $\text{partition} = (\text{slot\_id} - 1) \pmod 8$.
5. **Ultra-Fast Server SDK SLA ($< 1\mu\text{s}$)**:
   - Backend services use the Server SDK to evaluate rules in-memory against local AST snapshots.
   - Benchmarked at **34 nanoseconds per evaluation** ($> 30\text{M}$ evals/sec per core).

---

## 🖥️ Fleet Mission Control UI

The service includes a real-time cluster observability and chaos engineering dashboard built directly into the codebase:

- **Live URL**: `http://localhost:9090`
- **Real-Time etcd Ring Topology**: Shows active virtual node members and upstream connection slots.
- **Dynamic Kubernetes HPA Gauges**: Tracks live CPU utilization against the $50\%$ autoscaling threshold.
- **Real-Time Pod Resource Consumption**: Shows actual CPU millicores (`m`) and memory (`MiB`) per pod.
- **One-Click Chaos Triggers**: Instantly kill pods (`evaluator-0`, `conn-server-0`, `load-balancer`) to visually observe automated failover and ring rebalancing.

---

## 🚀 Quickstart & Local Setup

### Prerequisites
- [Go 1.22+](https://go.dev/)
- [Docker & Docker Compose](https://www.docker.com/)
- [kind (Kubernetes in Docker)](https://kind.sigs.k8s.io/) *(optional, for HPA cluster)*
- [kubectl](https://kubernetes.io/docs/tasks/tools/)

### Option 1: Run with Docker Compose
```bash
# 1. Start all infrastructure and microservices
docker compose up -d

# 2. Run Database Migrations
migrate -path migrations -database "postgres://postgres:postgres@localhost:5440/featureflags?sslmode=disable" up

# 3. Seed High-Volume Tenants, Environments, Segments, and Flags
go run cmd/seed/main.go
```

### Option 2: Run with Kubernetes (`kind`) with Dynamic HPA
```bash
# 1. Create kind cluster with mapped ports (:18080 LB, :18081 Mgmt API)
kind create cluster --name feature-flag-cluster --config deploy/k8s/00-kind-cluster.yaml

# 2. Deploy infrastructure, services, and HPA
kubectl apply -f deploy/k8s/

# 3. Start the Fleet Mission Control Dashboard
go run cmd/fleet-watch-ui/main.go -port=9090
# Open http://localhost:9090 in your browser
```

---

## 🧪 Simulation, Verification & Load Testing

### 1. Run Automated 5-Point End-to-End Verification
```bash
go run cmd/verify-e2e/main.go -gateway=http://localhost:18080 -mgmt=http://localhost:18081
```
Verifies all 5 core invariants:
- **Test 1**: Closed-Loop SSE Propagation & Content Integrity ($< 35\text{ms}$).
- **Test 2**: Emergency Kill-Switch Fast Path SLA ($< 30\text{ms}$).
- **Test 3**: Segment Targeting Isolation (zero leaks to non-targeted users).
- **Test 4**: Optimistic Concurrency Control (OCC 412 race collision handling).
- **Test 5**: Server SDK In-Memory AST Benchmark ($< 1\mu\text{s}$ SLA).

### 2. High-Throughput Traffic Generators
```bash
# Operator Simulator: 40 writes/sec across 200 flags (rollouts, toggles, kills)
go run cmd/sim-operator/main.go -target=http://localhost:18081 -rate=40 -concurrency=8

# Client Fleet Simulator: 200 persistent SSE connections + 1.5M Server SDK evals/sec
go run cmd/sim-clients/main.go -gateway=http://localhost:18080 -mgmt=http://localhost:18081 -mobile=200 -server=10
```

---

## 📄 License
MIT License.
