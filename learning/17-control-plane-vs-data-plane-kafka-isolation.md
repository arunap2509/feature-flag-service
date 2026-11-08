# Control Plane vs. Data Plane Kafka Isolation

## The Core Question
> *"Shouldn't we be using a different Redpanda instance, as we're going to test this with scaled data?"*

**Yes. In production high-scale systems, isolating the Control Plane from the Data Plane is a fundamental architectural best practice.**

---

## 1. Workload Disparity: Control Plane vs. Data Plane

In our feature flag architecture, Kafka carries two completely different kinds of traffic:

| Dimension | Control Plane (`flag-changes`) | Data Plane (`deliveries`) |
| :--- | :--- | :--- |
| **Purpose** | Propagate admin flag edits & 2:00 AM emergency kill switches | Deliver diff patch batches to Connection Server slot mailboxes |
| **Throughput** | **Low**: ~10–50 events/minute (peak ~17 writes/sec across 5,000 tenants) | **High**: Bursts of tens of thousands of messages across hundreds of partitions |
| **Latency SLA** | **Ultra-Critical**: Must land on all Evaluators in $< 10\text{ms}$ | **Best-Effort Streaming**: Delivered within $< 500\text{ms}$ |
| **Consumer Pattern** | Broadcast consumption (all Evaluators read every record) | Partition-targeted point-to-point (each Connection Server reads only its slot partition) |
| **Failure Impact** | If blocked, **kill switches fail to reach evaluators** | If blocked, live patch delivery lags slightly |

---

## 2. The "Noisy Neighbor" Risk on a Shared Broker

If both topics live on the same broker under heavy load:

```
                  Shared Single Redpanda Broker
             ┌─────────────────────────────────────┐
             │                                     │
Outbox CDC ─►│ Topic: flag-changes (17 msg/sec)    │◄─ Evaluators
             │                                     │
Evaluators ─►│ Topic: deliveries (50,000 msg/burst)│◄─ Connection Servers
             │                                     │
             └─────────────────────────────────────┘
                               ▲
                    High Socket & Disk Contention
                    Can Delay Emergency Kill Switch!
```

1. **TCP Buffer Saturation**:
   When 250 connection servers are actively consuming large batch payloads from `deliveries`, network interfaces and broker socket buffers are dominated by data plane traffic.
2. **Page Cache & Disk I/O Starvation**:
   High-throughput writes to `deliveries` flush heavily to disk, potentially delaying the low-volume but critical `flag-changes` message.
3. **Head-of-Line Blocking**:
   An emergency kill switch published during an intense percentage rollout could sit in queue behind thousands of delivery messages.

---

## 3. The Isolated Multi-Cluster Pattern

```
┌─────────────────────────────────┐      ┌───────────────────────────────────┐
│     Control Plane Redpanda      │      │       Data Plane Redpanda         │
│         (flag-changes)          │      │          (deliveries)             │
│                                 │      │                                   │
│  - Dedicated to Outbox CDC      │      │  - Dedicated to Slot Mailboxes    │
│  - Ultra-low latency (<5ms)     │      │  - Scaled for high throughput     │
│  - Isolated network & memory    │      │  - Tuned batch sizes & buffers    │
└─────────────────────────────────┘      └───────────────────────────────────┘
```

### Benefits:
1. **Zero Blast-Radius Coupling**: A storm of delivery traffic on the edge can never delay an incoming kill switch.
2. **Independent Tuning**:
   - `redpanda-control` is tuned for **instant flush and zero latency**.
   - `redpanda-deliveries` is tuned for **high throughput and larger I/O batches**.
3. **Independent Scaling**: If we scale from 3 connection servers to 50, we scale the `deliveries` cluster without touching the control cluster.
