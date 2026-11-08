# Scaling to 50+ Evaluator Nodes: From Docker Compose to Production Kubernetes

## 1. Why Docker Compose Doesn't Scale Beyond Local Testing
In local development, Docker Compose is ideal for fast iteration with 1–3 containers. However, scaling to 50+ instances on Docker Compose fails due to:
1. **Single-Host Resource Limits**: All containers contend for the CPU, RAM, and network bandwidth of a single physical machine.
2. **Port Collisions**: Host port mappings (`9001:9001`, `9002:9002`) cannot scale to 50 without manual port incrementing.
3. **No Cross-Host Scheduling**: If a server hardware fails, Compose cannot migrate containers to another rack or availability zone.
4. **No Auto-Healing or Dynamic Scaling**: No built-in Horizontal Pod Autoscaler (HPA) to scale nodes based on traffic volume.

---

## 2. Production Topology: Kubernetes (`Deployment` or `StatefulSet`)

In production (e.g., EKS, GKE, self-hosted K8s), the 50 Evaluators are deployed as a single **Kubernetes Deployment** or **StatefulSet**:

```yaml
# production-evaluator-k8s.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: evaluator-fleet
  namespace: feature-flags
spec:
  replicas: 50 # Scaled horizontally to 50 pods
  selector:
    matchLabels:
      app: evaluator
  template:
    metadata:
      labels:
        app: evaluator
    spec:
      containers:
        - name: evaluator
          image: internal-registry.corp/feature-flag-evaluator:v1.2.0
          env:
            - name: PORT
              value: "9001"
            # In K8s, Downward API provides unique node name per pod automatically!
            - name: NODE_ID
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: POD_IP
              valueFrom:
                fieldRef:
                  fieldPath: status.podIP
            - name: KAFKA_BROKER
              value: "kafka-cluster-kafka-bootstrap.kafka.svc:9092"
            - name: ETCD_ENDPOINTS
              value: "etcd-cluster-client.etcd.svc:2379"
            - name: SHARD_COUNT
              value: "1024" # High discrete shard space for uniform distribution
          resources:
            requests:
              cpu: "1000m"
              memory: "2Gi"
            limits:
              cpu: "2000m"
              memory: "4Gi"
          readinessProbe:
            httpGet:
              path: /healthz
              port: 9001
            initialDelaySeconds: 2
            periodSeconds: 3
```

---

## 3. How the Architecture Accommodates 50+ Nodes Automatically

Our Go codebase was explicitly designed for dynamic cloud-native scaling:

### 1. Zero Code Changes for Dynamic Node Identity
In `cmd/evaluator/main.go`:
```go
nodeID := os.Getenv("NODE_ID") // In K8s: evaluator-fleet-7b49f8649-abcde
```
Every pod receives its unique Pod name from Kubernetes, registers its own 3-second lease at `/members/evaluators/<pod-name>` in `etcd`, and joins the ring.

### 2. High Shard Space (`SHARD_COUNT = 1024` or `4096`)
In local testing, we used `SHARD_COUNT: 3`. In production, you configure a fixed, high power of 2 (e.g. 1024 shards):
$$\text{Average Shards per Evaluator} = \frac{1024 \text{ shards}}{50 \text{ nodes}} \approx 20.48 \text{ shards/node}$$

When node 51 starts or node 12 terminates:
* **Only $\approx 20$ shards ($1/50 = 2\%$)** move across the ring!
* The remaining $98\%$ of shards stay exactly where they are with zero cache invalidation.

### 3. Kafka Broadcast Consumption
Each of the 50 Evaluators registers an independent consumer group:
```go
kgo.ConsumerGroup(fmt.Sprintf("%s-%s", constants.KafkaGroupEvaluator, cfg.NodeID))
```
Kafka's broadcast fan-out pushes the outbox CDC event to all 50 evaluators simultaneously in $< 10\text{ms}$.

### 4. Client Routing via Connection Server Gateways
Clients never talk directly to random evaluator pods. They connect to an edge fleet of **stateless Connection Servers**:
```
Clients (Web / iOS / Android)
        │
        ▼ (Anycast DNS / AWS NLB)
┌──────────────────────────────────────┐
│  Stateless Edge Connection Gateways  │  (Handles 100k+ idle SSE sockets)
└──────────────────┬───────────────────┘
                   │ Looks up Hash Ring in etcd
                   ▼
┌──────────────────────────────────────┐
│       50 Evaluator Pods (K8s)        │  (Runs in-memory rule engine)
└──────────────────────────────────────┘
```
