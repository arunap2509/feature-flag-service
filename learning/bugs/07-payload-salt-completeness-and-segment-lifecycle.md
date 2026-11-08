# Bug Analysis: Payload Salt Completeness & Segment Lifecycle Outbox Events

## Bug 14: Missing `salt` in `OutboxEventPayload`

### Problem
Consistent percentage rollouts evaluate users via:
$$\text{bucket} = \text{hash}(\text{flag\_key} + \text{salt} + \text{user\_id}) \pmod{100,000}$$
While migration 000003 archived `salt` in PostgreSQL `flag_history`, `OutboxEventPayload` omitted the `Salt` field entirely.
When flag events were published to Kafka, downstream Evaluators received no salt. Evaluators could either evaluate with an empty salt (violating stickiness across version rollbacks) or make synchronous database calls back to PostgreSQL on every change, violating decoupled event-driven architecture.

### Fix
1. Added `Salt string 'json:"salt,omitempty"'` to `model.OutboxEventPayload`.
2. Passed `Salt` into `OutboxEventPayload` across all flag lifecycle operations: `CreateFlag`, `UpdateFlag`, `KillFlag`, and `RollbackFlag`.

---

## Bug 15: Silent `CreateSegment` (Missing Outbox Event & ETag)

### Problem
`CreateSegment` previously executed an isolated `INSERT INTO segments` without a transaction, without writing to the `outbox` table, and without bumping `environment_version`.
Furthermore:
1. `EventTypeSegmentCreated` did not exist in `internal/constants/constants.go`.
2. Downstream Evaluator nodes consuming `flag-changes` were never notified that a segment was created until someone later updated it.
3. The HTTP handler omitted the `ETag` header on `201 Created`, preventing clients from immediately using the ETag for optimistic concurrency on subsequent `PUT` requests.

### Fix
1. Added `EventTypeSegmentCreated = "SEGMENT_CREATED"` to `internal/constants/constants.go`.
2. Wrapped `CreateSegment` in a database transaction that:
   - Inserts the new segment with default `version = 1`.
   - Atomically increments `environments.environment_version`.
   - Inserts an outbox record with `EventTypeSegmentCreated`.
3. Added `w.Header().Set(constants.HeaderETag, fmt.Sprintf("\"%d\"", seg.Version))` to the `CreateSegment` HTTP handler.
