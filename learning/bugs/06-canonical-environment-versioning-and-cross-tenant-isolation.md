# Bug Analysis: Canonical Environment Versioning & Cross-Tenant Isolation

## Bug 12: Global Sequence Tenant Activity Leakage & Version Gaps

### Problem
In migration 000004, a single global PostgreSQL sequence `environment_version_seq` was introduced to avoid row locks on the `environments` table:
```sql
SELECT nextval('environment_version_seq');
```
This caused two critical architectural defects:
1. **Cross-Tenant Activity Leak**: Because all tenants drew from the same sequence, Tenant A updating 100 flags caused Tenant B's environment version to skip by 100 numbers. A malicious or competitive tenant could determine global mutation volume across all customers.
2. **Broken Gap Detection**: Downstream SDKs rely on monotonic gapless sequences ($V, V+1, V+2$) to know whether a reconnect missed an event. Arbitrary gaps broke this contract.

### Fix
1. Removed the global sequence in migration 000005 (`DROP SEQUENCE IF EXISTS environment_version_seq`).
2. Updated all mutations (`CreateFlag`, `UpdateFlag`, `KillFlag`, `RollbackFlag`, `CreateSegment`, `UpdateSegment`) to increment and return `environments.environment_version` directly within the mutation transaction:
   ```sql
   UPDATE environments 
   SET environment_version = environment_version + 1 
   WHERE id = $1 
   RETURNING environment_version;
   ```
   At production write volumes (~17 writes/sec across 5,000 tenants), updates are partitioned across thousands of environments, rendering row lock contention negligible while guaranteeing strict monotonicity and perfect tenant isolation.

---

## Bug 13: Correlated Outbox Subquery Reversion on Retention Cleanup

### Problem
Because `environments.environment_version` was no longer updated, `GetEnvironment` and `ListEnvironments` executed:
```sql
SELECT e.id, e.name, 
       COALESCE((SELECT MAX(o.env_version) FROM outbox o WHERE o.env_id = e.id), e.environment_version) AS environment_version
FROM environments e;
```
If an operator pruned historical outbox rows (e.g. `DELETE FROM outbox WHERE created_at < NOW() - INTERVAL '14 days'`), an environment that had no updates in 14 days would have `MAX(o.env_version) == NULL`. The query fell back to `e.environment_version`, which was still `1`! Environments reverted from version 10,000 back to 1.

### Fix
1. Migration 000005 synced canonical `environment_version` in the `environments` table.
2. Replaced the correlated subquery in `GetEnvironment` and `ListEnvironments` with a direct read of `e.environment_version`. The entity table is now the sole, permanent source of truth.
