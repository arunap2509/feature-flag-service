# Bug Analysis: Segment Optimistic Locking & Database Hot-Row Contention

## Bug 5: Missing Optimistic Locking on User Segments

### Problem
While `flags` implemented optimistic locking via `If-Match` ETags, the `segments` table lacked a `version` column entirely.
`PUT /api/v1/environments/{env_id}/segments/{key}` unconditionally overwrote user ID arrays. If two developers simultaneously modified segment membership, the slower request would silently overwrite the faster request's additions (lost updates).

### Fix
1. Migration `000004_segment_version_and_env_seq.up.sql`:
   ```sql
   ALTER TABLE segments ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 1;
   ```
2. Updated `UpdateSegment` repository query to acquire row lock and compare `currentVer == *expectedVersion`.
3. If versions mismatch, return `412 Precondition Failed`.
4. Returns `ETag: "<version>"` header on `POST`, `GET`, and `PUT` segment endpoints.

---

## Bug 6: Hot-Row Lock Serialization on Environments

### Problem
Every flag creation, update, kill, rollback, and segment update previously executed:
```sql
UPDATE environments SET environment_version = environment_version + 1 WHERE id = $1;
```
Because this ran inside an open transaction alongside flag updates, it acquired an exclusive `ROW EXCLUSIVE` lock on that environment's row.
If 10 developers or automated deployment scripts updated 10 *different* flags in the same environment at the same time, all 10 transactions were serialized into a lock wait queue on the single `environments` row, causing lock contention and query timeouts under load.

### Fix
1. Migration `000004_segment_version_and_env_seq.up.sql`:
   ```sql
   CREATE SEQUENCE IF NOT EXISTS environment_version_seq START WITH 1000;
   ```
2. Mutations now fetch `nextval('environment_version_seq')` to stamp the outbox event and the environment cursor. PostgreSQL sequences are atomic and **completely lock-free**, allowing unlimited concurrent flag operations within the same environment.
3. `GetEnvironment` and `ListEnvironments` dynamically query `COALESCE((SELECT MAX(env_version) FROM outbox WHERE env_id = e.id), e.environment_version)`, backed by the existing `(env_id, env_version)` index.
