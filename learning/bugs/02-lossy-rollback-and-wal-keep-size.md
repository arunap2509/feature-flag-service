# Bug Analysis: Lossy Rollback & Engine Safeguards

## Bug 3: Lossy Rollback (Scrambled Percentage Allocations)

### Problem
The `flag_history` table schema only stored:
`flag_id, version, state, rules, changed_by, created_at`.
It completely omitted:
* `fallback_value`
* `salt`

When an engineer called `POST /flags/{key}/rollback/{version}`:
1. `state` and `rules` were copied from the historical version.
2. But `salt` and `fallback_value` remained whatever was in the latest version!
3. If version 4 reshuffled the flag's salt, rolling back to version 3 retained version 4's salt. Because percentage rollouts compute `hash(flag_key + salt + user_id) % 100`, every user's percentage rollout bucket was scrambled, turning a rollback into an active incident.

### Fix
1. Migration `000003_add_salt_and_fallback_to_history.up.sql`:
   ```sql
   ALTER TABLE flag_history ADD COLUMN IF NOT EXISTS fallback_value BOOLEAN NOT NULL DEFAULT FALSE;
   ALTER TABLE flag_history ADD COLUMN IF NOT EXISTS salt VARCHAR(64) NOT NULL DEFAULT '';
   ```
2. Updated `CreateFlag`, `UpdateFlag`, `KillFlag` to archive `salt` and `fallback_value` into every history row.
3. Updated `RollbackFlag` to restore `fallback_value` and `salt` directly from history, ensuring a 100% deterministic snapshot restoration.

---

## Bug 4: Missing `max_slot_wal_keep_size` Engine Circuit Breaker

### Problem
In `docker-compose.yml`, PostgreSQL was started with `wal_level=logical`, but without a WAL retention ceiling. If a replication slot stopped consuming, PostgreSQL would hold WAL segments until the host disk ran out of space.

### Fix
Appended `-c max_slot_wal_keep_size=10GB` to PostgreSQL's startup command in `docker-compose.yml`. If a replication slot falls behind by more than 10GB, PostgreSQL invalidates the slot to preserve database uptime.
