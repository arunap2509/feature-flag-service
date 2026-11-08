# Deterministic Percentage Rollouts, Salting, and Stateless Evaluation

## 1. The Naive Approach and Why It Fails
A common naive design for percentage rollouts is:
```go
// ANTI-PATTERN: Never do this in production
func IsEnabled(rolloutPercent int) bool {
    return rand.Float64() * 100 < float64(rolloutPercent)
}
```
### Why Random Rollouts Fail:
1. **Flips on Every Request**: If user Alice refreshes her checkout page, she might see the new checkout flow on request 1 (`rand=0.15`), and the old checkout flow on request 2 (`rand=0.85`). This creates confusing bugs, session corruption, and angry customers.
2. **Breaks Caching**: Client-side SDK caches and CDN edge caches cannot store the evaluation result if it is non-deterministic.
3. **No Coordinated Edge Evaluation**: If Alice talks to Edge Server A on one request and Edge Server B on the next, they must evaluate the exact same flag value without communicating with each other.

---

## 2. Deterministic Hash Bucketing (The Production Standard)
In production feature flag systems (LaunchDarkly, Split.io, Google Experiments), percentage rollouts use **deterministic cryptographic hashing** across a large discrete bucket space:

$$\text{bucket} = \text{SHA-256}(\text{flag\_key} + \text{salt} + \text{user\_id}) \pmod{100,000}$$

```
0                                 20,000                                 100,000
[======== 20% Cohort (ON) ========|============= 80% Cohort (OFF) =============]
    ↑                                    ↑
usr_david (Bucket: 14,376)          usr_alice (Bucket: 33,561)
   -> Evaluates TRUE                    -> Evaluates FALSE
```

### Why 100,000 Buckets?
Using 100,000 buckets provides **0.001% granularity** (e.g., enabling smooth canary rollouts at `0.01%`, `0.1%`, `1.5%`, `20%`).

### Properties:
1. **Consistency**: User Alice will *always* hash into the exact same bucket for a given flag, regardless of when, where, or how many times she accesses the application.
2. **Uniform Distribution**: SHA-256 provides pseudo-random uniform distribution, ensuring that a 20% rollout targets exactly 20% of random users across the globe.
3. **Zero Database Lookups**: The evaluation executes purely in memory in under **1 microsecond** ($< 1\mu\text{s}$) with zero network I/O.

---

## 3. The Critical Role of Salting (`flag.salt`)

### Problem 1: Cohort Correlation (The Guinea Pig Problem)
If the hash was simply `SHA-256(user_id) % 100,000`:
- Any user whose `user_id` hashes into bucket `0` through `19,999` would receive the 20% rollout for **EVERY feature flag** deployed across the entire company.
- That unfortunate 20% of users would experience all unstable alpha features, while the other 80% would never see any experimental features.

### Solution: Per-Flag Salt
By combining `flag_key + salt + user_id`:
- The hash space is completely re-shuffled for each flag.
- Alice might be in bucket `14,000` (included) for flag `checkout_v2`, but in bucket `85,000` (excluded) for flag `dark_mode`.

### Problem 2: Lossless Rollback & Experiment Resumption
When a feature is killed or temporarily rolled back, the system preserves the flag's `salt`. 
When the feature is turned back on or ramped back up, users who were already in the rollout cohort maintain their exact same experience without cohort reshuffling.

---

## 4. Why the Evaluator is Completely Stateless Regarding Users
The Evaluator does **not** maintain a database table of registered users. 
Any arbitrary client string (e.g., `usr_alice`, `usr_david`, `session_982341`, `device_uuid_883`) is evaluated on-the-fly:

```go
// internal/evaluator/engine/engine.go
func EvaluateFlag(flag *ParsedFlag, segments map[string]*ParsedSegment, ctx UserContext) bool {
    // 1. Fast Path: Kill switch takes precedence
    if flag.State != constants.FlagStateOn {
        return flag.FallbackValue
    }

    // 2. Evaluate targeting rules in strict priority order
    for _, rule := range flag.Rules {
        // Percentage Rollout check
        if rule.RolloutPercentage > 0 {
            bucket := ComputeBucket(flag.Key, flag.Salt, ctx.UserID)
            threshold := uint64(rule.RolloutPercentage * constants.RolloutMultiplier)
            if bucket >= threshold {
                continue // Outside percentage cohort, skip rule
            }
        }

        return rule.Value
    }

    return flag.FallbackValue
}
```

### Live Example Trace:
* **Flag**: `show_animation` (Salt: `cbda9ba252d8d95e`, State: `on`, Rollout: `20%`)
* **Threshold**: $20 \times 1,000 = 20,000$

| User ID | Evaluated SHA-256 Bucket | Comparison | Final Result |
| :--- | :--- | :--- | :--- |
| `usr_alice` | **33,561** | $33,561 \ge 20,000$ (Excluded) | `false` (Fallback) |
| `usr_bob` | **95,343** | $95,343 \ge 20,000$ (Excluded) | `false` (Fallback) |
| `usr_david` | **14,376** | $14,376 < 20,000$ (Included) | **`true`** (Rule Match) |
| `user_3` | **18,188** | $18,188 < 20,000$ (Included) | **`true`** (Rule Match) |
| `user_10` | **2,826** | $2,826 < 20,000$ (Included) | **`true`** (Rule Match) |
