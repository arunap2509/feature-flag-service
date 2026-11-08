# Bug Analysis: Rule AST Validation & HTTP Server Hardening

## Bug 7: Production HTTP Server Hardening & Graceful Shutdown

### Problem
`cmd/mgmt-api/main.go` invoked Go's raw `http.ListenAndServe("...", loggingMux)`:
1. **Zero Timeouts**: Go's default HTTP server has no `ReadTimeout`, `WriteTimeout`, or `IdleTimeout`. Malicious or slow clients could hold TCP connections open indefinitely (Slowloris attack), exhausting server file descriptors and memory.
2. **Abrupt Termination**: Killing the container or deploying updates (`SIGTERM`) aborted in-flight HTTP requests and uncommitted database transactions immediately without draining.

### Fix
Constructed an explicit `&http.Server`:
```go
server := &http.Server{
    Addr:         fmt.Sprintf(":%s", port),
    Handler:      loggingMux,
    ReadTimeout:  5 * time.Second,
    WriteTimeout: 10 * time.Second,
    IdleTimeout:  120 * time.Second,
}
```
And implemented graceful shutdown using `signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)`:
When a signal is caught, `server.Shutdown(ctx)` is invoked with a 10-second drain deadline, allowing active API transactions to complete cleanly.

---

## Bug 8: Rule AST Validation

### Problem
Rules were accepted as unstructured `json.RawMessage`. Clients could store malformed JSON (negative rollout percentages, missing segments, or nonsensical operators). The Management API would persist it and stream it to Kafka, causing downstream evaluators to crash.

### Fix
Created `validateRules()` in `internal/mgmt/service/service.go`:
* Unmarshals into `[]model.Rule` and validates:
  1. `rule.Order > 0`.
  2. If `RolloutPercentage` is set, it must be between `0` and `100`.
  3. The rule must specify at least one valid evaluation criteria (either a `Segment` string or a `RolloutPercentage`).
* Invalid requests fail immediately with `400 Bad Request` before touching the database or outbox stream.
