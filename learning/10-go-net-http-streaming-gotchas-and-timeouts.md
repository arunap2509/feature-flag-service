# Go `net/http` Long-Lived Streaming, SSE Gotchas, and Server Timeouts

## 1. The Catastrophic Streaming Gotcha: `http.Server.WriteTimeout`
In standard Go REST services, best practice is to configure explicit timeouts on `http.Server`:
```go
// ANTI-PATTERN FOR STREAMING SERVERS (SSE, WebSockets, gRPC-Web)
server := &http.Server{
    Addr:         ":9001",
    Handler:      mux,
    ReadTimeout:  5 * time.Second,
    WriteTimeout: 10 * time.Second, // <--- CATASTROPHIC FOR LONG-LIVED SSE!
    IdleTimeout:  120 * time.Second,
}
```

### Why This Breaks Streaming:
In Go's `net/http`, `WriteTimeout` is an **absolute deadline** calculated from the moment the request headers are read to the final byte of the response:
- For a standard REST endpoint (`/healthz` or `/api/v1/flags`), 10 seconds is more than enough.
- For a **streaming connection** (Server-Sent Events or WebSockets) meant to stay open for hours:
  **The server abruptly cuts the TCP connection exactly 10 seconds after connecting!**
- If a user opens the stream, waits 10 seconds, and triggers a feature flag kill switch, the connection has already been severed by Go's internal timeout!

---

## 2. The Production Solution (Go 1.20+)

### Step 1: Per-Request Long-Lived Override with `http.NewResponseController`
Go 1.20 introduced `http.NewResponseController`, which allows a specific HTTP handler to dynamically manipulate connection deadlines:

```go
// In the SSE stream handler:
func HandleSSEStream(w http.ResponseWriter, r *http.Request) {
    flusher, ok := w.(http.Flusher)
    if !ok {
        http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
        return
    }

    // Disable response write timeout specifically for this long-lived stream!
    rc := http.NewResponseController(w)
    _ = rc.SetWriteDeadline(time.Time{}) // time.Time{} removes the write deadline
    
    // ...
}
```

### Step 2: Edge Server Configuration for Streaming Fleets
For gateway fleets and evaluators hosting persistent client streams, configure the server with:
```go
server := &http.Server{
    Addr:              ":9001",
    Handler:           mux,
    ReadHeaderTimeout: 5 * time.Second,  // Protects against Slowloris attacks
    WriteTimeout:      0,                // 0 disables the global write deadline for streaming
    IdleTimeout:       120 * time.Second,
}
```

---

## 3. SSE Heartbeat & Keepalive Protocol
Proxies, load balancers (AWS ALB, NGINX, Cloudflare), and OS NAT tables silently drop idle TCP connections after 30 to 60 seconds without data.

To keep streaming connections healthy, the streaming loop must emit periodic **SSE comment frames** (`: keepalive\n\n`), which are ignored by clients but keep TCP buffers active:

```go
ticker := time.NewTicker(15 * time.Second)
defer ticker.Stop()

ctx := r.Context()
for {
    select {
    case <-ctx.Done():
        // Client disconnected (tab closed, network dropped)
        return

    case <-ticker.C:
        // Periodic keepalive comment prevents proxy timeout
        if _, err := fmt.Fprintf(w, ": keepalive\n\n"); err != nil {
            return
        }
        flusher.Flush()

    case patch := <-patchChan:
        // Push real-time event frame
        data, _ := json.Marshal(patch)
        if _, err := fmt.Fprintf(w, "event: patch\ndata: %s\n\n", data); err != nil {
            return
        }
        flusher.Flush()
    }
}
```

---

## 4. Client-Side `EventSource` Auto-Reconnection Safeguard
Browsers have native auto-reconnection built into `EventSource`.

### The Client Gotcha:
If the frontend unconditionally calls `eventSource.close()` inside `onerror`:
```javascript
// BUG: Kills browser auto-reconnect on transient hiccups
eventSource.onerror = (err) => {
    eventSource.close(); // <--- Prevents browser from reconnecting!
};
```

### The Correct Pattern:
Only close if `readyState` has permanently transitioned to `CLOSED`:
```javascript
eventSource.onerror = (err) => {
    // If readyState is CONNECTING (0), the browser is actively auto-reconnecting.
    // Only tear down state if the connection was permanently closed (2).
    if (activeEventSource && activeEventSource.readyState === EventSource.CLOSED) {
        activeEventSource.close();
        activeEventSource = null;
        updateUIOnDisconnect();
    }
};
```
