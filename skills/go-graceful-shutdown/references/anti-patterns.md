# 12 Anti-Patterns + 3 Counterintuitive Notes

Use this checklist when reviewing or refactoring graceful shutdown.

---

## 12 Anti-Patterns

### Anti-pattern 1: No signal handling

```go
// Wrong
func main() {
	server.ListenAndServe() // dies immediately on SIGTERM
}
```

### Anti-pattern 2: `defer server.Shutdown` + `os.Exit`

```go
// Wrong: defer runs too late for a real graceful stop,
// and os.Exit skips defers entirely
func main() {
	server := startServer()
	defer server.Shutdown(context.Background()) // too late / may never run
	select {
	case <-ctx.Done():
		os.Exit(0) // skips defer
	}
}
```

Call `Shutdown` **explicitly** on the signal path.

### Anti-pattern 3: Shutdown timeout too short

```go
// Wrong: 1s is too short; long requests time out
server.Shutdown(context.WithTimeout(ctx, 1*time.Second))

// Correct: tune to the workload; default at least 30s
shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
if err := server.Shutdown(shutdownCtx); err != nil {
	log.Printf("shutdown error: %v", err)
}
```

### Anti-pattern 4: Context not passed downstream

```go
// Wrong: background semantics underneath
func process() {
	db.QueryRow("SELECT ...") // will not cancel on signal
}

// Correct
func process(ctx context.Context) {
	db.QueryRowContext(ctx, "SELECT ...")
}
```

### Anti-pattern 5: Ignoring Shutdown errors

```go
// Wrong
server.Shutdown(ctx) // error swallowed

// Correct
if err := server.Shutdown(ctx); err != nil {
	log.Printf("shutdown error: %v", err)
}
```

### Anti-pattern 6: Workers without ctx

```go
// Wrong: infinite loop ignores SIGTERM
go func() {
	for {
		job := <-jobCh
		process(job)
	}
}()

// Correct
go func() {
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-jobCh:
			process(ctx, job)
		}
	}
}()
```

### Anti-pattern 7: Close DB before stopping HTTP

```go
// Wrong: in-flight requests still need the DB
defer db.Close()
server.ListenAndServe()

// Correct: stop HTTP first, then close DB
<-ctx.Done()
_ = server.Shutdown(shutdownCtx)
_ = db.Close()
```

### Anti-pattern 8: Handling SIGKILL

```go
// Wrong: SIGKILL cannot be caught
signal.NotifyContext(ctx, syscall.SIGKILL)
```

Correct approach: give Kubernetes enough `terminationGracePeriodSeconds` (e.g. 60s); SIGKILL is the last resort after timeout.

### Anti-pattern 9: Making outbound calls during shutdown

```go
// Wrong: you are dying; do not fan out more RPCs
<-ctx.Done()
notifyOtherService("I'm dying")

// Correct: rely on heartbeats or health checks
// Peers discover failure through probes, not farewell RPCs
```

### Anti-pattern 10: Long-lived HTTP connections ignore cancellation

```go
// Wrong: streaming / long handlers never observe ctx → Shutdown times out → exit 137
http.Serve(l, nil)

// Correct: observe request ctx in the business loop
select {
case <-r.Context().Done():
	return
case data := <-ch:
	send(data)
}
```

Note: setting `ConnContext` alone is not enough. Handlers / streaming loops must observe `r.Context().Done()` (or an equivalent cancel signal) and exit.

### Anti-pattern 11: Noisy shutdown logs

```go
// Wrong: one line per component/worker
log.Println("worker 1 shutting down")
log.Println("worker 2 shutting down")

// Correct: aggregate
log.Info("shutdown started", "components", []string{"http", "grpc"})
log.Info("shutdown complete", "duration_ms", 1234)
```

### Anti-pattern 12: No `terminationGracePeriodSeconds`

```yaml
# Wrong: default 30s; long-lived services get SIGKILL (exit 137)

# Correct: budget for the real workload
terminationGracePeriodSeconds: 60
```

Formula: `terminationGracePeriodSeconds` ≥ `preStop sleep` + app Shutdown timeout + DB/client close slack.

---

## 3 Counterintuitive Notes

### 1. Graceful shutdown is not "wait for every request forever"

Many people think graceful shutdown means wait indefinitely.

In reality: graceful shutdown means **finish within a time budget (e.g. 30s), then force-kill**.

Workloads must be interruptible; otherwise 30s will not save you (classic case: long connections that never read `ctx.Done()`).

### 2. preStop is not redundant

Many people think "just Shutdown on SIGTERM."

Reality:

- Removing a Pod from the Service / syncing the data plane takes time
- SIGTERM can arrive before drain completes
- Requests may still be routed to a dying Pod

A short preStop sleep lets endpoint removal finish before application `Shutdown`.

### 3. `defer` does not always run

```go
defer server.Shutdown(ctx)
// os.Exit(0) after SIGTERM skips defer
// Call Shutdown explicitly
```

---

## Review Cheat Sheet

| Check | Bad signal | Good signal |
|-------|------------|-------------|
| Signals | Only `ListenAndServe` | `NotifyContext` + explicit Shutdown |
| Timeout | 1s / unset | ≥ 30s; longer for long conns |
| ctx | `QueryRow` / workers without ctx | `QueryRowContext` / `select <-ctx.Done()` |
| Order | defer closes DB first | Stop HTTP, then close DB |
| K8s | Default 30s, no preStop | preStop 5–10s + grace 60s+ |
| Force kill | Trying to catch SIGKILL | Rely on grace period |
| Exit notify | Farewell RPC during shutdown | Health checks / heartbeats |
| Logging | Per-worker spam | Aggregated started/complete |
