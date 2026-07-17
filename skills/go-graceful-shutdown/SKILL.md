---
name: go-graceful-shutdown
description: >-
  Implement and review production-grade Go graceful shutdown: signal.NotifyContext,
  http.Server.Shutdown, grpc.GracefulStop, errgroup workers, Kafka/Redis/SQL/pgx
  close order, context propagation, K8s preStop and terminationGracePeriodSeconds.
  Use when writing or reviewing main/bootstrap lifecycle, fixing rolling-update
  request failures, exit 137, SIGTERM handling, in-flight loss, or deploy drain issues.
compatibility: Requires Go 1.16+ (signal.NotifyContext)
metadata:
  domain: language
  triggers: graceful shutdown, SIGTERM, Shutdown, GracefulStop, preStop, errgroup, rolling update, exit 137
  related-skills: go-context, go-concurrency, kubernetes-specialist
---

# Go Graceful Shutdown

Failed user requests during service upgrades are one of the most common Go production pitfalls.

Typical failure chain:

1. Kubernetes sends SIGTERM
2. The process exits immediately
3. Thousands of in-flight requests fail

The root cause is often this simple — no signal handling, no `Shutdown`:

```go
func main() {
	server := &http.Server{Addr: ":8080", Handler: mux}
	server.ListenAndServe() // no signal handling, no graceful shutdown
}
```

It runs, but it cannot stop cleanly. Every Kubernetes rolling update becomes a small outage.

**Correct definition of graceful shutdown**: within a time budget (usually 30s), stop accepting new traffic and finish in-flight work when possible; force-kill on timeout. It is not "wait forever for every request."

## When to Apply

- Writing or reviewing `main`, bootstrapper, HTTP/gRPC/worker lifecycle
- Rolling-update request loss, exit 137, consumer message loss, leftover workers
- Close ordering (HTTP before DB), broken ctx chains, preStop / grace period config

## Agent Workflow

1. **Find the entrypoint**: does `main` / bootstrapper use `signal.NotifyContext`?
2. **Check the stop path**: after the signal, is `Shutdown` / `GracefulStop` called explicitly (not only `defer` + `os.Exit`)?
3. **Check timeout**: ≥ 30s; long-lived connections 60–90s; force `Stop` after timeout
4. **Check close order**: stop traffic and workers first, then close DB/Redis
5. **Check ctx propagation**: DB/Redis/HTTP downstream and long connections observe `ctx.Done()`
6. **Check Kubernetes**: `preStop` delay + `terminationGracePeriodSeconds` ≥ preStop + app timeout
7. **Cross-check anti-patterns**: see [references/anti-patterns.md](references/anti-patterns.md)

## 4-Step Checklist (required for code review)

```
- [ ] Listen for SIGTERM via signal.NotifyContext (also SIGINT for local use)
- [ ] Give Shutdown ≥ 30s; on timeout, force Stop — do not give up silently
- [ ] Propagate ctx all the way down (QueryRowContext / r.Context() / worker select)
- [ ] preStop delay ~5–10s so Service endpoint removal finishes first
```

These four items prevent most upgrade incidents.

## Three Hard Requirements

1. Shutdown timeout **at least 30 seconds**
2. **Propagate ctx to the bottom of the call stack**
3. **preStop delay ~5 seconds** (5–10s is common in production)

## Shutdown Order (wrong order = incomplete shutdown)

```
SIGTERM
  → (optional preStop sleep to drain from the Service)
  → stop new traffic: HTTP Shutdown / gRPC GracefulStop
  → cancel business ctx: workers / Kafka fetch exit
  → wait for in-flight work (with timeout)
  → then close DB / Redis / other clients
  → process exits
```

Wrong-order example: `defer db.Close()` before stopping HTTP → in-flight requests hit a closed pool.

## 12-Pattern Index

Full code and incident notes: [references/patterns.md](references/patterns.md).

| # | Pattern | Key point |
|---|---------|-----------|
| 1 | `signal.NotifyContext` | Standard ctx; cancellation propagates; no hand-rolled signal channels |
| 2 | Multiple signals | SIGTERM = K8s stop; SIGINT = Ctrl+C; SIGHUP = reload (optional) |
| 3 | `http.Server.Shutdown` | Stop new conns + wait for active requests; long conns must end in business code |
| 4 | `sql.DB.Close` | Shutdown HTTP **first**, then Close DB; do not rely on defer alone |
| 5 | `pgxpool.Close` | Waits for active queries; still stop traffic first |
| 6 | errgroup workers | `WithContext` + `Wait`; one cancel stops all |
| 7 | Kafka Reader | `FetchMessage(ctx)` → exit on Canceled → `Close`; do not drop uncommitted work unsafely |
| 8 | go-redis `Close` | Closes the pool; pipelines are not retried → commands need business ctx |
| 9 | `grpc.GracefulStop` | Wait for in-flight RPCs; use `Stop` on timeout |
| 10 | Multi-server errgroup | HTTP/gRPC/cron share `gctx` |
| 11 | ctx end-to-end | Broken chain = work keeps running after SIGTERM |
| 12 | K8s preStop + grace | sleep to drain; grace ≥ full shutdown budget |

## Minimal HTTP Skeleton

```go
ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
defer stop()

server := &http.Server{Addr: ":8080", Handler: mux}
errCh := make(chan error, 1)
go func() {
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		errCh <- err
	}
}()

select {
case err := <-errCh:
	log.Printf("server failed: %v", err)
	os.Exit(1)
case <-ctx.Done():
	log.Println("shutting down...")
}

shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
if err := server.Shutdown(shutdownCtx); err != nil {
	log.Printf("shutdown error: %v", err)
	os.Exit(1)
}
```

Richer HTTP/gRPC/worker/Kafka/K8s examples are in the patterns reference.

## Three Code Review Questions

When someone says "upgrades are fine," ask:

1. How long after SIGTERM until the process exits?
2. What happens to in-flight requests / messages?
3. Are K8s preStop and `terminationGracePeriodSeconds` configured?

One question surfaces three blind spots.

## References (load on demand)

| File | Load when |
|------|-----------|
| [references/patterns.md](references/patterns.md) | Implementing a pattern; need full code and incident cases |
| [references/anti-patterns.md](references/anti-patterns.md) | Reviewing against the 12 anti-patterns and 3 counterintuitive notes |

## Related Skills

- `go-context` — ctx signatures and propagation
- `go-concurrency` — goroutines/channels (not shutdown-specific)
- `kubernetes-specialist` — Deployment lifecycle details
