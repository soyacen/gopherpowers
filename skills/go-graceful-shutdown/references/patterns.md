# 12 Graceful Shutdown Patterns

From signal basics to production Kubernetes coordination. Pick by scenario when implementing or refactoring.

---

## 1. Signals and Cancellation Basics

### Pattern 1: `signal.NotifyContext` (recommended since Go 1.16)

```go
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop() // release resources

	<-ctx.Done() // block until signal
	log.Println("got signal, shutting down")
}
```

Why `NotifyContext` instead of `signal.Notify`?

- Returns a standard `context.Context` every library understands
- Cancellation propagates automatically downstream
- No manual signal channel bookkeeping

### Pattern 2: Multiple signals

```go
// SIGTERM: Kubernetes "please stop" signal
// SIGINT: Ctrl+C for local development
// SIGHUP: traditional Unix "reload config" (optional)

ctx, stop := signal.NotifyContext(context.Background(),
	syscall.SIGINT,
	syscall.SIGTERM,
	syscall.SIGHUP, // reload config
)
defer stop()
```

Note: **do not** listen for `SIGKILL` — it cannot be caught. Rely on the K8s grace period; SIGKILL comes after timeout.

---

## 2. HTTP Server Graceful Shutdown

### Pattern 3: `http.Server.Shutdown`

```go
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	serverErr := make(chan error, 1)
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serverErr <- err
		}
	}()

	select {
	case err := <-serverErr:
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
	log.Println("shutdown complete")
}
```

What `Shutdown` does:

1. Immediately stop accepting new connections
2. Wait for active requests to finish (or timeout)
3. Close idle connections
4. Return `nil` or a timeout error

#### Real incident: long connections outlast Shutdown

A video site was killed by Kubernetes every ~30s during upgrades (exit 137).

Logs:

```text
got signal, shutting down...
shutdown timeout
K8s killed (exit 137)
```

Root cause: streaming / long-lived connections did not finish within 30s, so `Shutdown` timed out.

Fix — observe `ctx.Done()` in business code and disconnect proactively:

```go
select {
case <-r.Context().Done():
	return
case data := <-ch:
	send(data)
}
```

---

## 3. Database Connection Graceful Close

### Pattern 4: `sql.DB.Close` — stop HTTP first, then close DB

```go
import "database/sql"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatal(err)
	}
	// Do not treat defer db.Close() as graceful shutdown — timing is wrong,
	// and os.Exit skips defers.

	server := startServer(db)

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil { // 1. stop HTTP first
		log.Printf("shutdown error: %v", err)
	}
	if err := db.Close(); err != nil { // 2. then close DB
		log.Printf("db close error: %v", err)
	}
}
```

`db.Close()` behavior:

- Closes the connection pool
- Waits for active connections to be returned (does not forcibly kill in-use connections)
- If in-flight requests still need the DB, HTTP must be stopped first

### Pattern 5: `pgxpool.Pool` graceful close

```go
import "github.com/jackc/pgx/v5/pgxpool"

pool, err := pgxpool.New(ctx, dsn)
if err != nil {
	log.Fatal(err)
}

// On the exit path: stop the server first, then pool.Close()
// pgxpool.Close waits for active queries to finish
```

Compared with `database/sql`, `pgxpool` is more aggressive: `Close` waits for active queries. Still stop traffic first so new requests do not arrive.

---

## 4. Worker Pool Graceful Exit

### Pattern 6: Shut down workers with errgroup

```go
import "golang.org/x/sync/errgroup"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	g, gctx := errgroup.WithContext(ctx)

	for i := 0; i < 10; i++ {
		id := i
		g.Go(func() error {
			return worker(gctx, id)
		})
	}

	if err := g.Wait(); err != nil {
		log.Printf("worker error: %v", err)
	}
	log.Println("all workers stopped")
}

func worker(ctx context.Context, id int) error {
	for {
		select {
		case <-ctx.Done():
			log.Printf("worker %d stopping", id)
			return nil
		case job := <-jobCh:
			process(ctx, job)
		}
	}
}
```

Why errgroup helps:

- Any worker error cancels every worker's `ctx`
- `g.Wait()` waits for all workers to exit
- One cancel stops the whole pool

#### Real incident: bare `go worker()` loses data

```go
// Wrong
go worker() // no ctx
go worker()
// On SIGTERM, main exits while workers keep running
// K8s SIGKILL kills the process; in-flight worker data is lost
```

```go
// Correct
g, gctx := errgroup.WithContext(ctx)
g.Go(func() error { return worker(gctx) })
_ = g.Wait()
```

---

## 5. Kafka Consumer Graceful Exit

### Pattern 7: Consumer group stop (segmentio/kafka-go)

```go
import (
	"context"
	"errors"
	"log"
	"os/signal"
	"syscall"

	"github.com/segmentio/kafka-go"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"kafka:9092"},
		GroupID: "my-group",
		Topic:   "events",
	})

	for {
		m, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				break // signal received; exit loop
			}
			log.Printf("fetch error: %v", err)
			continue
		}

		if err := process(ctx, m); err != nil {
			log.Printf("process error: %v", err)
			continue
		}

		if err := reader.CommitMessages(ctx, m); err != nil {
			log.Printf("commit error: %v", err)
		}
	}

	if err := reader.Close(); err != nil {
		log.Printf("close error: %v", err)
	}
}
```

Key points:

- `FetchMessage(ctx)` returns `context.Canceled` after cancel
- Do not drop uncommitted messages unsafely (at least preserve redelivery semantics)
- `Close` waits for active fetches to finish

---

## 6. Redis Client Graceful Exit

### Pattern 8: go-redis `client.Close`

```go
import "github.com/redis/go-redis/v9"

rdb := redis.NewClient(&redis.Options{Addr: "redis:6379"})
// Exit path: stop business/HTTP first, then rdb.Close()
defer rdb.Close() // close the pool; wait for active commands
```

Pitfall: unfinished pipelines are **not retried** on `Close`.

Fix: always pass a cancellable business `ctx`:

```go
rdb.Set(ctx, key, val, 0) // returns error when ctx is canceled
```

---

## 7. gRPC Server Graceful Exit

### Pattern 9: `grpc.GracefulStop`

```go
import "google.golang.org/grpc"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	s := grpc.NewServer()
	pb.RegisterMyServiceServer(s, &server{})

	go func() {
		if err := s.Serve(lis); err != nil {
			log.Printf("serve: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down...")

	done := make(chan struct{})
	go func() {
		s.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
		log.Println("grpc stopped")
	case <-time.After(30 * time.Second):
		s.Stop() // force stop
		log.Println("grpc force stopped")
	}
}
```

| API | Behavior |
|-----|----------|
| `GracefulStop` | Wait for in-flight RPCs to finish |
| `Stop` | Close immediately |

---

## 8. Multiple Servers, One Graceful Exit

### Pattern 10: Manage multiple servers with errgroup

```go
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error { return runHTTP(gctx, ":8080") })
	g.Go(func() error { return runGRPC(gctx, ":9090") })
	g.Go(func() error { return runCron(gctx) })

	if err := g.Wait(); err != nil {
		log.Printf("service error: %v", err)
	}
	log.Println("all servers stopped")
}

func runHTTP(ctx context.Context, addr string) error {
	server := &http.Server{Addr: addr, Handler: mux}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Printf("http shutdown: %v", err)
		}
	}()
	err := server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
```

---

## 9. Context Propagation Is the Key

### Pattern 11: Pass ctx all the way down

```go
// Wrong: ctx chain broken
func process() {
	data := db.QueryRow("SELECT ...") // background semantics
	// After SIGTERM, this query keeps running
}

// Correct: pass ctx through
func process(ctx context.Context) {
	data := db.QueryRowContext(ctx, "SELECT ...")
	// After SIGTERM, ctx cancels and the query cancels too
}
```

Benefits:

- Business logic exits with the signal
- DB queries cancel with the signal
- Outbound HTTP calls cancel with the signal
- One cancel drains the dependency chain

---

## 10. Kubernetes Graceful Shutdown Details

### Pattern 12: preStop hook + `terminationGracePeriodSeconds`

```yaml
apiVersion: apps/v1
kind: Deployment
spec:
  template:
    spec:
      terminationGracePeriodSeconds: 60 # SIGKILL after this budget
      containers:
      - name: app
        lifecycle:
          preStop:
            exec:
              command: ["sh", "-c", "sleep 10"] # delay before real app shutdown
```

Why preStop?

1. The Pod is removed from the Service, but removal can lag (kube-proxy / data-plane sync)
2. If SIGTERM arrives and the process exits immediately, traffic may still be routed to a dying Pod
3. A short preStop sleep lets endpoint removal finish before application `Shutdown`

How to set `terminationGracePeriodSeconds`:

| Scenario | Suggestion |
|----------|------------|
| Default | 30s (often too short) |
| Typical HTTP/gRPC | 60s |
| Long connections / large requests | 60–90s |
| Formula | ≥ preStop sleep + app Shutdown timeout + DB close slack |

---

## Summary: The Four-Piece Kit

1. `signal.NotifyContext` — standard signal handling
2. `server.Shutdown` / `GracefulStop` — stop traffic, wait for in-flight work
3. `errgroup` + ctx — unified multi-goroutine exit
4. `preStop` + `terminationGracePeriodSeconds` — Kubernetes drain and timeout budget
