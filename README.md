# go-devkit

[![Go Reference](https://pkg.go.dev/badge/github.com/diegodesousas/go-devkit.svg)](https://pkg.go.dev/github.com/diegodesousas/go-devkit)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](./LICENSE)

Go developer kit for multipurpose applications.

```
go get github.com/diegodesousas/go-devkit
```

The reference documentation lives on [pkg.go.dev](https://pkg.go.dev/github.com/diegodesousas/go-devkit) — every package has an overview and runnable examples. The table below is a map of what is in the box.

## Packages

| Package | What it does |
|---|---|
| [`httpserver`](https://pkg.go.dev/github.com/diegodesousas/go-devkit/pkg/httpserver) | HTTP server from a route list, middleware chain and error handler. Graceful shutdown, request logging and metrics per route |
| [`httpserver/httpservertest`](https://pkg.go.dev/github.com/diegodesousas/go-devkit/pkg/httpserver/httpservertest) | Test helpers for handlers that read path parameters |
| [`stream`](https://pkg.go.dev/github.com/diegodesousas/go-devkit/pkg/stream) | Records, headers and message payloads, plus the `Reader`/`Writer` seam a Kafka driver implements |
| [`stream/kafka`](https://pkg.go.dev/github.com/diegodesousas/go-devkit/pkg/stream/kafka) | The franz-go driver: `NewReader` and `NewWriter`, with brokers, SASL, TLS and timeouts configured once |
| [`stream/consumer`](https://pkg.go.dev/github.com/diegodesousas/go-devkit/pkg/stream/consumer) | Typed Kafka consumer with per-partition concurrency, retry, dead letter topic and graceful shutdown |
| [`stream/dispatcher`](https://pkg.go.dev/github.com/diegodesousas/go-devkit/pkg/stream/dispatcher) | Kafka producer with synchronous delivery confirmation and trace propagation |
| [`log`](https://pkg.go.dev/github.com/diegodesousas/go-devkit/pkg/log) | Leveled logger with structured fields, carried through the context |
| [`database/sql`](https://pkg.go.dev/github.com/diegodesousas/go-devkit/pkg/database/sql) | PostgreSQL connection with context-scoped transactions and typed errors |
| [`database/sql/upsert`](https://pkg.go.dev/github.com/diegodesousas/go-devkit/pkg/database/sql/upsert) | Builder for `INSERT ... ON CONFLICT` statements |
| [`cache`](https://pkg.go.dev/github.com/diegodesousas/go-devkit/pkg/cache) | Redis cache, raw strings or JSON-encoded values through a generic repository |
| [`metrics`](https://pkg.go.dev/github.com/diegodesousas/go-devkit/pkg/metrics) | StatsD metrics emitted through a client carried in the context |
| [`validator`](https://pkg.go.dev/github.com/diegodesousas/go-devkit/pkg/validator) | Composable validation rules with machine-readable error codes |
| [`gen`](https://pkg.go.dev/github.com/diegodesousas/go-devkit/pkg/gen) | String generators: UUID, ULID and sequence |
| [`mapper`](https://pkg.go.dev/github.com/diegodesousas/go-devkit/pkg/mapper) | Generic registry keyed by a string-like type |
| [`encoding`](https://pkg.go.dev/github.com/diegodesousas/go-devkit/pkg/encoding) | JSON serializer behind an interface |
| [`httpclient`](https://pkg.go.dev/github.com/diegodesousas/go-devkit/pkg/httpclient) | `*http.Client` behind a one-method interface, with a mock |

## Migrating to v0.3.0

v0.3.0 replaces `confluent-kafka-go` with [franz-go](https://github.com/twmb/franz-go) and redesigns the consumer. Every break is in `pkg/stream`, `pkg/stream/consumer` and `pkg/stream/dispatcher`. The library is now pure Go: `CGO_ENABLED=1` is no longer required to build it.

| Before | After |
|---|---|
| `Handler.ID()`, `Handler.Topic()` | parameters of `kafka.NewReader(groupID, topics)` |
| `consumer.NewFactory(...)` | `kafka.NewReader(groupID, topics, ...)` |
| `dispatcher.NewClient(...)` | `kafka.NewWriter(...)` |
| `consumer.Client`, `consumer.Factory` | `stream.Reader` |
| `dispatcher.Client` | `stream.Writer` |
| `c.Run() (Shutdown, error)` + `ListenShutdown()` | `c.Run(ctx) error`, blocking until ctx is cancelled |
| `d.Shutdown()` | `d.Close(ctx) error` |
| `Handler.ShouldSkip()`, `Handler.ConfigRetry()` | options `consumer.WithSkip`, `consumer.WithRetry` |
| `stream.NewMessageType(*kafka.Message)` | `stream.NewMessageType(stream.Record)` |
| `consumer.WithBootstrapServer`, `dispatcher.WithBootstrapServers` | one `kafka.WithBrokers`, shared by reader and writer |

Three changes worth knowing about, none of which shows up as a compile error:

- **The group id is now yours to pass.** It used to be assembled as `devkit-` + `Handler.ID()`. Kafka cannot tell that a new name means the same consumer — it sees a group with no committed offsets and starts from the earliest record in retention — so an existing deployment has to pass its old name, prefix included.
- **`Handle` is now called concurrently**, one goroutine per partition of a batch and serial within each. A handler holding state has to be safe for concurrent use.
- **The consumer stops itself** with `consumer.ErrDeadLetterUnavailable` when every partition it is consuming has halted on a failed dead letter publication. Restarting is the recovery path; nothing is committed past an unresolved record, so nothing is lost.

## Runnable examples

The `examples/` directory holds end-to-end programs, one `package main` per feature. They need the services they talk to — Kafka, Redis, PostgreSQL — running locally.

- [http server](./examples/httpserver/main.go)
- [stream consumer](./examples/stream/consumer/main.go) · [stream dispatcher](./examples/stream/dispatcher/main.go)
- [logger](./examples/log/main.go)
- [cache](./examples/cache/main.go)
- [database](./examples/db/main.go)

Shorter, self-contained snippets are the `Example` functions in each package, rendered on pkg.go.dev.

## Development

The library is pure Go and builds without cgo. `-race` still requires `CGO_ENABLED=1`, but that is a Go toolchain requirement, not a dependency one.

| Command | What it does |
|---|---|
| `make test` | Unit tests with `-race`. No Docker needed |
| `make test-integration` | `pkg/database/sql` only, with `-tags=integration`. Needs Docker |
| `make test-all` | Both |
| `make lint` | `gofmt -l`, `go vet` and `golangci-lint` |
| `make fmt` | `gofmt -w` over `./pkg` and `./examples` |
| `make release [BUMP=patch\|minor\|major]` | Cuts a version: validates, tests, tags, pushes and opens the GitHub release |

## License

[MIT](./LICENSE)
