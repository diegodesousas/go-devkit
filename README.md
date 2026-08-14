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
| [`stream/consumer`](https://pkg.go.dev/github.com/diegodesousas/go-devkit/pkg/stream/consumer) | Typed Kafka consumer with retry, dead letter topic and graceful shutdown |
| [`stream/dispatcher`](https://pkg.go.dev/github.com/diegodesousas/go-devkit/pkg/stream/dispatcher) | Kafka producer with synchronous delivery confirmation |
| [`stream`](https://pkg.go.dev/github.com/diegodesousas/go-devkit/pkg/stream) | Message payloads exchanged over Kafka, JSON and text |
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
