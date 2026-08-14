# Kafka franz-go Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the Kafka packages off `confluent-kafka-go` onto `franz-go`, dropping the cgo requirement, and fix the consumer's lifecycle, cancellation and dead-letter gaps in the same round.

**Architecture:** `pkg/stream` declares driver-neutral types (`Record`, `Header`) and two seams (`Reader`, `Writer`). A new `pkg/stream/kafka` package is the only place that imports franz-go and holds all broker configuration. `pkg/stream/consumer` and `pkg/stream/dispatcher` depend on the seams and never on the driver, so the next driver swap touches one package and breaks no API.

**Tech Stack:** Go 1.26, `github.com/twmb/franz-go` v1.21.6, `github.com/cenkalti/backoff/v5` v5.0.3, `github.com/pkg/errors`, `testify/mock`, `testing/synctest`, dd-trace-go v2.

**Spec:** [docs/superpowers/specs/2026-08-13-kafka-franz-go-design.md](../specs/2026-08-13-kafka-franz-go-design.md)

## Global Constraints

- **Errors: `github.com/pkg/errors` only.** `errors.Wrap`, `errors.Wrapf`, `errors.WithStack`, `errors.Cause`, `errors.Is`, `errors.As`. No `fmt.Errorf`, no `%w` wrapping anywhere in production code — `pkg/log/helpers.go` extracts `StackTrace()` and only works with these wrappers.
- **Exported sentinel errors live in `errors.go`, prefixed `Err`.**
- **Functional options are `func(s settings) settings`** — value in, value out. Options never return errors and never validate; validation runs in the constructor.
- **Constructors return an interface, with a private struct as the implementation.**
- **`ctx context.Context` is always the first parameter** on I/O APIs.
- **Everything in English** — symbols, comments, error messages, log messages.
- **Tests are black-box (`package x_test`) by default.** White-box only where private `settings` must be touched.
- **`testify/assert`, not `require`.** `assert.Nil(t, err)` is used as a synonym for `assert.NoError` throughout.
- **Mocks are hand-written** with `testify/mock`. No mockery, no `go:generate`. `mock_test.go` for package-internal mocks.
- **Table-driven tests use `wantErr assert.ErrorAssertionFunc`** with an inline closure, never `wantErr bool`.
- **Branch per task**, Conventional Commits in English, PR against `main` via `gh pr create`. `make test` and `make lint` green before every PR.
- **Target version: v0.3.0** (breaking).

---

### Task 1: Driver-neutral record types in `pkg/stream`

**Files:**
- Create: `pkg/stream/record.go`
- Test: `pkg/stream/record_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `stream.Record` struct with fields `Topic string`, `Partition int32`, `Offset int64`, `Key []byte`, `Value []byte`, `Headers []Header`, `Timestamp time.Time`; `stream.Header` struct with `Key string`, `Value []byte`; method `func (r Record) Header(key string) ([]byte, bool)`.

- [ ] **Step 1: Write the failing test**

```go
package stream_test

import (
	"testing"
	"time"

	"github.com/diegodesousas/go-devkit/pkg/stream"
	"github.com/stretchr/testify/assert"
)

func TestRecord_Header(t *testing.T) {
	record := stream.Record{
		Topic:     "orders",
		Partition: 3,
		Offset:    42,
		Key:       []byte("order-1"),
		Value:     []byte(`{"id":"1"}`),
		Headers: []stream.Header{
			{Key: "DEVKIT_CONTENT_TYPE", Value: []byte("json")},
			{Key: "x-trace", Value: []byte("abc")},
		},
		Timestamp: time.Unix(1700000000, 0),
	}

	tests := []struct {
		name      string
		key       string
		wantValue []byte
		wantFound bool
	}{
		{
			name:      "returns the value of an existing header",
			key:       "DEVKIT_CONTENT_TYPE",
			wantValue: []byte("json"),
			wantFound: true,
		},
		{
			name:      "finds a header that is not the first",
			key:       "x-trace",
			wantValue: []byte("abc"),
			wantFound: true,
		},
		{
			name:      "reports a missing header",
			key:       "absent",
			wantValue: nil,
			wantFound: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, found := record.Header(tt.key)

			assert.Equal(t, tt.wantFound, found)
			assert.Equal(t, tt.wantValue, value)
		})
	}
}

func TestRecord_Header_NoHeaders(t *testing.T) {
	record := stream.Record{Topic: "orders"}

	value, found := record.Header("DEVKIT_CONTENT_TYPE")

	assert.False(t, found)
	assert.Nil(t, value)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/stream/ -run TestRecord -v`
Expected: FAIL — `undefined: stream.Record`

- [ ] **Step 3: Write minimal implementation**

Create `pkg/stream/record.go`:

```go
package stream

import "time"

// Header is one key/value pair carried alongside a Record. Kafka does not
// interpret headers; this package uses them to name the payload encoding and
// to carry the trace context.
type Header struct {
	Key   string
	Value []byte
}

// Record is one message on the wire, independent of any Kafka driver.
//
// It is the type the Reader and Writer seams speak, which is what keeps the
// driver out of the public API: swapping the client library changes how a
// Record is built, not what callers see.
//
// When producing, Partition, Offset and Timestamp are left unset - the broker
// and the driver assign them.
type Record struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   []Header
	Timestamp time.Time
}

// Header returns the value of the first header with the given key, and whether
// such a header exists. A nil value and false mean the header is absent.
func (r Record) Header(key string) ([]byte, bool) {
	for _, header := range r.Headers {
		if header.Key == key {
			return header.Value, true
		}
	}

	return nil, false
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/stream/ -run TestRecord -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git checkout main && git pull
git checkout -b feat/stream-record-type
git add pkg/stream/record.go pkg/stream/record_test.go
git commit -m "feat: add driver-neutral Record type to pkg/stream"
```

---

### Task 2: `Reader` and `Writer` seams in `pkg/stream`

**Files:**
- Create: `pkg/stream/transport.go`
- Test: none of its own — interfaces are exercised by their implementations and by the consumer/dispatcher tests.

**Interfaces:**
- Consumes: `stream.Record` (Task 1).
- Produces: `stream.Reader` with `Poll(ctx context.Context) ([]Record, error)`, `Commit(ctx context.Context, records ...Record) error`, `Close() error`; `stream.Writer` with `Produce(ctx context.Context, record Record) error`, `Flush(ctx context.Context) error`, `Close() error`.

- [ ] **Step 1: Write the interfaces**

Create `pkg/stream/transport.go`:

```go
package stream

import "context"

// Reader consumes records from a topic as a member of a consumer group.
//
// Poll returns a batch because that is what the underlying clients deliver, and
// because a batch is what lets a consumer process partitions concurrently.
// An empty slice with a nil error means the poll timed out with nothing
// available, which is normal and not a failure.
//
// Commit acknowledges the given records. Implementations commit the highest
// offset per partition among them, so passing every processed record is
// correct and passing only the last one per partition is an optimisation.
//
// Offsets are never committed automatically: the caller decides when a record
// counts as processed.
type Reader interface {
	Poll(ctx context.Context) ([]Record, error)
	Commit(ctx context.Context, records ...Record) error
	Close() error
}

// Writer publishes records to a topic.
//
// Produce is synchronous: it returns once the broker has acknowledged the
// record or reported a failure, and honours cancellation of ctx.
//
// Flush waits for anything still buffered. Close releases the connection; call
// Flush first unless losing buffered records is acceptable.
type Writer interface {
	Produce(ctx context.Context, record Record) error
	Flush(ctx context.Context) error
	Close() error
}
```

- [ ] **Step 2: Verify it compiles**

Run: `go build ./pkg/stream/`
Expected: no output

- [ ] **Step 3: Commit**

```bash
git add pkg/stream/transport.go
git commit -m "feat: declare Reader and Writer transport seams"
```

---

### Task 3: `NewMessageType` takes a `Record`

**Files:**
- Modify: `pkg/stream/message.go` (the `NewMessageType` function and its imports)
- Modify: `pkg/stream/message_test.go` (every call site building a `*kafka.Message`)
- Modify: `pkg/stream/example_test.go` if it references `NewMessageType`

**Interfaces:**
- Consumes: `stream.Record`, `Record.Header` (Task 1).
- Produces: `func NewMessageType(record Record) (Message, error)` — same return contract as before, `ErrUnknownMessageType` when the header is absent or unrecognised.

- [ ] **Step 1: Write the failing test**

Replace the `TestNewMessageType` block in `pkg/stream/message_test.go` with:

```go
func TestNewMessageType(t *testing.T) {
	tests := []struct {
		name     string
		record   stream.Record
		wantType string
		wantErr  assert.ErrorAssertionFunc
	}{
		{
			name: "json content type",
			record: stream.Record{
				Value:   []byte(`{"id":"1"}`),
				Headers: []stream.Header{{Key: stream.ContentTypeHeaderKey, Value: []byte("json")}},
			},
			wantType: "json",
			wantErr: func(t assert.TestingT, err error, i ...any) bool {
				return assert.Nil(t, err)
			},
		},
		{
			name: "postgresql content type decodes as json",
			record: stream.Record{
				Value:   []byte(`{"id":"1"}`),
				Headers: []stream.Header{{Key: stream.ContentTypeHeaderKey, Value: []byte("postgresql")}},
			},
			wantType: "json",
			wantErr: func(t assert.TestingT, err error, i ...any) bool {
				return assert.Nil(t, err)
			},
		},
		{
			name: "text content type",
			record: stream.Record{
				Value:   []byte("plain"),
				Headers: []stream.Header{{Key: stream.ContentTypeHeaderKey, Value: []byte("text")}},
			},
			wantType: "text",
			wantErr: func(t assert.TestingT, err error, i ...any) bool {
				return assert.Nil(t, err)
			},
		},
		{
			name:   "no headers at all",
			record: stream.Record{Value: []byte("plain")},
			wantErr: func(t assert.TestingT, err error, i ...any) bool {
				return assert.ErrorIs(t, err, stream.ErrUnknownMessageType)
			},
		},
		{
			name: "unrecognised content type",
			record: stream.Record{
				Value:   []byte("plain"),
				Headers: []stream.Header{{Key: stream.ContentTypeHeaderKey, Value: []byte("avro")}},
			},
			wantErr: func(t assert.TestingT, err error, i ...any) bool {
				return assert.ErrorIs(t, err, stream.ErrUnknownMessageType)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			message, err := stream.NewMessageType(tt.record)

			if !tt.wantErr(t, err) {
				return
			}

			if err == nil {
				assert.Equal(t, tt.wantType, message.Type())
			}
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/stream/ -run TestNewMessageType -v`
Expected: FAIL — cannot use `stream.Record` as `*kafka.Message`

- [ ] **Step 3: Rewrite `NewMessageType`**

In `pkg/stream/message.go`, delete the `"github.com/confluentinc/confluent-kafka-go/kafka"` import and replace the function with:

```go
// NewMessageType selects the Message implementation matching the
// ContentTypeHeaderKey header of a record.
//
// It returns ErrUnknownMessageType when the header is absent or names an
// encoding this package does not implement - which is what makes the consumer
// route the record to the dead letter topic instead of guessing.
func NewMessageType(record Record) (Message, error) {
	contentType, ok := record.Header(ContentTypeHeaderKey)
	if !ok {
		return nil, ErrUnknownMessageType
	}

	switch string(contentType) {
	case jsonType, postgresType:
		return NewJSONMessage(record.Value), nil
	case textType:
		return NewTextMessage(string(record.Value)), nil
	}

	return nil, ErrUnknownMessageType
}
```

- [ ] **Step 4: Run the package tests**

Run: `go test ./pkg/stream/ -v`
Expected: PASS. If `example_test.go` references `NewMessageType`, update it to build a `stream.Record`.

- [ ] **Step 5: Verify confluent is gone from `pkg/stream`**

Run: `grep -rn 'confluent-kafka-go' pkg/stream/*.go`
Expected: no output

- [ ] **Step 6: Commit**

```bash
git add pkg/stream/
git commit -m "refactor!: NewMessageType takes a stream.Record"
```

---

### Task 4: franz-go writer in `pkg/stream/kafka`

**Files:**
- Create: `pkg/stream/kafka/options.go`
- Create: `pkg/stream/kafka/writer.go`
- Create: `pkg/stream/kafka/doc.go`
- Test: `pkg/stream/kafka/options_test.go`
- Modify: `go.mod` (add `github.com/twmb/franz-go`)

**Interfaces:**
- Consumes: `stream.Record`, `stream.Header`, `stream.Writer` (Tasks 1-2).
- Produces: `kafka.Option` (`func(s settings) settings`); options `WithBrokers(...string)`, `WithSASLPlain(user, pass string)`, `WithSASLSCRAM(user, pass string, m SCRAMMechanism)`, `WithTLS(*tls.Config)`, `WithClientID(string)`, `WithSessionTimeout(time.Duration)`, `WithStartOffset(StartOffset)`, `WithProduceTimeout(time.Duration)`; consts `SCRAMSHA256`, `SCRAMSHA512`, `StartEarliest`, `StartLatest`; `func NewWriter(opts ...Option) (stream.Writer, error)`; internal `func (s settings) clientOpts() ([]kgo.Opt, error)`.

- [ ] **Step 1: Add the dependency**

```bash
go get github.com/twmb/franz-go@v1.21.6
```

- [ ] **Step 2: Write the options file**

Create `pkg/stream/kafka/options.go`:

```go
package kafka

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

const (
	defaultSessionTimeout = 45 * time.Second

	// defaultProduceTimeout is how long a produced record may go
	// unacknowledged before delivery is reported as failed.
	//
	// The previous implementation used one second, against a librdkafka
	// default of five minutes, which turned any broker hiccup into a delivery
	// error. Thirty seconds rides out a leader election without hiding a
	// broker that is genuinely gone.
	defaultProduceTimeout = 30 * time.Second
)

// StartOffset is where a new consumer group begins reading.
type StartOffset int

const (
	// StartEarliest reads a topic from its oldest retained record.
	StartEarliest StartOffset = iota
	// StartLatest reads only records produced after the group joins.
	StartLatest
)

// SCRAMMechanism selects the hash used by SCRAM authentication.
type SCRAMMechanism int

const (
	SCRAMSHA256 SCRAMMechanism = iota
	SCRAMSHA512
)

// ErrUnknownSCRAMMechanism is returned when a SCRAMMechanism is not one of the
// constants declared by this package.
var ErrUnknownSCRAMMechanism = errors.New("kafka: unknown SCRAM mechanism")

type settings struct {
	brokers        []string
	clientID       string
	sasl           sasl.Mechanism
	saslErr        error
	tls            *tls.Config
	sessionTimeout time.Duration
	startOffset    StartOffset
	produceTimeout time.Duration
}

func defaultSettings() settings {
	return settings{
		sessionTimeout: defaultSessionTimeout,
		startOffset:    StartEarliest,
		produceTimeout: defaultProduceTimeout,
	}
}

// Option configures a Reader or a Writer. The same options serve both, so
// broker, authentication and TLS settings are written once for an application.
type Option func(s settings) settings

// WithBrokers sets the seed brokers, as "host:port" strings.
func WithBrokers(brokers ...string) Option {
	return func(s settings) settings {
		s.brokers = append(s.brokers, brokers...)

		return s
	}
}

// WithClientID sets the client id reported to the broker, which is what shows
// up in broker-side metrics and quotas.
func WithClientID(id string) Option {
	return func(s settings) settings {
		s.clientID = id

		return s
	}
}

// WithSASLPlain authenticates with SASL/PLAIN. Use it only over TLS - PLAIN
// sends the password unencrypted.
func WithSASLPlain(user, pass string) Option {
	return func(s settings) settings {
		s.sasl = plain.Plain(func(context.Context) (plain.Auth, error) {
			return plain.Auth{User: user, Pass: pass}, nil
		})

		return s
	}
}

// WithSASLSCRAM authenticates with SASL/SCRAM, which is what Confluent Cloud
// and MSK expect.
func WithSASLSCRAM(user, pass string, mechanism SCRAMMechanism) Option {
	return func(s settings) settings {
		auth := func(context.Context) (scram.Auth, error) {
			return scram.Auth{User: user, Pass: pass}, nil
		}

		switch mechanism {
		case SCRAMSHA256:
			s.sasl = scram.Sha256(auth)
		case SCRAMSHA512:
			s.sasl = scram.Sha512(auth)
		default:
			// Options never return errors, so the failure is carried to the
			// constructor, which is where this package validates.
			s.saslErr = errors.Wrapf(ErrUnknownSCRAMMechanism, "%d", mechanism)
		}

		return s
	}
}

// WithTLS dials the brokers over TLS with the given configuration.
func WithTLS(cfg *tls.Config) Option {
	return func(s settings) settings {
		s.tls = cfg

		return s
	}
}

// WithSessionTimeout sets how long the broker waits for a heartbeat before
// evicting this member from its group. Defaults to 45s.
func WithSessionTimeout(d time.Duration) Option {
	return func(s settings) settings {
		s.sessionTimeout = d

		return s
	}
}

// WithStartOffset sets where a group with no committed offsets begins.
// Defaults to StartEarliest.
func WithStartOffset(o StartOffset) Option {
	return func(s settings) settings {
		s.startOffset = o

		return s
	}
}

// WithProduceTimeout sets how long a produced record may go unacknowledged
// before Produce reports a failure. Defaults to 30s.
func WithProduceTimeout(d time.Duration) Option {
	return func(s settings) settings {
		s.produceTimeout = d

		return s
	}
}

// clientOpts translates the settings into franz-go options shared by readers
// and writers.
func (s settings) clientOpts() ([]kgo.Opt, error) {
	if s.saslErr != nil {
		return nil, s.saslErr
	}

	if len(s.brokers) == 0 {
		return nil, ErrNoBrokers
	}

	opts := []kgo.Opt{kgo.SeedBrokers(s.brokers...)}

	if s.clientID != "" {
		opts = append(opts, kgo.ClientID(s.clientID))
	}

	if s.sasl != nil {
		opts = append(opts, kgo.SASL(s.sasl))
	}

	if s.tls != nil {
		opts = append(opts, kgo.DialTLSConfig(s.tls))
	}

	return opts, nil
}

func (s settings) startAt() kgo.Offset {
	if s.startOffset == StartLatest {
		return kgo.NewOffset().AtEnd()
	}

	return kgo.NewOffset().AtStart()
}
```

- [ ] **Step 3: Write the errors file**

Create `pkg/stream/kafka/errors.go`:

```go
package kafka

import "github.com/pkg/errors"

// Errors returned when a client cannot be built.
var (
	// ErrNoBrokers means WithBrokers was never called, or was called with an
	// empty list. There is no default broker: guessing localhost would turn a
	// misconfiguration into a connection that silently goes nowhere.
	ErrNoBrokers = errors.New("kafka: at least one broker is required")

	// ErrNoTopics means NewReader was called with an empty topic list.
	ErrNoTopics = errors.New("kafka: at least one topic is required")

	// ErrNoGroupID means NewReader was called with an empty group id.
	ErrNoGroupID = errors.New("kafka: a consumer group id is required")
)
```

- [ ] **Step 4: Write the failing options test**

Create `pkg/stream/kafka/options_test.go` (white-box: it inspects private settings):

```go
package kafka

import (
	"crypto/tls"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSettings_Defaults(t *testing.T) {
	s := defaultSettings()

	assert.Equal(t, 45*time.Second, s.sessionTimeout)
	assert.Equal(t, 30*time.Second, s.produceTimeout)
	assert.Equal(t, StartEarliest, s.startOffset)
	assert.Nil(t, s.sasl)
	assert.Nil(t, s.tls)
}

func TestSettings_ClientOpts(t *testing.T) {
	tests := []struct {
		name     string
		options  []Option
		wantOpts int
		wantErr  assert.ErrorAssertionFunc
	}{
		{
			name:     "brokers only",
			options:  []Option{WithBrokers("localhost:9092")},
			wantOpts: 1,
			wantErr: func(t assert.TestingT, err error, i ...any) bool {
				return assert.Nil(t, err)
			},
		},
		{
			name: "brokers, client id, sasl and tls",
			options: []Option{
				WithBrokers("a:9092", "b:9092"),
				WithClientID("billing"),
				WithSASLSCRAM("user", "pass", SCRAMSHA512),
				WithTLS(&tls.Config{MinVersion: tls.VersionTLS12}),
			},
			wantOpts: 4,
			wantErr: func(t assert.TestingT, err error, i ...any) bool {
				return assert.Nil(t, err)
			},
		},
		{
			name:    "no brokers",
			options: []Option{WithClientID("billing")},
			wantErr: func(t assert.TestingT, err error, i ...any) bool {
				return assert.ErrorIs(t, err, ErrNoBrokers)
			},
		},
		{
			name: "unknown scram mechanism",
			options: []Option{
				WithBrokers("localhost:9092"),
				WithSASLSCRAM("user", "pass", SCRAMMechanism(99)),
			},
			wantErr: func(t assert.TestingT, err error, i ...any) bool {
				return assert.ErrorIs(t, err, ErrUnknownSCRAMMechanism)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := defaultSettings()
			for _, option := range tt.options {
				s = option(s)
			}

			opts, err := s.clientOpts()

			if !tt.wantErr(t, err) {
				return
			}

			if err == nil {
				assert.Len(t, opts, tt.wantOpts)
			}
		})
	}
}

func TestSettings_StartAt(t *testing.T) {
	earliest := defaultSettings()
	latest := WithStartOffset(StartLatest)(defaultSettings())

	assert.NotEqual(t, earliest.startAt().String(), latest.startAt().String())
}
```

- [ ] **Step 5: Run the test**

Run: `go test ./pkg/stream/kafka/ -v`
Expected: PASS (options.go and errors.go were written in steps 2 and 3)

- [ ] **Step 6: Write the writer**

Create `pkg/stream/kafka/writer.go`:

```go
package kafka

import (
	"context"

	"github.com/diegodesousas/go-devkit/pkg/stream"
	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kgo"
)

type writer struct {
	client *kgo.Client
}

// NewWriter builds a Kafka producer.
//
// The producer is configured for ordering and durability over throughput: it
// waits for all in-sync replicas and keeps a single request in flight per
// broker, so a retry cannot reorder records within a partition.
func NewWriter(opts ...Option) (stream.Writer, error) {
	s := defaultSettings()
	for _, opt := range opts {
		s = opt(s)
	}

	clientOpts, err := s.clientOpts()
	if err != nil {
		return nil, err
	}

	clientOpts = append(clientOpts,
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.MaxProduceRequestsInflightPerBroker(1),
		kgo.RecordDeliveryTimeout(s.produceTimeout),
	)

	client, err := kgo.NewClient(clientOpts...)
	if err != nil {
		return nil, errors.Wrap(err, "kafka: building producer")
	}

	return &writer{client: client}, nil
}

func (w *writer) Produce(ctx context.Context, record stream.Record) error {
	results := w.client.ProduceSync(ctx, toKgoRecord(record))

	if err := results.FirstErr(); err != nil {
		return errors.Wrap(err, "kafka: producing record")
	}

	return nil
}

func (w *writer) Flush(ctx context.Context) error {
	if err := w.client.Flush(ctx); err != nil {
		return errors.Wrap(err, "kafka: flushing producer")
	}

	return nil
}

func (w *writer) Close() error {
	w.client.Close()

	return nil
}

func toKgoRecord(record stream.Record) *kgo.Record {
	headers := make([]kgo.RecordHeader, 0, len(record.Headers))
	for _, header := range record.Headers {
		headers = append(headers, kgo.RecordHeader{Key: header.Key, Value: header.Value})
	}

	return &kgo.Record{
		Topic:   record.Topic,
		Key:     record.Key,
		Value:   record.Value,
		Headers: headers,
	}
}
```

- [ ] **Step 7: Write the package doc**

Create `pkg/stream/kafka/doc.go`:

```go
// Package kafka implements the stream transport seams on top of franz-go.
//
// It is the only package in this repository that imports a Kafka client
// library. Everything driver-specific - connection, authentication, TLS,
// timeouts, offset handling - lives here, and pkg/stream/consumer and
// pkg/stream/dispatcher depend only on stream.Reader and stream.Writer. That
// boundary is what makes a future driver change a rewrite of one package
// rather than a break in the public API.
//
// A reader joins a consumer group; a writer produces:
//
//	reader, err := kafka.NewReader("devkit-billing", []string{"orders"},
//		kafka.WithBrokers("localhost:9092"),
//	)
//
//	writer, err := kafka.NewWriter(kafka.WithBrokers("localhost:9092"))
//
// The same options configure both, so brokers, SASL and TLS are declared once.
//
// The group id is a positional parameter rather than an option because there
// is no safe default. A group with no committed offsets starts from the
// beginning of the topic, so inheriting a silently different group id would
// reprocess everything.
//
// Auto-commit is disabled. Offsets are committed by pkg/stream/consumer once a
// handler has succeeded, which is what keeps at-least-once delivery honest.
package kafka
```

- [ ] **Step 8: Build and lint**

Run: `go build ./... && make lint`
Expected: `0 issues`

- [ ] **Step 9: Commit**

```bash
git add pkg/stream/kafka/ go.mod go.sum
git commit -m "feat: add franz-go writer and shared broker options"
```

---

### Task 5: franz-go reader in `pkg/stream/kafka`

**Files:**
- Create: `pkg/stream/kafka/reader.go`
- Test: `pkg/stream/kafka/reader_test.go`

**Interfaces:**
- Consumes: `settings`, `clientOpts`, `startAt`, `ErrNoTopics`, `ErrNoGroupID` (Task 4); `stream.Record`, `stream.Reader` (Tasks 1-2).
- Produces: `func NewReader(groupID string, topics []string, opts ...Option) (stream.Reader, error)`; internal `func fromKgoRecord(*kgo.Record) stream.Record`.

- [ ] **Step 1: Write the failing test**

Create `pkg/stream/kafka/reader_test.go` (white-box, to reach `fromKgoRecord`):

```go
package kafka

import (
	"testing"
	"time"

	"github.com/diegodesousas/go-devkit/pkg/stream"
	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestFromKgoRecord(t *testing.T) {
	timestamp := time.Unix(1700000000, 0)

	kgoRecord := &kgo.Record{
		Topic:     "orders",
		Partition: 3,
		Offset:    42,
		Key:       []byte("order-1"),
		Value:     []byte(`{"id":"1"}`),
		Timestamp: timestamp,
		Headers: []kgo.RecordHeader{
			{Key: stream.ContentTypeHeaderKey, Value: []byte("json")},
		},
	}

	record := fromKgoRecord(kgoRecord)

	assert.Equal(t, "orders", record.Topic)
	assert.Equal(t, int32(3), record.Partition)
	assert.Equal(t, int64(42), record.Offset)
	assert.Equal(t, []byte("order-1"), record.Key)
	assert.Equal(t, []byte(`{"id":"1"}`), record.Value)
	assert.Equal(t, timestamp, record.Timestamp)

	contentType, ok := record.Header(stream.ContentTypeHeaderKey)
	assert.True(t, ok)
	assert.Equal(t, []byte("json"), contentType)
}

func TestNewReader_Validation(t *testing.T) {
	tests := []struct {
		name    string
		groupID string
		topics  []string
		options []Option
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name:    "empty group id",
			groupID: "",
			topics:  []string{"orders"},
			options: []Option{WithBrokers("localhost:9092")},
			wantErr: func(t assert.TestingT, err error, i ...any) bool {
				return assert.ErrorIs(t, err, ErrNoGroupID)
			},
		},
		{
			name:    "no topics",
			groupID: "billing",
			topics:  nil,
			options: []Option{WithBrokers("localhost:9092")},
			wantErr: func(t assert.TestingT, err error, i ...any) bool {
				return assert.ErrorIs(t, err, ErrNoTopics)
			},
		},
		{
			name:    "no brokers",
			groupID: "billing",
			topics:  []string{"orders"},
			options: nil,
			wantErr: func(t assert.TestingT, err error, i ...any) bool {
				return assert.ErrorIs(t, err, ErrNoBrokers)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader, err := NewReader(tt.groupID, tt.topics, tt.options...)

			assert.Nil(t, reader)
			tt.wantErr(t, err)
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/stream/kafka/ -run 'TestFromKgoRecord|TestNewReader' -v`
Expected: FAIL — `undefined: fromKgoRecord`, `undefined: NewReader`

- [ ] **Step 3: Write the reader**

Create `pkg/stream/kafka/reader.go`:

```go
package kafka

import (
	"context"

	"github.com/diegodesousas/go-devkit/pkg/stream"
	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kgo"
)

// maxPollRecords caps how many records one Poll returns. It bounds the work a
// consumer takes on before committing, which bounds how much is reprocessed
// after a crash.
const maxPollRecords = 500

type reader struct {
	client *kgo.Client
}

// NewReader joins groupID and consumes topics.
//
// groupID is a positional parameter, not an option, because there is no safe
// default: a group with no committed offsets starts at the beginning of the
// topic, so a silently inherited id would reprocess everything. Naming it is
// deliberate.
//
// Auto-commit is disabled. Offsets advance only when the caller commits.
func NewReader(groupID string, topics []string, opts ...Option) (stream.Reader, error) {
	if groupID == "" {
		return nil, ErrNoGroupID
	}

	if len(topics) == 0 {
		return nil, ErrNoTopics
	}

	s := defaultSettings()
	for _, opt := range opts {
		s = opt(s)
	}

	clientOpts, err := s.clientOpts()
	if err != nil {
		return nil, err
	}

	clientOpts = append(clientOpts,
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
		kgo.SessionTimeout(s.sessionTimeout),
		kgo.ConsumeResetOffset(s.startAt()),
	)

	client, err := kgo.NewClient(clientOpts...)
	if err != nil {
		return nil, errors.Wrap(err, "kafka: building consumer")
	}

	return &reader{client: client}, nil
}

// Poll waits for records. A cancelled context yields that context's error
// rather than a partial batch.
func (r *reader) Poll(ctx context.Context) ([]stream.Record, error) {
	fetches := r.client.PollRecords(ctx, maxPollRecords)

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if fetches.IsClientClosed() {
		return nil, errors.WithStack(stream.ErrReaderClosed)
	}

	if err := fetches.Err(); err != nil {
		return nil, errors.Wrap(err, "kafka: polling records")
	}

	kgoRecords := fetches.Records()

	records := make([]stream.Record, 0, len(kgoRecords))
	for _, kgoRecord := range kgoRecords {
		records = append(records, fromKgoRecord(kgoRecord))
	}

	return records, nil
}

// Commit acknowledges records. franz-go commits the highest offset per
// partition among them, so passing every processed record is correct.
func (r *reader) Commit(ctx context.Context, records ...stream.Record) error {
	if len(records) == 0 {
		return nil
	}

	kgoRecords := make([]*kgo.Record, 0, len(records))
	for _, record := range records {
		kgoRecords = append(kgoRecords, &kgo.Record{
			Topic:     record.Topic,
			Partition: record.Partition,
			Offset:    record.Offset,
		})
	}

	if err := r.client.CommitRecords(ctx, kgoRecords...); err != nil {
		return errors.Wrap(err, "kafka: committing records")
	}

	return nil
}

func (r *reader) Close() error {
	r.client.Close()

	return nil
}

func fromKgoRecord(kgoRecord *kgo.Record) stream.Record {
	headers := make([]stream.Header, 0, len(kgoRecord.Headers))
	for _, header := range kgoRecord.Headers {
		headers = append(headers, stream.Header{Key: header.Key, Value: header.Value})
	}

	return stream.Record{
		Topic:     kgoRecord.Topic,
		Partition: kgoRecord.Partition,
		Offset:    kgoRecord.Offset,
		Key:       kgoRecord.Key,
		Value:     kgoRecord.Value,
		Headers:   headers,
		Timestamp: kgoRecord.Timestamp,
	}
}
```

- [ ] **Step 4: Add the sentinel the reader references**

Append to `pkg/stream/errors.go`:

```go
	// ErrReaderClosed is returned by Reader.Poll after the reader has been
	// closed. It ends a consumer loop without being treated as a broker
	// failure.
	ErrReaderClosed = errors.New("stream: reader is closed")
```

- [ ] **Step 5: Run the tests**

Run: `go test ./pkg/stream/... -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add pkg/stream/
git commit -m "feat: add franz-go reader"
```

---

### Task 6: Dispatcher on `stream.Writer`, with trace propagation

**Files:**
- Rewrite: `pkg/stream/dispatcher/dispatcher.go`
- Delete: `pkg/stream/dispatcher/dispatcher_client.go`, `pkg/stream/dispatcher/dispatcher_client_test.go`
- Rewrite: `pkg/stream/dispatcher/mock_test.go`, `pkg/stream/dispatcher/dispatcher_test.go`, `pkg/stream/dispatcher/example_test.go`
- Modify: `pkg/stream/dispatcher/doc.go`

**Interfaces:**
- Consumes: `stream.Writer`, `stream.Record`, `stream.Header`, `stream.Message`, `stream.ContentTypeHeaderKey` (Tasks 1-3).
- Produces: `dispatcher.Dispatcher` with `Dispatch(ctx context.Context, topic, key string, content stream.Message) error` and `Close(ctx context.Context) error`; `func New(w stream.Writer, opts ...Option) Dispatcher`; `func WithFlushTimeout(time.Duration) Option`.

- [ ] **Step 1: Write the writer mock**

Replace `pkg/stream/dispatcher/mock_test.go`:

```go
package dispatcher_test

import (
	"context"

	"github.com/diegodesousas/go-devkit/pkg/stream"
	"github.com/stretchr/testify/mock"
)

type writerMock struct {
	mock.Mock
}

func (w *writerMock) Produce(ctx context.Context, record stream.Record) error {
	args := w.Called(ctx, record)

	return args.Error(0)
}

func (w *writerMock) Flush(ctx context.Context) error {
	args := w.Called(ctx)

	return args.Error(0)
}

func (w *writerMock) Close() error {
	args := w.Called()

	return args.Error(0)
}
```

- [ ] **Step 2: Write the failing test**

Replace `pkg/stream/dispatcher/dispatcher_test.go` with these cases (keep the file black-box, `package dispatcher_test`):

```go
package dispatcher_test

import (
	"context"
	"testing"

	"github.com/diegodesousas/go-devkit/pkg/stream"
	"github.com/diegodesousas/go-devkit/pkg/stream/dispatcher"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestDispatch_Success(t *testing.T) {
	var (
		expectedTopic = "orders"
		expectedKey   = "order-1"
	)

	writer := &writerMock{}
	writer.
		On("Produce", mock.Anything, mock.MatchedBy(func(record stream.Record) bool {
			contentType, ok := record.Header(stream.ContentTypeHeaderKey)

			return record.Topic == expectedTopic &&
				string(record.Key) == expectedKey &&
				ok && string(contentType) == "json"
		})).
		Return(nil).
		Once()

	d := dispatcher.New(writer)

	err := d.Dispatch(context.Background(), expectedTopic, expectedKey, stream.NewJSONMessage(map[string]string{"id": "1"}))

	assert.Nil(t, err)
	writer.AssertExpectations(t)
}

func TestDispatch_SerializeError(t *testing.T) {
	writer := &writerMock{}

	d := dispatcher.New(writer)

	// A channel cannot be marshalled to JSON.
	err := d.Dispatch(context.Background(), "orders", "key", stream.NewJSONMessage(make(chan int)))

	assert.NotNil(t, err)
	writer.AssertNotCalled(t, "Produce", mock.Anything, mock.Anything)
}

func TestDispatch_ProduceError(t *testing.T) {
	expectedErr := errors.New("broker down")

	writer := &writerMock{}
	writer.On("Produce", mock.Anything, mock.Anything).Return(expectedErr).Once()

	d := dispatcher.New(writer)

	err := d.Dispatch(context.Background(), "orders", "key", stream.NewJSONMessage(map[string]string{}))

	assert.ErrorIs(t, err, expectedErr)
	writer.AssertExpectations(t)
}

func TestDispatch_InjectsTraceHeaders(t *testing.T) {
	var captured stream.Record

	writer := &writerMock{}
	writer.
		On("Produce", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			captured = args.Get(1).(stream.Record)
		}).
		Return(nil).
		Once()

	d := dispatcher.New(writer)

	err := d.Dispatch(context.Background(), "orders", "key", stream.NewJSONMessage(map[string]string{}))

	assert.Nil(t, err)
	// Beyond the content type, the span context must ride along so the
	// consumer can continue the trace.
	assert.Greater(t, len(captured.Headers), 1)
}

func TestClose(t *testing.T) {
	writer := &writerMock{}
	writer.On("Flush", mock.Anything).Return(nil).Once()
	writer.On("Close").Return(nil).Once()

	d := dispatcher.New(writer)

	assert.Nil(t, d.Close(context.Background()))
	writer.AssertExpectations(t)
}

func TestClose_FlushError(t *testing.T) {
	expectedErr := errors.New("flush failed")

	writer := &writerMock{}
	writer.On("Flush", mock.Anything).Return(expectedErr).Once()
	writer.On("Close").Return(nil).Once()

	d := dispatcher.New(writer)

	err := d.Close(context.Background())

	// Close still closes the writer, but reports what was lost.
	assert.ErrorIs(t, err, expectedErr)
	writer.AssertExpectations(t)
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./pkg/stream/dispatcher/ -v`
Expected: FAIL — `dispatcher.New` signature mismatch, `Close` undefined

- [ ] **Step 4: Rewrite the dispatcher**

Replace `pkg/stream/dispatcher/dispatcher.go`:

```go
package dispatcher

import (
	"context"
	"time"

	"github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"
	"github.com/diegodesousas/go-devkit/pkg/stream"
	"github.com/pkg/errors"
)

const defaultFlushTimeout = 5 * time.Second

// Dispatcher publishes messages to a topic.
type Dispatcher interface {
	Dispatch(ctx context.Context, topic, key string, content stream.Message) error
	Close(ctx context.Context) error
}

type settings struct {
	flushTimeout time.Duration
}

// Option configures a Dispatcher.
type Option func(s settings) settings

// WithFlushTimeout caps how long Close waits for buffered records. Defaults to
// 5s.
func WithFlushTimeout(d time.Duration) Option {
	return func(s settings) settings {
		s.flushTimeout = d

		return s
	}
}

type dispatcher struct {
	writer       stream.Writer
	flushTimeout time.Duration
}

// New returns a Dispatcher publishing through w.
func New(w stream.Writer, opts ...Option) Dispatcher {
	s := settings{flushTimeout: defaultFlushTimeout}
	for _, opt := range opts {
		s = opt(s)
	}

	return &dispatcher{
		writer:       w,
		flushTimeout: s.flushTimeout,
	}
}

// Dispatch serializes content and publishes it, blocking until the broker
// acknowledges the record.
//
// The span context is injected into the record headers so the consumer can
// continue the trace instead of starting a disconnected one.
func (d *dispatcher) Dispatch(ctx context.Context, topic, key string, content stream.Message) error {
	span, ctx := tracer.StartSpanFromContext(ctx, "stream.dispatcher")
	defer span.Finish()

	span.SetTag("topic", topic)
	span.SetTag("key", key)

	payload, err := content.Serialize()
	if err != nil {
		return errors.Wrap(err, "dispatcher: serializing message")
	}

	headers := []stream.Header{
		{Key: stream.ContentTypeHeaderKey, Value: []byte(content.Type())},
	}

	carrier := stream.HeaderCarrier{}
	if err := tracer.Inject(span.Context(), carrier); err == nil {
		headers = append(headers, carrier.Headers()...)
	}

	record := stream.Record{
		Topic:   topic,
		Key:     []byte(key),
		Value:   payload,
		Headers: headers,
	}

	if err := d.writer.Produce(ctx, record); err != nil {
		return errors.Wrap(err, "dispatcher: producing record")
	}

	return nil
}

// Close flushes buffered records and releases the writer.
//
// It reports a failed flush instead of discarding it: records still buffered
// when the process exits are lost, and that must not be silent.
func (d *dispatcher) Close(ctx context.Context) error {
	flushCtx, cancel := context.WithTimeout(ctx, d.flushTimeout)
	defer cancel()

	flushErr := d.writer.Flush(flushCtx)
	closeErr := d.writer.Close()

	if flushErr != nil {
		return errors.Wrap(flushErr, "dispatcher: flushing on close")
	}

	if closeErr != nil {
		return errors.Wrap(closeErr, "dispatcher: closing writer")
	}

	return nil
}
```

- [ ] **Step 5: Add the trace carrier to `pkg/stream`**

Create `pkg/stream/trace.go`:

```go
package stream

// HeaderCarrier adapts a set of record headers to the TextMapWriter and
// TextMapReader interfaces the tracer uses, so a span context can ride along
// with a message and the consumer can pick the trace back up.
type HeaderCarrier map[string]string

// Set records one trace key. It satisfies tracer.TextMapWriter.
func (c HeaderCarrier) Set(key, val string) {
	c[key] = val
}

// ForeachKey iterates the carrier. It satisfies tracer.TextMapReader.
func (c HeaderCarrier) ForeachKey(handler func(key, val string) error) error {
	for key, val := range c {
		if err := handler(key, val); err != nil {
			return err
		}
	}

	return nil
}

// Headers converts the carrier into record headers.
func (c HeaderCarrier) Headers() []Header {
	headers := make([]Header, 0, len(c))
	for key, val := range c {
		headers = append(headers, Header{Key: key, Value: []byte(val)})
	}

	return headers
}

// NewHeaderCarrier builds a carrier from the headers of a consumed record, for
// extracting the span context the producer injected.
func NewHeaderCarrier(headers []Header) HeaderCarrier {
	carrier := make(HeaderCarrier, len(headers))
	for _, header := range headers {
		carrier[header.Key] = string(header.Value)
	}

	return carrier
}
```

- [ ] **Step 6: Delete the obsolete client**

```bash
git rm pkg/stream/dispatcher/dispatcher_client.go pkg/stream/dispatcher/dispatcher_client_test.go
```

- [ ] **Step 7: Update the dispatcher doc and example**

Replace the usage block in `pkg/stream/dispatcher/doc.go` and the body of `ExampleNew` in `example_test.go` with:

```go
writer, err := kafka.NewWriter(kafka.WithBrokers("localhost:9092"))
if err != nil {
	panic(err)
}

d := dispatcher.New(writer)
defer func() { _ = d.Close(context.Background()) }()

order := orderPlaced{ID: "42", Total: 250}

err = d.Dispatch(context.Background(), "orders", order.ID, stream.NewJSONMessage(order))
if err != nil {
	panic(err)
}
```

Delete `ExampleDispatcher_Dispatch_timeout`'s `dispatcher.WithDispatchTimeoutMs` call — that option moved to `kafka.WithProduceTimeout`. The examples need no `// Output:` comment; they require a broker.

Also update the `doc.go` prose: the paragraph describing `Shutdown` must now describe `Close(ctx) error`, and the sentence saying the span "does not currently propagate to the consumer" is no longer true — it does, via record headers.

- [ ] **Step 8: Run the tests**

Run: `go test ./pkg/stream/... -v && make lint`
Expected: PASS, `0 issues`

- [ ] **Step 9: Commit**

```bash
git add pkg/stream/
git commit -m "refactor!: dispatcher writes through stream.Writer and propagates trace"
```

---

### Task 7: Consumer handler contract and options

**Files:**
- Rewrite: `pkg/stream/consumer/handler.go`
- Create: `pkg/stream/consumer/errors.go`
- Rewrite: `pkg/stream/consumer/consumer_option.go`
- Rewrite: `pkg/stream/consumer/handler_test.go`
- Delete: `pkg/stream/consumer/client.go`, `pkg/stream/consumer/client_option.go`, `pkg/stream/consumer/client_test.go`, `pkg/stream/consumer/client_option_test.go`

**Interfaces:**
- Consumes: `log.Logger`, `gen.StringGenerator`.
- Produces: `Handler[T]` with the single method `Handle(ctx context.Context, content T) error`; `ConfigRetry` struct (unchanged fields `RetryableErrors []error`, `InitialInterval`, `MaxElapsedTime`, `MaxInterval time.Duration`); `NewConfigRetry(...ConfigRetryOption) ConfigRetry` and its four options (unchanged); `Option func(s settings) settings`; `WithSkip[T]`, `WithRetry`, `WithDeadLetterTopic`, `WithLogger`, `WithStringGenerator`; `ErrDeadLetterUnavailable`.

- [ ] **Step 1: Write the handler file**

Replace `pkg/stream/consumer/handler.go` — keep `ConfigRetry`, `ConfigRetryOption`, `NewConfigRetry`, `WithRetryableErrors`, `WithInitialInterval`, `WithMaxElapsedTime`, `WithMaxInterval` exactly as they are today, and replace only the `Handler` interface with:

```go
// Handler is the user code a Consumer drives.
//
// It has one method. Everything the previous contract demanded - the group id,
// the topic, whether to skip, the retry policy - moved to where it belongs:
// the group and topic are parameters of the reader, which is what actually
// subscribes, and skipping and retrying are policy, configured with options.
//
// Handle receives a context carrying a logger scoped to the record and the
// trace continued from the producer. The context is cancelled when the
// consumer shuts down, so a long handler should honour it.
//
// Returning nil marks the record processed and allows its offset to be
// committed.
type Handler[T any] interface {
	Handle(ctx context.Context, content T) error
}
```

- [ ] **Step 2: Write the errors file**

Create `pkg/stream/consumer/errors.go`:

```go
package consumer

import "github.com/pkg/errors"

var (
	// ErrDeadLetterUnavailable means a record could not be processed and could
	// not be published to the dead letter topic either.
	//
	// The consumer stops committing that partition and leaves the record
	// uncommitted, so nothing is lost and the partition resumes once the dead
	// letter topic accepts writes again. Other partitions keep going.
	ErrDeadLetterUnavailable = errors.New("consumer: dead letter topic unavailable")
)
```

- [ ] **Step 3: Write the options file**

Replace `pkg/stream/consumer/consumer_option.go`:

```go
package consumer

import (
	"github.com/diegodesousas/go-devkit/pkg/gen"
	"github.com/diegodesousas/go-devkit/pkg/log"
)

// deadLetterSuffix is appended to the source topic when no dead letter topic
// is configured.
const deadLetterSuffix = "dlt"

type settings[T any] struct {
	logger          log.Logger
	stringGenerator gen.StringGenerator
	skip            func(T) bool
	retry           ConfigRetry
	deadLetterTopic string
}

// Option configures a Consumer built by New.
type Option[T any] func(s settings[T]) settings[T]

// WithLogger sets the base logger. The Consumer derives from it per record,
// adding the topic, partition, offset and a trace id. Defaults to log.New().
func WithLogger[T any](logger log.Logger) Option[T] {
	return func(s settings[T]) settings[T] {
		s.logger = logger

		return s
	}
}

// WithStringGenerator sets the source of per-record trace ids. Defaults to
// gen.UUIDGenerator; inject gen.SequenceGenerator in tests.
func WithStringGenerator[T any](generator gen.StringGenerator) Option[T] {
	return func(s settings[T]) settings[T] {
		s.stringGenerator = generator

		return s
	}
}

// WithSkip installs a predicate consulted before the handler. Returning true
// commits the record without processing it.
func WithSkip[T any](skip func(T) bool) Option[T] {
	return func(s settings[T]) settings[T] {
		s.skip = skip

		return s
	}
}

// WithRetry sets the retry policy. Without it, no error is retried and the
// first failure sends the record to the dead letter topic.
func WithRetry[T any](retry ConfigRetry) Option[T] {
	return func(s settings[T]) settings[T] {
		s.retry = retry

		return s
	}
}

// WithDeadLetterTopic overrides the dead letter topic name. Defaults to the
// source topic of the record suffixed with "-dlt".
func WithDeadLetterTopic[T any](topic string) Option[T] {
	return func(s settings[T]) settings[T] {
		s.deadLetterTopic = topic

		return s
	}
}
```

- [ ] **Step 4: Delete the obsolete client files**

```bash
git rm pkg/stream/consumer/client.go pkg/stream/consumer/client_option.go \
       pkg/stream/consumer/client_test.go pkg/stream/consumer/client_option_test.go
```

- [ ] **Step 5: Trim the handler test**

In `pkg/stream/consumer/handler_test.go`, keep only the `NewConfigRetry` cases. Delete any test asserting on `Handler.ID`, `Handler.Topic` or `Handler.ShouldSkip`.

- [ ] **Step 6: Verify it compiles**

Run: `go vet ./pkg/stream/consumer/ 2>&1 | head`
Expected: errors only from `consumer.go`, which Task 8 rewrites.

- [ ] **Step 7: Commit**

```bash
git add pkg/stream/consumer/
git commit -m "refactor!: shrink Handler to one method and move policy to options"
```

---

### Task 8: Consumer loop — per-partition concurrency, cancellation, dead-letter policy

**Files:**
- Rewrite: `pkg/stream/consumer/consumer.go`
- Test: `pkg/stream/consumer/consumer_test.go` (rewritten in Task 9)

**Interfaces:**
- Consumes: `stream.Reader`, `stream.Record`, `stream.NewMessageType`, `stream.NewTextMessage`, `stream.NewHeaderCarrier` (Tasks 1-6); `dispatcher.Dispatcher` (Task 6); `Handler[T]`, `settings[T]`, `Option[T]`, `ConfigRetry`, `ErrDeadLetterUnavailable`, `deadLetterSuffix` (Task 7).
- Produces: `Consumer` interface with `Run(ctx context.Context) error`; `func New[T any](reader stream.Reader, dlt dispatcher.Dispatcher, handler Handler[T], opts ...Option[T]) (Consumer, error)`.

- [ ] **Step 1: Write the consumer**

Replace `pkg/stream/consumer/consumer.go`:

```go
package consumer

import (
	"context"
	"fmt"

	"github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"
	"github.com/cenkalti/backoff/v5"
	"github.com/diegodesousas/go-devkit/pkg/gen"
	"github.com/diegodesousas/go-devkit/pkg/log"
	"github.com/diegodesousas/go-devkit/pkg/stream"
	"github.com/diegodesousas/go-devkit/pkg/stream/dispatcher"
	"github.com/pkg/errors"
)

const loggerTraceKey = "trace-id"

// Consumer runs a Kafka consumer loop.
type Consumer interface {
	Run(ctx context.Context) error
}

type defaultConsumer[T any] struct {
	reader          stream.Reader
	dlt             dispatcher.Dispatcher
	handler         Handler[T]
	logger          log.Logger
	stringGenerator gen.StringGenerator
	skip            func(T) bool
	retry           ConfigRetry
	deadLetterTopic string
}

// New builds a Consumer decoding records into T.
//
// reader supplies the records and owns the group membership; dlt publishes to
// the dead letter topic, and is therefore required even when the handler never
// fails.
//
// New starts nothing. Call Run.
func New[T any](
	reader stream.Reader,
	dlt dispatcher.Dispatcher,
	handler Handler[T],
	opts ...Option[T],
) (Consumer, error) {
	s := settings[T]{
		logger:          log.New(),
		stringGenerator: gen.UUIDGenerator(),
	}

	for _, opt := range opts {
		s = opt(s)
	}

	return &defaultConsumer[T]{
		reader:          reader,
		dlt:             dlt,
		handler:         handler,
		logger:          s.logger,
		stringGenerator: s.stringGenerator,
		skip:            s.skip,
		retry:           s.retry,
		deadLetterTopic: s.deadLetterTopic,
	}, nil
}

// Run polls and processes until ctx is cancelled or the reader fails.
//
// It blocks. A cancelled context finishes the batch in flight, commits what
// succeeded and returns nil; a reader failure returns that error.
func (c *defaultConsumer[T]) Run(ctx context.Context) error {
	defer func() { _ = c.reader.Close() }()

	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		records, err := c.reader.Poll(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, stream.ErrReaderClosed) {
				return nil
			}

			return err
		}

		if len(records) == 0 {
			continue
		}

		committable := c.processBatch(ctx, records)

		if len(committable) > 0 {
			// Commit with a context that outlives cancellation: work that
			// finished must not be reprocessed just because shutdown began.
			commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.commitTimeout())
			err := c.reader.Commit(commitCtx, committable...)
			cancel()

			if err != nil {
				return err
			}
		}
	}
}

// processBatch fans the batch out by partition and returns the records whose
// offsets may be committed.
//
// Records within a partition are processed in order; partitions run
// concurrently. That preserves the only ordering Kafka actually guarantees
// while letting throughput scale with the partition count.
func (c *defaultConsumer[T]) processBatch(ctx context.Context, records []stream.Record) []stream.Record {
	byPartition := make(map[partitionKey][]stream.Record)
	for _, record := range records {
		key := partitionKey{topic: record.Topic, partition: record.Partition}
		byPartition[key] = append(byPartition[key], record)
	}

	// Each goroutine writes to its own slot, so the results need no lock.
	partitions := make([][]stream.Record, 0, len(byPartition))
	for _, partitionRecords := range byPartition {
		partitions = append(partitions, partitionRecords)
	}

	results := make([][]stream.Record, len(partitions))

	var waitGroup sync.WaitGroup
	for i, partitionRecords := range partitions {
		waitGroup.Add(1)

		go func() {
			defer waitGroup.Done()

			results[i] = c.processPartition(ctx, partitionRecords)
		}()
	}

	waitGroup.Wait()

	var committable []stream.Record
	for _, done := range results {
		committable = append(committable, done...)
	}

	return committable
}

type partitionKey struct {
	topic     string
	partition int32
}

// processPartition handles one partition's records in order and returns the
// contiguous prefix that may be committed.
//
// It stops at the first record it cannot resolve - one whose dead letter
// publication failed - because committing past it would lose it.
func (c *defaultConsumer[T]) processPartition(ctx context.Context, records []stream.Record) []stream.Record {
	processed := make([]stream.Record, 0, len(records))

	for _, record := range records {
		if ctx.Err() != nil {
			return processed
		}

		recordCtx := c.contextFor(ctx, record)

		if err := c.processRecord(recordCtx, record); err != nil {
			log.Error(recordCtx, err, log.WarningTypeCritical)

			return processed
		}

		processed = append(processed, record)
	}

	return processed
}

// contextFor derives the per-record context: the trace continued from the
// producer, plus a logger scoped to this record.
func (c *defaultConsumer[T]) contextFor(ctx context.Context, record stream.Record) context.Context {
	if spanCtx, err := tracer.Extract(stream.NewHeaderCarrier(record.Headers)); err == nil {
		span := tracer.StartSpan("stream.consumer", tracer.ChildOf(spanCtx))
		ctx = tracer.ContextWithSpan(ctx, span)
	}

	logger := c.logger.WithFields(
		log.NewField("consumer-topic", record.Topic),
		log.NewField("message-partition", record.Partition),
		log.NewField("message-offset", record.Offset),
		log.NewField(loggerTraceKey, c.stringGenerator()),
	)

	return log.WithLogger(ctx, logger)
}

// processRecord resolves one record. A nil return means the offset may be
// committed - including when the record was routed to the dead letter topic,
// which is a resolution, not a failure.
func (c *defaultConsumer[T]) processRecord(ctx context.Context, record stream.Record) error {
	message, err := stream.NewMessageType(record)
	if err != nil {
		return c.toDeadLetterAsText(ctx, record)
	}

	var content T
	if err := message.Deserialize(record.Value, &content); err != nil {
		return c.toDeadLetterAsText(ctx, record)
	}

	if c.skip != nil && c.skip(content) {
		log.Debug(ctx, "record skipped")

		return nil
	}

	err = c.handler.Handle(ctx, content)
	if err == nil {
		return nil
	}

	log.Error(ctx, err)

	if !c.isRetryable(err) {
		return c.toDeadLetter(ctx, record, message, content)
	}

	if err := c.runRetry(ctx, content); err != nil {
		return c.toDeadLetter(ctx, record, message, content)
	}

	log.Info(ctx, "handler succeeded after retry")

	return nil
}

func (c *defaultConsumer[T]) isRetryable(err error) bool {
	for _, retryable := range c.retry.RetryableErrors {
		if errors.Is(err, retryable) {
			return true
		}
	}

	return false
}

// runRetry retries the handler with exponential backoff, honouring
// cancellation. The previous implementation slept and retried without a
// context, so a shutdown had to wait out the whole policy and the broker
// evicted the consumer for not polling.
func (c *defaultConsumer[T]) runRetry(ctx context.Context, content T) error {
	attempts := 0

	operation := func() (struct{}, error) {
		attempts++
		log.Debug(ctx, "retrying handler", log.NewField("attempt", attempts))

		return struct{}{}, c.handler.Handle(ctx, content)
	}

	exponential := backoff.NewExponentialBackOff()
	if c.retry.InitialInterval > 0 {
		exponential.InitialInterval = c.retry.InitialInterval
	}
	if c.retry.MaxInterval > 0 {
		exponential.MaxInterval = c.retry.MaxInterval
	}

	retryOpts := []backoff.RetryOption{backoff.WithBackOff(exponential)}
	if c.retry.MaxElapsedTime > 0 {
		retryOpts = append(retryOpts, backoff.WithMaxElapsedTime(c.retry.MaxElapsedTime))
	}

	if _, err := backoff.Retry(ctx, operation, retryOpts...); err != nil {
		return err
	}

	return nil
}

func (c *defaultConsumer[T]) toDeadLetter(
	ctx context.Context,
	record stream.Record,
	message stream.Message,
	content T,
) error {
	return c.publishDeadLetter(ctx, record, message.NewWithData(content))
}

func (c *defaultConsumer[T]) toDeadLetterAsText(ctx context.Context, record stream.Record) error {
	return c.publishDeadLetter(ctx, record, stream.NewTextMessage(string(record.Value)))
}

// publishDeadLetter retries the dead letter publication before giving up.
//
// Giving up returns ErrDeadLetterUnavailable, which halts this partition
// without committing - the record is redelivered once the topic accepts writes
// again. Other partitions are unaffected.
func (c *defaultConsumer[T]) publishDeadLetter(ctx context.Context, record stream.Record, message stream.Message) error {
	topic := c.deadLetterTopicFor(record)

	operation := func() (struct{}, error) {
		return struct{}{}, c.dlt.Dispatch(ctx, topic, string(record.Key), message)
	}

	_, err := backoff.Retry(
		ctx,
		operation,
		backoff.WithBackOff(backoff.NewExponentialBackOff()),
		backoff.WithMaxTries(deadLetterMaxTries),
	)
	if err != nil {
		return errors.Wrapf(ErrDeadLetterUnavailable, "topic %s: %s", topic, err)
	}

	log.Warn(ctx, "record sent to dead letter topic", log.WarningTypeCritical)

	return nil
}

func (c *defaultConsumer[T]) deadLetterTopicFor(record stream.Record) string {
	if c.deadLetterTopic != "" {
		return c.deadLetterTopic
	}

	return fmt.Sprintf("%s-%s", record.Topic, deadLetterSuffix)
}
```

- [ ] **Step 2: Add the two constants the loop references**

Append to `pkg/stream/consumer/consumer_option.go`:

```go
const (
	// deadLetterMaxTries bounds how often a dead letter publication is retried
	// before the partition halts.
	deadLetterMaxTries = 3

	// defaultCommitTimeout bounds the commit issued after a batch, including
	// the one issued during shutdown.
	defaultCommitTimeout = 10 * time.Second
)
```

And add the method to `consumer.go`:

```go
func (c *defaultConsumer[T]) commitTimeout() time.Duration {
	return defaultCommitTimeout
}
```

Add `"time"` to the imports of `consumer_option.go` and `consumer.go`, and `"sync"` to `consumer.go`.

- [ ] **Step 3: Build**

Run: `go build ./...`
Expected: no output. No new dependency is needed — the fan-out uses `sync.WaitGroup` with one result slot per goroutine, so nothing is shared for writing.

- [ ] **Step 4: Commit**

```bash
git add pkg/stream/consumer/ go.mod go.sum
git commit -m "feat!: per-partition concurrency, cancellation and dead letter policy"
```

---

### Task 9: Consumer tests on `synctest`

**Files:**
- Rewrite: `pkg/stream/consumer/consumer_test.go`
- Rewrite: `pkg/stream/consumer/mock_test.go`
- Rewrite: `pkg/stream/consumer/example_test.go`
- Modify: `pkg/stream/consumer/doc.go`

**Interfaces:**
- Consumes: everything from Tasks 7-8.
- Produces: `readerMock`, `dispatcherMock`, `handlerMock[T]` in `mock_test.go`.

- [ ] **Step 1: Write the mocks**

Replace `pkg/stream/consumer/mock_test.go`:

```go
package consumer_test

import (
	"context"

	"github.com/diegodesousas/go-devkit/pkg/stream"
	"github.com/stretchr/testify/mock"
)

type readerMock struct {
	mock.Mock
}

func (r *readerMock) Poll(ctx context.Context) ([]stream.Record, error) {
	args := r.Called(ctx)

	records, _ := args.Get(0).([]stream.Record)

	return records, args.Error(1)
}

func (r *readerMock) Commit(ctx context.Context, records ...stream.Record) error {
	args := r.Called(ctx, records)

	return args.Error(0)
}

func (r *readerMock) Close() error {
	args := r.Called()

	return args.Error(0)
}

type dispatcherMock struct {
	mock.Mock
}

func (d *dispatcherMock) Dispatch(ctx context.Context, topic, key string, content stream.Message) error {
	args := d.Called(ctx, topic, key, content)

	return args.Error(0)
}

func (d *dispatcherMock) Close(ctx context.Context) error {
	args := d.Called(ctx)

	return args.Error(0)
}

type handlerMock struct {
	mock.Mock
}

func (h *handlerMock) Handle(ctx context.Context, content string) error {
	args := h.Called(ctx, content)

	return args.Error(0)
}
```

- [ ] **Step 2: Write the failing tests**

Replace `pkg/stream/consumer/consumer_test.go`. These are the required cases; write each as its own function with an `expected*` block at the top, matching the repo's one-test-per-scenario style:

```go
package consumer_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"
	"github.com/diegodesousas/go-devkit/pkg/stream"
	"github.com/diegodesousas/go-devkit/pkg/stream/consumer"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func jsonRecord(topic string, partition int32, offset int64, value string) stream.Record {
	return stream.Record{
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
		Key:       []byte("key"),
		Value:     []byte(`"` + value + `"`),
		Headers: []stream.Header{
			{Key: stream.ContentTypeHeaderKey, Value: []byte("json")},
		},
	}
}

// A record whose handler succeeds is committed.
func TestRun_CommitsOnSuccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var (
			expectedTopic   = "orders"
			expectedContent = "payload"
		)

		record := jsonRecord(expectedTopic, 0, 1, expectedContent)

		reader := &readerMock{}
		reader.On("Poll", mock.Anything).Return([]stream.Record{record}, nil).Once()
		reader.On("Poll", mock.Anything).Return(nil, stream.ErrReaderClosed)
		reader.On("Commit", mock.Anything, []stream.Record{record}).Return(nil).Once()
		reader.On("Close").Return(nil).Once()

		handler := &handlerMock{}
		handler.On("Handle", mock.Anything, expectedContent).Return(nil).Once()

		c, err := consumer.New[string](reader, &dispatcherMock{}, handler)
		assert.Nil(t, err)

		assert.Nil(t, c.Run(context.Background()))

		reader.AssertExpectations(t)
		handler.AssertExpectations(t)
	})
}

// Cancelling the context ends Run without an error.
func TestRun_StopsOnContextCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		reader := &readerMock{}
		reader.On("Poll", mock.Anything).Return(nil, context.Canceled)
		reader.On("Close").Return(nil).Once()

		c, err := consumer.New[string](reader, &dispatcherMock{}, &handlerMock{})
		assert.Nil(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		assert.Nil(t, c.Run(ctx))
		reader.AssertExpectations(t)
	})
}

// A non-retryable failure sends the record to the dead letter topic and the
// offset is still committed - the record is resolved, not lost.
func TestRun_DeadLettersNonRetryableFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var (
			expectedTopic    = "orders"
			expectedDLT      = "orders-dlt"
			expectedHandlerErr = errors.New("permanent")
		)

		record := jsonRecord(expectedTopic, 0, 1, "payload")

		reader := &readerMock{}
		reader.On("Poll", mock.Anything).Return([]stream.Record{record}, nil).Once()
		reader.On("Poll", mock.Anything).Return(nil, stream.ErrReaderClosed)
		reader.On("Commit", mock.Anything, []stream.Record{record}).Return(nil).Once()
		reader.On("Close").Return(nil).Once()

		handler := &handlerMock{}
		handler.On("Handle", mock.Anything, "payload").Return(expectedHandlerErr).Once()

		dlt := &dispatcherMock{}
		dlt.On("Dispatch", mock.Anything, expectedDLT, "key", mock.Anything).Return(nil).Once()

		c, err := consumer.New[string](reader, dlt, handler)
		assert.Nil(t, err)

		assert.Nil(t, c.Run(context.Background()))

		dlt.AssertExpectations(t)
		reader.AssertExpectations(t)
	})
}

// When the dead letter topic is unavailable, the partition is not committed -
// nothing is lost and the record is redelivered later.
func TestRun_HaltsPartitionWhenDeadLetterFails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		record := jsonRecord("orders", 0, 1, "payload")

		reader := &readerMock{}
		reader.On("Poll", mock.Anything).Return([]stream.Record{record}, nil).Once()
		reader.On("Poll", mock.Anything).Return(nil, stream.ErrReaderClosed)
		reader.On("Close").Return(nil).Once()

		handler := &handlerMock{}
		handler.On("Handle", mock.Anything, "payload").Return(errors.New("permanent")).Once()

		dlt := &dispatcherMock{}
		dlt.On("Dispatch", mock.Anything, "orders-dlt", "key", mock.Anything).
			Return(errors.New("broker down"))

		c, err := consumer.New[string](reader, dlt, handler)
		assert.Nil(t, err)

		assert.Nil(t, c.Run(context.Background()))

		// Nothing was committable, so Commit must never have been called.
		reader.AssertNotCalled(t, "Commit", mock.Anything, mock.Anything)
	})
}

// A failure on one partition must not stop another.
func TestRun_OnePartitionFailureDoesNotBlockAnother(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		good := jsonRecord("orders", 0, 1, "good")
		bad := jsonRecord("orders", 1, 1, "bad")

		reader := &readerMock{}
		reader.On("Poll", mock.Anything).Return([]stream.Record{good, bad}, nil).Once()
		reader.On("Poll", mock.Anything).Return(nil, stream.ErrReaderClosed)
		reader.On("Commit", mock.Anything, mock.MatchedBy(func(records []stream.Record) bool {
			return len(records) == 1 && records[0].Partition == 0
		})).Return(nil).Once()
		reader.On("Close").Return(nil).Once()

		handler := &handlerMock{}
		handler.On("Handle", mock.Anything, "good").Return(nil).Once()
		handler.On("Handle", mock.Anything, "bad").Return(errors.New("permanent")).Once()

		dlt := &dispatcherMock{}
		dlt.On("Dispatch", mock.Anything, "orders-dlt", "key", mock.Anything).
			Return(errors.New("broker down"))

		c, err := consumer.New[string](reader, dlt, handler)
		assert.Nil(t, err)

		assert.Nil(t, c.Run(context.Background()))
		reader.AssertExpectations(t)
	})
}

// Retry is driven by the fake clock: synctest advances time when every
// goroutine is blocked, so the backoff completes instantly and deterministically.
func TestRun_RetriesRetryableError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		expectedRetryable := errors.New("transient")

		record := jsonRecord("orders", 0, 1, "payload")

		reader := &readerMock{}
		reader.On("Poll", mock.Anything).Return([]stream.Record{record}, nil).Once()
		reader.On("Poll", mock.Anything).Return(nil, stream.ErrReaderClosed)
		reader.On("Commit", mock.Anything, []stream.Record{record}).Return(nil).Once()
		reader.On("Close").Return(nil).Once()

		handler := &handlerMock{}
		handler.On("Handle", mock.Anything, "payload").Return(expectedRetryable).Once()
		handler.On("Handle", mock.Anything, "payload").Return(nil).Once()

		c, err := consumer.New[string](
			reader,
			&dispatcherMock{},
			handler,
			consumer.WithRetry[string](consumer.NewConfigRetry(
				consumer.WithRetryableErrors(expectedRetryable),
				consumer.WithInitialInterval(time.Second),
				consumer.WithMaxInterval(5*time.Second),
				consumer.WithMaxElapsedTime(30*time.Second),
			)),
		)
		assert.Nil(t, err)

		assert.Nil(t, c.Run(context.Background()))

		handler.AssertNumberOfCalls(t, "Handle", 2)
	})
}

// An undecodable payload goes to the dead letter topic as text and is committed.
func TestRun_DeadLettersUndecodableRecord(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		record := stream.Record{
			Topic:     "orders",
			Partition: 0,
			Offset:    1,
			Key:       []byte("key"),
			Value:     []byte("not json"),
			// No content type header at all.
		}

		reader := &readerMock{}
		reader.On("Poll", mock.Anything).Return([]stream.Record{record}, nil).Once()
		reader.On("Poll", mock.Anything).Return(nil, stream.ErrReaderClosed)
		reader.On("Commit", mock.Anything, []stream.Record{record}).Return(nil).Once()
		reader.On("Close").Return(nil).Once()

		dlt := &dispatcherMock{}
		dlt.On("Dispatch", mock.Anything, "orders-dlt", "key", mock.Anything).Return(nil).Once()

		handler := &handlerMock{}

		c, err := consumer.New[string](reader, dlt, handler)
		assert.Nil(t, err)

		assert.Nil(t, c.Run(context.Background()))

		handler.AssertNotCalled(t, "Handle", mock.Anything, mock.Anything)
		dlt.AssertExpectations(t)
	})
}

// A skip predicate commits the record without invoking the handler.
func TestRun_SkipsRecord(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		record := jsonRecord("orders", 0, 1, "skip-me")

		reader := &readerMock{}
		reader.On("Poll", mock.Anything).Return([]stream.Record{record}, nil).Once()
		reader.On("Poll", mock.Anything).Return(nil, stream.ErrReaderClosed)
		reader.On("Commit", mock.Anything, []stream.Record{record}).Return(nil).Once()
		reader.On("Close").Return(nil).Once()

		handler := &handlerMock{}

		c, err := consumer.New[string](
			reader,
			&dispatcherMock{},
			handler,
			consumer.WithSkip(func(content string) bool { return content == "skip-me" }),
		)
		assert.Nil(t, err)

		assert.Nil(t, c.Run(context.Background()))

		handler.AssertNotCalled(t, "Handle", mock.Anything, mock.Anything)
	})
}

// Shutdown interrupts a retry in progress instead of waiting the policy out.
// This is the regression test for the old runRetry, which slept without a
// context and kept the consumer from polling until the whole policy expired -
// long enough for the broker to evict it from the group.
func TestRun_CancelInterruptsRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		expectedRetryable := errors.New("transient")

		record := jsonRecord("orders", 0, 1, "payload")

		reader := &readerMock{}
		reader.On("Poll", mock.Anything).Return([]stream.Record{record}, nil).Once()
		reader.On("Poll", mock.Anything).Return(nil, stream.ErrReaderClosed)
		reader.On("Close").Return(nil).Once()

		ctx, cancel := context.WithCancel(context.Background())

		handler := &handlerMock{}
		// The handler always fails, and cancels on the second attempt. Without
		// a cancellable retry the backoff would run for its full hour.
		handler.
			On("Handle", mock.Anything, "payload").
			Run(func(mock.Arguments) { cancel() }).
			Return(expectedRetryable)

		dlt := &dispatcherMock{}
		dlt.On("Dispatch", mock.Anything, "orders-dlt", "key", mock.Anything).Return(nil).Maybe()

		c, err := consumer.New[string](
			reader,
			dlt,
			handler,
			consumer.WithRetry[string](consumer.NewConfigRetry(
				consumer.WithRetryableErrors(expectedRetryable),
				consumer.WithInitialInterval(time.Second),
				consumer.WithMaxElapsedTime(time.Hour),
			)),
		)
		assert.Nil(t, err)

		assert.Nil(t, c.Run(ctx))

		// The point is that Run returned at all: an uncancellable retry would
		// have blocked here for the full MaxElapsedTime.
		reader.AssertExpectations(t)
	})
}

// The span context injected by the dispatcher is extracted on the way back, so
// producer and consumer land in one trace instead of two disconnected ones.
func TestRun_ExtractsTraceFromHeaders(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		record := jsonRecord("orders", 0, 1, "payload")

		carrier := stream.HeaderCarrier{}
		span := tracer.StartSpan("producer.test")
		assert.Nil(t, tracer.Inject(span.Context(), carrier))
		span.Finish()

		record.Headers = append(record.Headers, carrier.Headers()...)

		reader := &readerMock{}
		reader.On("Poll", mock.Anything).Return([]stream.Record{record}, nil).Once()
		reader.On("Poll", mock.Anything).Return(nil, stream.ErrReaderClosed)
		reader.On("Commit", mock.Anything, mock.Anything).Return(nil).Once()
		reader.On("Close").Return(nil).Once()

		var handlerSpanFound bool

		handler := &handlerMock{}
		handler.
			On("Handle", mock.Anything, "payload").
			Run(func(args mock.Arguments) {
				ctx := args.Get(0).(context.Context)
				_, handlerSpanFound = tracer.SpanFromContext(ctx)
			}).
			Return(nil).
			Once()

		c, err := consumer.New[string](reader, &dispatcherMock{}, handler)
		assert.Nil(t, err)

		assert.Nil(t, c.Run(context.Background()))

		assert.True(t, handlerSpanFound, "handler context should carry the continued span")
	})
}

// A reader failure that is not a shutdown signal propagates out of Run.
func TestRun_PropagatesPollError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		expectedErr := errors.New("broker unreachable")

		reader := &readerMock{}
		reader.On("Poll", mock.Anything).Return(nil, expectedErr)
		reader.On("Close").Return(nil).Once()

		c, err := consumer.New[string](reader, &dispatcherMock{}, &handlerMock{})
		assert.Nil(t, err)

		assert.ErrorIs(t, c.Run(context.Background()), expectedErr)
		reader.AssertExpectations(t)
	})
}
```

- [ ] **Step 3: Run the tests**

Run: `go test ./pkg/stream/consumer/ -race -v`
Expected: PASS. `synctest.Test` requires Go 1.25+; the module targets 1.26.

- [ ] **Step 4: Verify no sleeps survived**

Run: `grep -n 'time.Sleep\|waitForCalls' pkg/stream/consumer/*_test.go`
Expected: no output

- [ ] **Step 5: Update the example and doc**

Rewrite `pkg/stream/consumer/example_test.go` for the new API:

```go
func ExampleNew() {
	writer, err := kafka.NewWriter(kafka.WithBrokers("localhost:9092"))
	if err != nil {
		panic(err)
	}

	dlt := dispatcher.New(writer)
	defer func() { _ = dlt.Close(context.Background()) }()

	reader, err := kafka.NewReader("devkit-billing", []string{"orders"},
		kafka.WithBrokers("localhost:9092"),
	)
	if err != nil {
		panic(err)
	}

	c, err := consumer.New[orderPlaced](reader, dlt, orderHandler{},
		consumer.WithRetry[orderPlaced](consumer.NewConfigRetry(
			consumer.WithRetryableErrors(errChargeUnavailable),
			consumer.WithInitialInterval(time.Second),
			consumer.WithMaxElapsedTime(30*time.Second),
		)),
	)
	if err != nil {
		panic(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := c.Run(ctx); err != nil {
		panic(err)
	}
}
```

Update `orderHandler` in that file to have only the `Handle` method. Rewrite the `pkg/stream/consumer/doc.go` code block to match.

- [ ] **Step 6: Full suite and lint**

Run: `make test && make lint`
Expected: PASS, `0 issues`

- [ ] **Step 7: Commit**

```bash
git add pkg/stream/consumer/
git commit -m "test: rewrite consumer tests on testing/synctest"
```

---

### Task 10: Drop cgo, update examples, README and CI

**Files:**
- Modify: `README.md`, `CLAUDE.md`
- Rewrite: `examples/stream/consumer/main.go`, `examples/stream/dispatcher/main.go`
- Modify: `go.mod` (remove `confluent-kafka-go`)

**Note:** neither the `Makefile` nor the workflows ever set `CGO_ENABLED` — cgo was on simply because it is the Go default. So dropping the cgo *requirement* is a documentation change plus a verification, not a build-file change. Do not go looking for a flag to delete.

**Interfaces:**
- Consumes: the full public API from Tasks 4-9.
- Produces: nothing new.

- [ ] **Step 1: Rewrite the dispatcher example**

Replace the client construction and shutdown in `examples/stream/dispatcher/main.go`:

```go
writer, err := kafka.NewWriter(
	kafka.WithBrokers("localhost:9092"),
	kafka.WithClientID("devkit-example-dispatcher"),
)
if err != nil {
	log.Fatal(ctx, err.Error())
}

d := dispatcher.New(writer)
defer func() {
	if err := d.Close(context.Background()); err != nil {
		log.Error(ctx, err)
	}
}()
```

The `Dispatch` calls in the loop are unchanged apart from `stream.NewJSONMessage`, which already has that name.

- [ ] **Step 2: Rewrite the consumer example**

`examples/stream/consumer/main.go` uses `kafka.NewReader("devkit-basic-consumer", []string{"externaldb.public.bet-events"}, ...)`, a handler with only `Handle`, and the blocking `Run(ctx)` with `signal.NotifyContext`. The whole signal block collapses to:

```go
ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
defer stop()

if err := c.Run(ctx); err != nil {
	log.Fatal(ctx, err.Error())
}
```

- [ ] **Step 3: Drop the confluent dependency**

```bash
go mod tidy
grep -rn 'confluent-kafka-go' go.mod pkg examples
```
Expected: no output

- [ ] **Step 4: Verify the build works without cgo**

Run: `CGO_ENABLED=0 go build ./... && CGO_ENABLED=0 go vet ./...`
Expected: no output — this is the payoff of the whole migration.

- [ ] **Step 5: Confirm `-race` still needs cgo**

`-race` requires cgo on darwin/amd64 and arm64 regardless of dependencies, so `make test` keeps `CGO_ENABLED=1`. Verify:

Run: `CGO_ENABLED=0 go test ./pkg/stream/... -race 2>&1 | head -3`
Expected: an error saying `-race` requires cgo. Record the exact message in the PR.

- [ ] **Step 6: Update the docs**

In `README.md`, replace the cgo paragraph with: the library is pure Go and builds without cgo; `-race` still requires `CGO_ENABLED=1`, which is a Go toolchain requirement rather than a dependency one.

In `CLAUDE.md`, update the same claim, the dependency list, and the "Armadilhas" section that says dd-trace and cgo are always on.

- [ ] **Step 7: Full verification**

Run: `make test-all && make lint`
Expected: PASS, `0 issues`

- [ ] **Step 8: Commit**

```bash
git add .
git commit -m "chore!: drop confluent-kafka-go and the cgo requirement"
```

---

### Task 11: Release v0.3.0

- [ ] **Step 1: Open the PR**

The migration table from the spec is the PR body. Lead with the group id hazard: readers must be constructed with the same group id previously in use, which was `devkit-<handler.ID()>`. Migrating without the `devkit-` prefix makes Kafka see a new group with no committed offsets and reprocess the topic from the beginning.

- [ ] **Step 2: Wait for CI**

Run: `gh pr checks <number> --watch`
Expected: Continuous Integration and Lint both pass

- [ ] **Step 3: Merge and clean up**

```bash
gh pr merge <number> --merge
git checkout main && git pull
git branch -d <branch>
git fetch --prune
```

- [ ] **Step 4: Release**

Because this touches no `pkg/database/sql` code, `make release` alone is sufficient — but run the full suite first anyway, since the change is large:

```bash
make test-all
make release BUMP=minor
```
Expected: `v0.3.0` published

- [ ] **Step 5: Verify the docs publish**

```bash
GOPROXY=https://proxy.golang.org go list -m github.com/diegodesousas/go-devkit@v0.3.0
curl -s -o /dev/null -w '%{http_code}\n' https://pkg.go.dev/github.com/diegodesousas/go-devkit@v0.3.0
```
Expected: the version resolves, and pkg.go.dev returns 200 within a few minutes.
