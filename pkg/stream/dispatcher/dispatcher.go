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
