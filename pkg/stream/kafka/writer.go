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
// The producer is configured for durability over throughput: it waits for all
// in-sync replicas, which is also what the driver requires before it will keep
// its idempotent producer enabled.
//
// Ordering within a partition comes from that idempotent producer - each batch
// carries a sequence number, so a retried batch cannot land behind a later one
// - and not from capping in-flight requests, which the driver sizes itself.
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
		kgo.RecordDeliveryTimeout(s.produceTimeout),
	)

	client, err := kgo.NewClient(clientOpts...)
	if err != nil {
		return nil, errors.Wrap(err, "kafka: building producer")
	}

	return &writer{client: client}, nil
}

// Produce publishes one record and waits for the brokers to acknowledge it.
//
// A delivery timeout is reported as stream.ErrProcessMessageTimedOut, so
// callers can keep separating an unreachable or overloaded broker from a
// record the broker actively rejected. The driver's own message is kept in the
// wrapped text, because it is often the only clue as to why the broker went
// away.
//
// Neither that timeout nor cancelling ctx aborts a record already in flight:
// the idempotent producer waits for its outcome rather than leave a hole in
// the sequence numbers. See WithProduceTimeout.
func (w *writer) Produce(ctx context.Context, record stream.Record) error {
	results := w.client.ProduceSync(ctx, toKgoRecord(record))

	if err := results.FirstErr(); err != nil {
		if errors.Is(err, kgo.ErrRecordTimeout) {
			return errors.Wrapf(stream.ErrProcessMessageTimedOut, "kafka: producing record: %v", err)
		}

		return errors.Wrap(err, "kafka: producing record")
	}

	return nil
}

// Flush waits for every buffered record to be acknowledged or to fail.
func (w *writer) Flush(ctx context.Context) error {
	if err := w.client.Flush(ctx); err != nil {
		return errors.Wrap(err, "kafka: flushing producer")
	}

	return nil
}

// Close releases the connections held by the producer.
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
