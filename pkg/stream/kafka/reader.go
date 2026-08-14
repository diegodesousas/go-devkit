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
// Of the shared Option set, NewReader honours WithBrokers, WithClientID,
// WithSASLPlain, WithSASLSCRAM, WithTLS, WithSessionTimeout and
// WithStartOffset; WithProduceTimeout is writer-only and has no effect here.
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

// Close releases the connections held by the consumer.
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
