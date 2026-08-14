package kafka

import (
	"context"

	"github.com/diegodesousas/go-devkit/pkg/log"
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
//
// Rebalances are blocked from the moment a Poll returns records until the next
// Poll begins, which is the window in which the caller processes and commits
// them. Without that, the default cooperative balancer can revoke a partition
// mid-batch and the commit that follows lands on a partition this member no
// longer owns - overwriting, at worst, a commit the new owner already made.
// The cost of blocking is the other side of the trade: a batch that takes
// longer than the broker's rebalance timeout gets this member kicked out of the
// group. Poll caps a batch at maxPollRecords to bound how long that can be.
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
		kgo.BlockRebalanceOnPoll(),
		kgo.SessionTimeout(s.sessionTimeout),
		kgo.ConsumeResetOffset(s.startAt()),
	)

	client, err := kgo.NewClient(clientOpts...)
	if err != nil {
		return nil, errors.Wrap(err, "kafka: building consumer")
	}

	return &reader{client: client}, nil
}

// Poll waits for records. A cancelled context or a closed reader yields that
// error and no records: both mean the loop is over.
//
// It opens by releasing the rebalance the previous batch was holding off, so a
// revocation lands between two batches and never between a record being polled
// and its offset being committed. This has to be the first thing Poll does:
// anything before it runs while the group is still blocked, and franz-go gives
// rebalances priority once released, so the release must happen before the poll
// that would otherwise queue behind it.
//
// Anything else the driver reports belongs to one partition, and the same fetch
// still carries whatever every other partition delivered. Franz-go injects some
// of those errors purely to inform - a data loss reset it has already recovered
// from, a group session it has already lost - so Poll logs them and hands the
// batch over. Only an error it cannot describe that way ends the poll, and even
// then the records that did arrive come back with it.
func (r *reader) Poll(ctx context.Context) ([]stream.Record, error) {
	r.client.AllowRebalance()

	fetches := r.client.PollRecords(ctx, maxPollRecords)

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if fetches.IsClientClosed() {
		return nil, errors.WithStack(stream.ErrReaderClosed)
	}

	kgoRecords := fetches.Records()

	records := make([]stream.Record, 0, len(kgoRecords))
	for _, kgoRecord := range kgoRecords {
		records = append(records, fromKgoRecord(kgoRecord))
	}

	return records, reportFetchErrors(ctx, fetches)
}

// reportFetchErrors logs every per-partition error franz-go injected into a
// fetch and returns the first one that is worth ending the poll for.
//
// The informational ones are not failures at all: ErrDataLoss says the client
// noticed a gap and has already reset itself, and ErrGroupSession says it lost
// the group session and will rejoin. Returning either would kill a consumer
// loop over something the driver has in hand - and, because a fetch reports
// errors per partition, would also throw away the records the healthy
// partitions delivered in the very same batch.
func reportFetchErrors(ctx context.Context, fetches kgo.Fetches) error {
	var fatal error

	for _, fetchErr := range fetches.Errors() {
		logger := log.FromContext(ctx).WithFields(
			log.NewField("consumer-topic", fetchErr.Topic),
			log.NewField("message-partition", fetchErr.Partition),
		)
		errCtx := log.WithLogger(ctx, logger)

		var dataLoss *kgo.ErrDataLoss
		var groupSession *kgo.ErrGroupSession

		switch {
		case errors.As(fetchErr.Err, &dataLoss):
			log.Error(errCtx, errors.Wrap(fetchErr.Err, "kafka: consuming reset after data loss"), log.WarningTypeCritical)
		case errors.As(fetchErr.Err, &groupSession):
			log.Warn(errCtx, "kafka: group session lost, rejoining: "+fetchErr.Err.Error())
		case errors.Is(fetchErr.Err, context.Canceled), errors.Is(fetchErr.Err, context.DeadlineExceeded):
			// The caller's own context, injected per partition. Poll answered
			// for it above; here it would only be noise.
		default:
			wrapped := errors.Wrapf(fetchErr.Err, "kafka: polling %s/%d", fetchErr.Topic, fetchErr.Partition)

			if fatal == nil {
				// The one Poll returns. Logging it here as well would only
				// double whatever the caller does with it.
				fatal = wrapped

				continue
			}

			// Every fetch error after the first would be lost otherwise.
			log.Error(errCtx, wrapped)
		}
	}

	return fatal
}

// Commit acknowledges records. franz-go keeps, per partition, the highest
// leader epoch and the highest offset within it, so passing every processed
// record is correct.
//
// The leader epoch travels back exactly as it arrived. Kafka checks it on the
// next fetch (KIP-320): an epoch the partition has already moved past means the
// commit belonged to a leader that has since been replaced, and the broker
// answers with the offset that epoch ended at instead of the one committed.
// Sending a fabricated epoch - 0, say, which is what a Record rebuilt by hand
// carries - therefore rewinds the group to wherever the first leader election
// happened, on every partition that ever had one.
func (r *reader) Commit(ctx context.Context, records ...stream.Record) error {
	if len(records) == 0 {
		return nil
	}

	if err := r.client.CommitRecords(ctx, toCommitRecords(records)...); err != nil {
		return errors.Wrap(err, "kafka: committing records")
	}

	return nil
}

// Close leaves the group and releases the connections held by the consumer.
//
// It releases the rebalance the last batch was holding off first. Leaving a
// group is itself a rebalance, so a close that did not release would wait for
// a poll that is never coming and hang forever.
func (r *reader) Close() error {
	r.client.CloseAllowingRebalance()

	return nil
}

// toCommitRecords reduces records to what a commit needs: the coordinates of
// the offset and the leader epoch that produced it.
func toCommitRecords(records []stream.Record) []*kgo.Record {
	kgoRecords := make([]*kgo.Record, 0, len(records))
	for _, record := range records {
		kgoRecords = append(kgoRecords, &kgo.Record{
			Topic:       record.Topic,
			Partition:   record.Partition,
			Offset:      record.Offset,
			LeaderEpoch: record.LeaderEpoch,
		})
	}

	return kgoRecords
}

func fromKgoRecord(kgoRecord *kgo.Record) stream.Record {
	headers := make([]stream.Header, 0, len(kgoRecord.Headers))
	for _, header := range kgoRecord.Headers {
		headers = append(headers, stream.Header{Key: header.Key, Value: header.Value})
	}

	return stream.Record{
		Topic:       kgoRecord.Topic,
		Partition:   kgoRecord.Partition,
		Offset:      kgoRecord.Offset,
		LeaderEpoch: kgoRecord.LeaderEpoch,
		Key:         kgoRecord.Key,
		Value:       kgoRecord.Value,
		Headers:     headers,
		Timestamp:   kgoRecord.Timestamp,
	}
}
