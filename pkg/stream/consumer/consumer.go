package consumer

import (
	"context"
	"fmt"
	"sync"

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

	// halted holds the partitions that stopped committing because a record
	// could not be resolved. It is read while the batch is grouped and written
	// after the fan-out has joined - both serial sections of a Run that is
	// itself single-threaded - so it needs no lock.
	halted map[partitionKey]struct{}
}

// New builds a Consumer decoding records into T.
//
// reader supplies the records and owns the group membership; dlt publishes to
// the dead letter topic, and is therefore required even when the handler never
// fails.
//
// New starts nothing. Call Run, once: a Consumer drives a single loop and is
// not meant to be shared between goroutines.
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
		halted:          make(map[partitionKey]struct{}),
	}, nil
}

// Run polls and processes until ctx is cancelled or the reader fails.
//
// It blocks. Cancelling ctx ends the run without an error: the record each
// partition has in flight is finished, the records the batch had not started
// are left for the next process, what completed is committed, and Run returns
// nil. A reader failure returns that error.
func (c *defaultConsumer[T]) Run(ctx context.Context) error {
	defer func() { _ = c.reader.Close() }()

	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		records, err := c.reader.Poll(ctx)
		if err != nil {
			if isShutdown(ctx, err) {
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
			commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), defaultCommitTimeout)
			err := c.reader.Commit(commitCtx, committable...)
			cancel()

			if err != nil {
				return err
			}
		}
	}
}

// isShutdown reports whether a poll failure is the shutdown making itself felt
// rather than a broker problem.
//
// The context is consulted directly, and not only matched against the error,
// because a reader reports an expired context however it sees fit -
// pkg/stream/kafka returns ctx.Err() raw, so a deadline arrives as
// context.DeadlineExceeded. Run promises nil for every cancellation, whether it
// lands while Poll blocks or between iterations.
func isShutdown(ctx context.Context, err error) bool {
	return ctx.Err() != nil ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, stream.ErrReaderClosed)
}

type partitionKey struct {
	topic     string
	partition int32
}

// partitionBatch is one partition's share of a poll.
type partitionBatch struct {
	key     partitionKey
	records []stream.Record
}

// partitionResult is what processPartition made of a partitionBatch: the
// records whose offsets may be committed, and whether the partition hit a
// record it could not resolve and must stop committing for good.
type partitionResult struct {
	committable []stream.Record
	halted      bool
}

// processBatch fans the batch out by partition and returns the records whose
// offsets may be committed.
//
// Records within a partition are processed in order; partitions run
// concurrently. That preserves the only ordering Kafka actually guarantees
// while letting throughput scale with the partition count.
//
// Records belonging to a halted partition are dropped unprocessed: that
// partition's offset must not move again.
func (c *defaultConsumer[T]) processBatch(ctx context.Context, records []stream.Record) []stream.Record {
	byPartition := make(map[partitionKey][]stream.Record)
	for _, record := range records {
		key := partitionKey{topic: record.Topic, partition: record.Partition}
		if _, halted := c.halted[key]; halted {
			continue
		}

		byPartition[key] = append(byPartition[key], record)
	}

	// Each goroutine writes to its own slot, so the results need no lock.
	batches := make([]partitionBatch, 0, len(byPartition))
	for key, partitionRecords := range byPartition {
		batches = append(batches, partitionBatch{key: key, records: partitionRecords})
	}

	results := make([]partitionResult, len(batches))

	var waitGroup sync.WaitGroup
	for i, batch := range batches {
		waitGroup.Go(func() {
			results[i] = c.processPartition(ctx, batch.records)
		})
	}

	waitGroup.Wait()

	var committable []stream.Record
	for i, result := range results {
		committable = append(committable, result.committable...)

		if result.halted {
			c.halt(ctx, batches[i].key)
		}
	}

	return committable
}

// halt stops committing a partition for the rest of this process's life.
//
// Its records keep being delivered - the reader is still subscribed and its
// fetch position moves on regardless - and are dropped unprocessed from every
// later batch, so the committed offset stays put and the whole backlog from the
// unresolved record onwards is redelivered to the next process.
//
// Halting is one-way and recorded once, which is what keeps the critical
// warning from repeating on every poll.
func (c *defaultConsumer[T]) halt(ctx context.Context, key partitionKey) {
	c.halted[key] = struct{}{}

	logger := c.logger.WithFields(
		log.NewField("consumer-topic", key.topic),
		log.NewField("message-partition", key.partition),
	)

	log.Warn(
		log.WithLogger(ctx, logger),
		"partition halted: a record could not be resolved and its offset will not advance",
		log.WarningTypeCritical,
	)
}

// processPartition handles one partition's records in order and reports the
// contiguous prefix that may be committed.
//
// It stops at the first record it cannot resolve - one whose dead letter
// publication failed - because committing past it would lose it, and reports
// the partition halted so that later batches skip it too.
//
// A record left unfinished by a cancelled context is not a halt: it is simply
// not committed, and the next process picks it up.
func (c *defaultConsumer[T]) processPartition(ctx context.Context, records []stream.Record) partitionResult {
	result := partitionResult{committable: make([]stream.Record, 0, len(records))}

	for _, record := range records {
		if ctx.Err() != nil {
			return result
		}

		if err := c.handleRecord(ctx, record); err != nil {
			result.halted = ctx.Err() == nil

			return result
		}

		result.committable = append(result.committable, record)
	}

	return result
}

// handleRecord resolves one record under its own context, and closes the span
// that context opened whatever the outcome.
//
// An unresolved record is worth waking someone for - it is about to stop a
// partition - unless the shutdown is what cut it short, in which case it is
// only uncommitted work that the next process will redo.
func (c *defaultConsumer[T]) handleRecord(ctx context.Context, record stream.Record) error {
	recordCtx, finishSpan := c.contextFor(ctx, record)
	defer finishSpan()

	err := c.processRecord(recordCtx, record)
	if err == nil {
		return nil
	}

	if ctx.Err() != nil {
		log.Error(recordCtx, err)

		return err
	}

	log.Error(recordCtx, err, log.WarningTypeCritical)

	return err
}

// contextFor derives the per-record context: the trace continued from the
// producer, plus a logger scoped to this record.
//
// The returned function ends the span the record opened, and does nothing when
// the producer sent no trace context.
func (c *defaultConsumer[T]) contextFor(ctx context.Context, record stream.Record) (context.Context, func()) {
	finishSpan := func() {}

	// ChildOf is deprecated in favour of Span.StartChild, which needs a local
	// parent span. There is none here - the parent is the producer - so ChildOf
	// remains the documented way to continue an extracted trace.
	if spanCtx, err := tracer.Extract(stream.NewHeaderCarrier(record.Headers)); err == nil {
		span := tracer.StartSpan("stream.consumer", tracer.ChildOf(spanCtx)) //nolint:staticcheck
		finishSpan = func() { span.Finish() }
		ctx = tracer.ContextWithSpan(ctx, span)
	}

	logger := c.logger.WithFields(
		log.NewField("consumer-topic", record.Topic),
		log.NewField("message-partition", record.Partition),
		log.NewField("message-offset", record.Offset),
		log.NewField(loggerTraceKey, c.stringGenerator()),
	)

	return log.WithLogger(ctx, logger), finishSpan
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
// without committing: nothing more of it is processed or committed by this
// process, and the next one picks the record back up. Other partitions are
// unaffected.
//
// A cancelled context returns before publishing anything. Handlers are told to
// honour cancellation, so on shutdown the record in flight fails with
// context.Canceled - and a record whose only fault was the shutdown belongs
// back on its own topic, not in the dead letter one.
func (c *defaultConsumer[T]) publishDeadLetter(ctx context.Context, record stream.Record, message stream.Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}

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
