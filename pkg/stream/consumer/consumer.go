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
			commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), defaultCommitTimeout)
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

		if err := c.handleRecord(ctx, record); err != nil {
			return processed
		}

		processed = append(processed, record)
	}

	return processed
}

// handleRecord resolves one record under its own context, and closes the span
// that context opened whatever the outcome.
func (c *defaultConsumer[T]) handleRecord(ctx context.Context, record stream.Record) error {
	recordCtx, finishSpan := c.contextFor(ctx, record)
	defer finishSpan()

	if err := c.processRecord(recordCtx, record); err != nil {
		log.Error(recordCtx, err, log.WarningTypeCritical)

		return err
	}

	return nil
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
