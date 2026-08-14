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

// A Poll failure of context.Canceled ends Run without an error, even though
// ctx itself is still live: isShutdown matches the error on its own, not only
// ctx.Err(), because a reader may report cancellation that way regardless of
// which context it was watching.
func TestRun_StopsOnContextCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		reader := &readerMock{}
		reader.On("Poll", mock.Anything).Return(nil, context.Canceled).Once()
		reader.On("Close").Return(nil).Once()

		c, err := consumer.New[string](reader, &dispatcherMock{}, &handlerMock{})
		assert.Nil(t, err)

		assert.Nil(t, c.Run(context.Background()))
		reader.AssertExpectations(t)
	})
}

// A context already cancelled before Run is ever called ends the loop before
// the first Poll: Run checks ctx.Err() at the top of every iteration,
// including the first, so a consumer told to shut down before it starts never
// makes a wasted call.
func TestRun_StopsImmediatelyWhenContextAlreadyCancelled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		reader := &readerMock{}
		reader.On("Close").Return(nil).Once()

		c, err := consumer.New[string](reader, &dispatcherMock{}, &handlerMock{})
		assert.Nil(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		assert.Nil(t, c.Run(ctx))

		reader.AssertNotCalled(t, "Poll", mock.Anything)
		reader.AssertExpectations(t)
	})
}

// Run also returns nil when the context ends by deadline rather than an
// explicit cancel. isShutdown consults ctx.Err() directly rather than only
// matching the error Poll returned, because a real reader reports an expired
// context however it sees fit - pkg/stream/kafka returns ctx.Err() raw, so a
// deadline surfaces as context.DeadlineExceeded, not context.Canceled.
func TestRun_StopsOnContextDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		reader := &readerMock{}
		// A real reader blocks on the context it was given and returns
		// whatever ctx.Err() is once it gives up - here that is
		// DeadlineExceeded, never Canceled.
		reader.
			On("Poll", mock.Anything).
			Run(func(args mock.Arguments) {
				ctx := args.Get(0).(context.Context)
				<-ctx.Done()
			}).
			Return(nil, context.DeadlineExceeded).
			Once()
		reader.On("Close").Return(nil).Once()

		c, err := consumer.New[string](reader, &dispatcherMock{}, &handlerMock{})
		assert.Nil(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		assert.Nil(t, c.Run(ctx))
		reader.AssertExpectations(t)
	})
}

// A non-retryable failure sends the record to the dead letter topic and the
// offset is still committed - the record is resolved, not lost.
func TestRun_DeadLettersNonRetryableFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var (
			expectedTopic      = "orders"
			expectedDLT        = "orders-dlt"
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

// The halt from a failed dead letter publication survives past the batch that
// caused it: later polls keep delivering the halted partition's records (the
// reader's fetch position moves on regardless), and this consumer must go on
// dropping them unprocessed even if the dead letter topic recovers in the
// meantime - a running consumer has no way to notice that recovery. A healthy
// partition is unaffected and keeps committing throughout.
//
// This is the regression test for the bug found in review: an earlier version
// tracked the halt only for the current batch, so a halted partition's next
// poll was treated as fresh - reprocessed by the handler, and if the dead
// letter topic happened to accept it this time, wrongly committed.
func TestRun_HaltedPartitionStaysHaltedAcrossPolls(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bad1 := jsonRecord("orders", 0, 1, "bad")
		good1 := jsonRecord("orders", 1, 1, "good")
		bad2 := jsonRecord("orders", 0, 2, "bad")
		good2 := jsonRecord("orders", 1, 2, "good")

		reader := &readerMock{}
		reader.On("Poll", mock.Anything).Return([]stream.Record{bad1, good1}, nil).Once()
		reader.On("Poll", mock.Anything).Return([]stream.Record{bad2, good2}, nil).Once()
		reader.On("Poll", mock.Anything).Return(nil, stream.ErrReaderClosed)
		reader.On("Close").Return(nil).Once()

		// The healthy partition must keep committing on every poll, including
		// the one after the other partition halted.
		reader.On("Commit", mock.Anything, mock.MatchedBy(func(records []stream.Record) bool {
			return len(records) == 1 && records[0].Partition == 1 && records[0].Offset == 1
		})).Return(nil).Once()
		reader.On("Commit", mock.Anything, mock.MatchedBy(func(records []stream.Record) bool {
			return len(records) == 1 && records[0].Partition == 1 && records[0].Offset == 2
		})).Return(nil).Once()

		handler := &handlerMock{}
		handler.On("Handle", mock.Anything, "good").Return(nil)
		handler.On("Handle", mock.Anything, "bad").Return(errors.New("permanent"))

		dlt := &dispatcherMock{}
		// The first three calls are the retries spent on bad1, in the batch
		// that trips the halt. If the halt does not persist, a fourth call
		// would come from bad2 on the next poll - and here it would succeed,
		// simulating the dead letter topic recovering. A correctly halted
		// partition never lets that fourth call happen.
		dlt.On("Dispatch", mock.Anything, "orders-dlt", "key", mock.Anything).
			Return(errors.New("broker down")).
			Times(3)
		dlt.On("Dispatch", mock.Anything, "orders-dlt", "key", mock.Anything).
			Return(nil)

		c, err := consumer.New[string](reader, dlt, handler)
		assert.Nil(t, err)

		assert.Nil(t, c.Run(context.Background()))

		reader.AssertExpectations(t)

		// bad1 is handled once, in the batch that halts its partition; bad2
		// must never reach the handler at all. good is handled once per poll.
		handler.AssertNumberOfCalls(t, "Handle", 3)

		// Only bad1's three failed retries: a halted partition never attempts
		// the dead letter topic again, even on a later poll.
		dlt.AssertNumberOfCalls(t, "Dispatch", 3)
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

// A record still being handled when shutdown begins is never dead-lettered:
// publishDeadLetter returns as soon as it sees the context is done, before
// dispatching anything. The record is simply left uncommitted, for the next
// process to redeliver - a shutdown is not the record's fault.
func TestRun_ShutdownDoesNotDeadLetterInFlightRecord(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		record := jsonRecord("orders", 0, 1, "payload")

		reader := &readerMock{}
		reader.On("Poll", mock.Anything).Return([]stream.Record{record}, nil).Once()
		reader.On("Close").Return(nil).Once()

		ctx, cancel := context.WithCancel(context.Background())

		handler := &handlerMock{}
		// No retry is configured, so this failure goes straight to the dead
		// letter path - except the context is cancelled first, right as the
		// handler observes the shutdown.
		handler.
			On("Handle", mock.Anything, "payload").
			Run(func(mock.Arguments) { cancel() }).
			Return(context.Canceled).
			Once()

		dlt := &dispatcherMock{}

		c, err := consumer.New[string](reader, dlt, handler)
		assert.Nil(t, err)

		assert.Nil(t, c.Run(ctx))

		dlt.AssertNotCalled(t, "Dispatch", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
		// The record was neither resolved nor lost: it stays uncommitted.
		reader.AssertNotCalled(t, "Commit", mock.Anything, mock.Anything)
		reader.AssertExpectations(t)
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
		// The handler always fails and cancels on every attempt. Without a
		// cancellable retry the backoff would run for its full hour before
		// Run ever returned.
		handler.
			On("Handle", mock.Anything, "payload").
			Run(func(mock.Arguments) { cancel() }).
			Return(expectedRetryable)

		dlt := &dispatcherMock{}

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

		// Exactly two calls: the first attempt in processRecord, and the one
		// retry attempt runRetry manages to make before it notices ctx is
		// done and gives up. An uncancellable retry would keep calling Handle
		// - with backoff growing between calls - until the hour-long policy
		// was exhausted, so this count is the thing that actually catches a
		// regression; Run returning at all does not.
		handler.AssertNumberOfCalls(t, "Handle", 2)

		// A cancelled context must never dead-letter the record left in
		// flight - see TestRun_ShutdownDoesNotDeadLetterInFlightRecord.
		dlt.AssertNotCalled(t, "Dispatch", mock.Anything, mock.Anything, mock.Anything, mock.Anything)

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
