package consumer_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"
	"github.com/diegodesousas/go-devkit/pkg/stream"
	"github.com/diegodesousas/go-devkit/pkg/stream/consumer"
	"github.com/diegodesousas/go-devkit/pkg/stream/dispatcher"
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

// The constructor validates; the options do not. A missing dependency has to
// fail here, because every one of them is only reached once a record is being
// resolved - a nil dispatcher, in particular, would panic inside the dead
// letter path, at the exact moment something else has already gone wrong.
func TestNew_Validation(t *testing.T) {
	tests := []struct {
		name    string
		reader  stream.Reader
		dlt     dispatcher.Dispatcher
		handler consumer.Handler[string]
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name:    "no reader",
			reader:  nil,
			dlt:     &dispatcherMock{},
			handler: &handlerMock{},
			wantErr: func(t assert.TestingT, err error, i ...any) bool {
				return assert.ErrorIs(t, err, consumer.ErrNoReader)
			},
		},
		{
			name:    "no dead letter dispatcher",
			reader:  &readerMock{},
			dlt:     nil,
			handler: &handlerMock{},
			wantErr: func(t assert.TestingT, err error, i ...any) bool {
				return assert.ErrorIs(t, err, consumer.ErrNoDeadLetterDispatcher)
			},
		},
		{
			name:    "no handler",
			reader:  &readerMock{},
			dlt:     &dispatcherMock{},
			handler: nil,
			wantErr: func(t assert.TestingT, err error, i ...any) bool {
				return assert.ErrorIs(t, err, consumer.ErrNoHandler)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := consumer.New(tt.reader, tt.dlt, tt.handler)

			assert.Nil(t, c)
			tt.wantErr(t, err)
		})
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

// dispatchedMessage matches a stream.Message by what it would actually put on
// the wire: its encoding and its serialized bytes.
//
// Matching on mock.Anything instead leaves the whole dead letter payload
// unverified - a consumer that dead-lettered an empty message, or the wrong
// one, would satisfy every expectation.
func dispatchedMessage(contentType, payload string) any {
	return mock.MatchedBy(func(message stream.Message) bool {
		serialized, err := message.Serialize()

		return err == nil && message.Type() == contentType && string(serialized) == payload
	})
}

// A non-retryable failure sends the record to the dead letter topic and the
// offset is still committed - the record is resolved, not lost.
//
// The message that lands there is the decoded payload re-encoded the way it
// arrived, not the raw bytes: toDeadLetter goes through NewWithData, so a
// record that came in as JSON leaves as JSON.
func TestRun_DeadLettersNonRetryableFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var (
			expectedTopic       = "orders"
			expectedDLT         = "orders-dlt"
			expectedHandlerErr  = errors.New("permanent")
			expectedContentType = "json"
			expectedPayload     = `"payload"`
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
		dlt.On("Dispatch", mock.Anything, expectedDLT, "key",
			dispatchedMessage(expectedContentType, expectedPayload)).Return(nil).Once()
		// Catches a wrong payload so the mismatch is reported by
		// AssertExpectations rather than by a panic inside a worker goroutine.
		dlt.On("Dispatch", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

		c, err := consumer.New[string](reader, dlt, handler)
		assert.Nil(t, err)

		assert.Nil(t, c.Run(context.Background()))

		dlt.AssertExpectations(t)
		reader.AssertExpectations(t)
	})
}

// When the dead letter topic is unavailable, the partition is not committed -
// nothing is lost and the record is redelivered later.
//
// It is also the batch's only partition, so with it halted the consumer has
// nothing left to make progress on: Run gives up with ErrDeadLetterUnavailable
// instead of polling a topic whose every record it would now drop.
func TestRun_HaltsPartitionWhenDeadLetterFails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		record := jsonRecord("orders", 0, 1, "payload")

		reader := &readerMock{}
		reader.On("Poll", mock.Anything).Return([]stream.Record{record}, nil).Once()
		reader.On("Close").Return(nil).Once()

		handler := &handlerMock{}
		handler.On("Handle", mock.Anything, "payload").Return(errors.New("permanent")).Once()

		dlt := &dispatcherMock{}
		dlt.On("Dispatch", mock.Anything, "orders-dlt", "key", mock.Anything).
			Return(errors.New("broker down"))

		c, err := consumer.New[string](reader, dlt, handler)
		assert.Nil(t, err)

		assert.ErrorIs(t, c.Run(context.Background()), consumer.ErrDeadLetterUnavailable)

		// Nothing was committable, so Commit must never have been called.
		reader.AssertNotCalled(t, "Commit", mock.Anything, mock.Anything)
		reader.AssertExpectations(t)
	})
}

// A consumer whose every partition has halted stops instead of spinning.
//
// The halt rule protects the partitions that are still healthy; with none of
// them left it protects nothing, and the loop degenerates into polling and
// discarding at whatever rate the broker serves - burning CPU and network,
// committing nothing, with a single log line from the moment of the halt as its
// only outward sign. Halts last for the life of the process by design, so the
// only way out is the restart that returning asks for.
func TestRun_StopsWhenEveryPartitionHasHalted(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bad0 := jsonRecord("orders", 0, 1, "bad")
		good1 := jsonRecord("orders", 1, 1, "good")
		bad0Again := jsonRecord("orders", 0, 2, "bad")
		bad1 := jsonRecord("orders", 1, 2, "bad")

		reader := &readerMock{}
		// The first poll halts partition 0 while partition 1 keeps committing,
		// so the run goes on. The second halts partition 1 too, which leaves
		// the consumer with nothing it is still allowed to commit.
		reader.On("Poll", mock.Anything).Return([]stream.Record{bad0, good1}, nil).Once()
		reader.On("Poll", mock.Anything).Return([]stream.Record{bad0Again, bad1}, nil).Once()
		// A consumer that kept looping would poll a third time. Answered here
		// only so that it fails on the call count below instead of panicking
		// on an unregistered call.
		reader.On("Poll", mock.Anything).Return(nil, stream.ErrReaderClosed).Maybe()
		reader.On("Commit", mock.Anything, []stream.Record{good1}).Return(nil).Once()
		reader.On("Close").Return(nil).Once()

		handler := &handlerMock{}
		handler.On("Handle", mock.Anything, "good").Return(nil)
		handler.On("Handle", mock.Anything, "bad").Return(errors.New("permanent"))

		dlt := &dispatcherMock{}
		dlt.On("Dispatch", mock.Anything, "orders-dlt", "key", mock.Anything).
			Return(errors.New("broker down"))

		c, err := consumer.New[string](reader, dlt, handler)
		assert.Nil(t, err)

		assert.ErrorIs(t, c.Run(context.Background()), consumer.ErrDeadLetterUnavailable)

		// Two polls and no third: a consumer that kept looping would poll
		// again, and there is no expectation left to answer it.
		reader.AssertNumberOfCalls(t, "Poll", 2)
		reader.AssertExpectations(t)

		// bad0 and good1 in the first batch, bad1 in the second. bad0Again is
		// dropped unprocessed, its partition already being halted.
		handler.AssertNumberOfCalls(t, "Handle", 3)
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

// processPartition stops at the first unresolved record instead of skipping
// over it: a later record on the same partition, in the same batch, must
// never reach the handler or be committed once an earlier one has halted the
// partition. Giving the failing partition a successor record, rather than
// leaving it as the batch's only record, is what makes this a test of the
// "stop", not just of the "halt": every other halt test in this file hands
// the failing partition exactly one record, so none of them can tell a
// consumer that breaks out of the loop apart from one that merely skips the
// unresolved record and keeps going - both would leave that single record
// uncommitted.
func TestRun_HaltStopsAtFirstUnresolvedRecordInPartition(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bad := jsonRecord("orders", 0, 1, "bad")
		good := jsonRecord("orders", 0, 2, "good")

		reader := &readerMock{}
		reader.On("Poll", mock.Anything).Return([]stream.Record{bad, good}, nil).Once()
		reader.On("Close").Return(nil).Once()

		handler := &handlerMock{}
		handler.On("Handle", mock.Anything, "bad").Return(errors.New("permanent")).Once()
		// Registered so that, if processPartition wrongly keeps going after
		// the halt, the call succeeds quietly instead of panicking on an
		// unregistered call - the point is to catch the bug with a plain
		// assertion, not a mock crash.
		handler.On("Handle", mock.Anything, "good").Return(nil)

		dlt := &dispatcherMock{}
		// bad's three retries, bounded by deadLetterMaxTries. good must never
		// reach the dead letter path at all, since it must never reach the
		// handler.
		dlt.On("Dispatch", mock.Anything, "orders-dlt", "key", mock.Anything).
			Return(errors.New("broker down")).
			Times(3)

		c, err := consumer.New[string](reader, dlt, handler)
		assert.Nil(t, err)

		// The batch's only partition halted, so the run ends there.
		assert.ErrorIs(t, c.Run(context.Background()), consumer.ErrDeadLetterUnavailable)

		// A correct implementation never calls Handle for good and never
		// commits anything in this batch - the partition halted on its first
		// record. A consumer that skips the unresolved record instead of
		// stopping at it would call Handle a second time and commit good@2.
		handler.AssertNumberOfCalls(t, "Handle", 1)
		reader.AssertNotCalled(t, "Commit", mock.Anything, mock.Anything)
	})
}

// partitionArrivals keeps the records the handler saw for one partition, in the
// order it saw them.
func partitionArrivals(arrivals []string, partition int32) []string {
	prefix := fmt.Sprintf("%d/", partition)

	var got []string
	for _, arrival := range arrivals {
		if strings.HasPrefix(arrival, prefix) {
			got = append(got, arrival)
		}
	}

	return got
}

// Partitions of a batch are processed concurrently, and in order within each -
// the reason the fan-out exists, and the only ordering Kafka itself guarantees.
//
// Nothing else in this file constrains it. Every other case here, including the
// one about a failure on one partition not blocking another, passes unchanged
// against a consumer that walks the partitions one after the other, so the
// concurrency has to be proved rather than inferred.
//
// The barrier is what proves it: the first record of each partition blocks
// until the other partition also has a record inside the handler. Two
// goroutines both reach it and both go on, so maxInFlight becomes 2. A serial
// consumer only ever has one there - and rather than hang, the fake clock jumps
// to the timeout the moment every goroutine is blocked, so the run finishes and
// fails on the assertion instead of on the suite's deadline.
func TestRun_ProcessesPartitionsConcurrentlyAndInOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			partitions          = 2
			recordsPerPartition = 3
		)

		var (
			expectedPartition0 = []string{"0/1", "0/2", "0/3"}
			expectedPartition1 = []string{"1/1", "1/2", "1/3"}
			expectedInFlight   = partitions
		)

		var records []stream.Record
		for partition := int32(0); partition < partitions; partition++ {
			for offset := int64(1); offset <= recordsPerPartition; offset++ {
				content := fmt.Sprintf("%d/%d", partition, offset)
				records = append(records, jsonRecord("orders", partition, offset, content))
			}
		}

		reader := &readerMock{}
		reader.On("Poll", mock.Anything).Return(records, nil).Once()
		reader.On("Poll", mock.Anything).Return(nil, stream.ErrReaderClosed)
		reader.On("Commit", mock.Anything, mock.MatchedBy(func(committed []stream.Record) bool {
			return len(committed) == len(records)
		})).Return(nil).Once()
		reader.On("Close").Return(nil).Once()

		var (
			mutex       sync.Mutex
			arrivals    []string
			inFlight    int
			maxInFlight int
			released    bool
		)

		bothInFlight := make(chan struct{})

		handler := &handlerMock{}
		handler.
			On("Handle", mock.Anything, mock.Anything).
			Run(func(args mock.Arguments) {
				mutex.Lock()
				arrivals = append(arrivals, args.Get(1).(string))
				inFlight++
				if inFlight > maxInFlight {
					maxInFlight = inFlight
				}
				release := inFlight == partitions && !released
				released = released || release
				mutex.Unlock()

				if release {
					close(bothInFlight)
				} else {
					select {
					case <-bothInFlight:
					case <-time.After(time.Minute):
					}
				}

				mutex.Lock()
				inFlight--
				mutex.Unlock()
			}).
			Return(nil)

		c, err := consumer.New[string](reader, &dispatcherMock{}, handler)
		assert.Nil(t, err)

		assert.Nil(t, c.Run(context.Background()))

		assert.Equal(t, expectedInFlight, maxInFlight,
			"both partitions must be inside the handler at once; a serial consumer never gets past one")
		assert.Equal(t, expectedPartition0, partitionArrivals(arrivals, 0))
		assert.Equal(t, expectedPartition1, partitionArrivals(arrivals, 1))
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
//
// This is the other of the two dead letter paths: nothing decoded it, so
// nothing can re-encode it, and the record's bytes are forwarded verbatim as
// text. Asserting the payload is what separates it from the JSON path - both
// were previously matched by mock.Anything, so neither was verified at all.
func TestRun_DeadLettersUndecodableRecord(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var (
			expectedContentType = "text"
			expectedPayload     = "not json"
		)

		record := stream.Record{
			Topic:     "orders",
			Partition: 0,
			Offset:    1,
			Key:       []byte("key"),
			Value:     []byte(expectedPayload),
			// No content type header at all.
		}

		reader := &readerMock{}
		reader.On("Poll", mock.Anything).Return([]stream.Record{record}, nil).Once()
		reader.On("Poll", mock.Anything).Return(nil, stream.ErrReaderClosed)
		reader.On("Commit", mock.Anything, []stream.Record{record}).Return(nil).Once()
		reader.On("Close").Return(nil).Once()

		dlt := &dispatcherMock{}
		dlt.On("Dispatch", mock.Anything, "orders-dlt", "key",
			dispatchedMessage(expectedContentType, expectedPayload)).Return(nil).Once()
		// See TestRun_DeadLettersNonRetryableFailure: catches a wrong payload
		// so the mismatch is an assertion, not a panic.
		dlt.On("Dispatch", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil).Maybe()

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
//
// Asserting only that the handler's context carries *a* span is not enough: a
// consumer that dropped tracer.ChildOf and started a fresh root span would
// still put a span in the context and still pass. What actually matters is
// that the handler's span shares the producer's trace id, which is what
// "landed in the same trace" means.
func TestRun_ExtractsTraceFromHeaders(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		record := jsonRecord("orders", 0, 1, "payload")

		carrier := stream.HeaderCarrier{}
		span := tracer.StartSpan("producer.test")
		expectedTraceID := span.Context().TraceID()
		assert.Nil(t, tracer.Inject(span.Context(), carrier))
		span.Finish()

		record.Headers = append(record.Headers, carrier.Headers()...)

		reader := &readerMock{}
		reader.On("Poll", mock.Anything).Return([]stream.Record{record}, nil).Once()
		reader.On("Poll", mock.Anything).Return(nil, stream.ErrReaderClosed)
		reader.On("Commit", mock.Anything, mock.Anything).Return(nil).Once()
		reader.On("Close").Return(nil).Once()

		var (
			handlerSpanFound bool
			handlerTraceID   string
		)

		handler := &handlerMock{}
		handler.
			On("Handle", mock.Anything, "payload").
			Run(func(args mock.Arguments) {
				ctx := args.Get(0).(context.Context)
				var handlerSpan *tracer.Span
				handlerSpan, handlerSpanFound = tracer.SpanFromContext(ctx)
				if handlerSpanFound {
					handlerTraceID = handlerSpan.Context().TraceID()
				}
			}).
			Return(nil).
			Once()

		c, err := consumer.New[string](reader, &dispatcherMock{}, handler)
		assert.Nil(t, err)

		assert.Nil(t, c.Run(context.Background()))

		assert.True(t, handlerSpanFound, "handler context should carry the continued span")
		assert.Equal(t, expectedTraceID, handlerTraceID, "handler span should share the producer's trace id, not start a fresh one")
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
