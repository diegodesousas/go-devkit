// Package consumer runs a typed Kafka consumer loop around a user-supplied
// handler.
//
// A Handler processes one message of type T. New wires it to a Reader, which
// owns the topic and group membership, and a Dispatcher, which publishes to
// the dead letter topic:
//
//	c, err := consumer.New[OrderPlaced](reader, dlt, handler)
//	if err != nil {
//		return err
//	}
//
//	err = c.Run(ctx)
//
// Run blocks. Cancelling ctx ends it without an error, once the record each
// partition has in flight is finished; a reader failure returns that error.
// Run also returns ErrDeadLetterUnavailable once every partition has halted -
// see below.
//
// Records within a partition are processed in order, but Handle is called
// concurrently across the partitions of a batch - one goroutine per
// partition, serial within each - so a handler holding state must be safe
// for concurrent use.
//
// Offsets are committed by this package, not by the driver: a record is
// committed only after Handle returns without error, or after it has been
// routed to the dead letter topic, which counts as resolved.
//
// When the handler fails, the error is matched against the RetryableErrors of
// the ConfigRetry passed to WithRetry. A retryable error is retried with
// exponential backoff, honouring ctx; a non-retryable one, or a retry
// sequence that exhausts MaxElapsedTime, sends the record to the dead letter
// topic, which defaults to the source topic suffixed with "-dlt". Records
// whose payload cannot be decoded at all go to the same topic as plain text.
//
// If the dead letter publication itself fails, that partition halts: it is
// left uncommitted, and every later batch drops its records unprocessed for
// the rest of the process's life, so the offset never advances past the
// unresolved record and the next process redelivers it. The partition does
// not recover on its own even if the dead letter topic comes back, because a
// running consumer has no way to notice that it has. Other partitions are
// unaffected - and when there are none left, Run gives up and returns
// ErrDeadLetterUnavailable rather than poll a topic it can no longer make any
// progress on.
//
// Each record gets its own logger, carrying the topic, the partition, the
// offset and a freshly generated trace id. The handler reads it with
// log.FromContext.
package consumer
