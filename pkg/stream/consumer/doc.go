// Package consumer runs a typed Kafka consumer loop around a user-supplied
// handler.
//
// A Handler declares the topic it reads, the consumer group it joins, how to
// process a message of type T, and its retry policy. New wires it to a client
// factory and a dispatcher:
//
//	c, err := consumer.New[OrderPlaced](d, factory, handler)
//	if err != nil {
//		return err
//	}
//
//	shutdown, err := c.Run()
//
// Run starts the poll loop in the background and returns immediately. Calling
// the returned Shutdown drains the in-flight message and closes the client. A
// failure inside the loop - a broker error, a commit error - is reported on the
// channel from ListenShutdown, and the loop stops.
//
// Offsets are committed by this package, not by librdkafka: the client factory
// disables auto-commit, and a message is committed only after the handler
// returns without error.
//
// When the handler fails, the error is matched against the RetryableErrors of
// its ConfigRetry. A retryable error is retried with exponential backoff; a
// non-retryable one, or a retry sequence that exhausts MaxElapsedTime, sends the
// message to the dead letter topic, which is the source topic suffixed with
// "-dlt". Messages whose payload cannot be decoded at all go to the same topic
// as plain text.
//
// Each message gets its own logger, carrying the consumer group, the topic, the
// partition, the offset and a freshly generated trace id. The handler reads it
// with log.FromContext.
package consumer
