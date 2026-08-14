// Package dispatcher publishes messages to Kafka and waits for the broker to
// confirm each one.
//
// A dispatcher wraps a producer client:
//
//	client, err := dispatcher.NewClient(
//		dispatcher.WithBootstrapServers("localhost:9092"),
//	)
//	if err != nil {
//		return err
//	}
//
//	d := dispatcher.New(client)
//	defer d.Shutdown()
//
//	err = d.Dispatch(ctx, "orders", order.ID, stream.NewJSONMessage(order))
//
// Dispatch is synchronous: it blocks on the delivery report and returns only
// once the broker has acknowledged the message or reported a failure. A
// delivery timeout surfaces as stream.ErrProcessMessageTimedOut, so callers can
// distinguish "the broker is slow" from a malformed message.
//
// The producer is configured for ordering and durability over throughput -
// acks=all with a single in-flight request - which combined with the
// synchronous wait caps the rate at roughly one message per round trip. It is
// the right default for event streams that must not reorder, and the wrong one
// for bulk loading.
//
// Dispatch opens a Datadog span named "stream.dispatcher" tagged with the topic
// and key. The span does not currently propagate to the consumer.
//
// Shutdown flushes whatever the producer still holds and closes it. Call it
// before the process exits, or buffered messages are lost.
package dispatcher
