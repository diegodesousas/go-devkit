// Package dispatcher publishes messages to a topic and waits for the broker to
// confirm each one.
//
// A dispatcher wraps a stream.Writer:
//
//	writer, err := kafka.NewWriter(kafka.WithBrokers("localhost:9092"))
//	if err != nil {
//		return err
//	}
//
//	d := dispatcher.New(writer)
//	defer func() { _ = d.Close(context.Background()) }()
//
//	err = d.Dispatch(ctx, "orders", order.ID, stream.NewJSONMessage(order))
//
// Dispatch is synchronous: it blocks on the delivery report and returns only
// once the broker has acknowledged the message or reported a failure. A
// delivery timeout surfaces as stream.ErrProcessMessageTimedOut - the kafka
// package maps franz-go's own timeout error to it - so callers can distinguish
// "the broker is slow" from a malformed message.
//
// Dispatch opens a Datadog span named "stream.dispatcher" tagged with the topic
// and key, and injects the span context into the record headers so the
// consumer can continue the same trace instead of starting a disconnected one.
//
// Close flushes whatever the writer still holds and closes it. Call it before
// the process exits, or buffered messages are lost.
package dispatcher
