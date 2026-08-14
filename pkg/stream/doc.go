// Package stream defines the message payloads exchanged over Kafka by the
// consumer and dispatcher packages.
//
// A Message knows how to serialize itself, how to decode a payload back into a
// Go value, and how to name its own encoding. Two implementations ship here:
//
//	stream.NewJSONMessage(order)   // encodes with encoding/json
//	stream.NewTextMessage("plain") // passes bytes through unchanged
//
// The encoding travels with the message. Dispatch writes the Type of the
// message into the DEVKIT_CONTENT_TYPE header, and on the way back
// NewMessageType reads that header to pick the matching implementation. A
// message that arrives without the header, or with a value nobody recognises,
// yields ErrUnknownMessageType - which is what makes the consumer route it to
// the dead letter topic as raw text instead of guessing.
//
// NewWithData produces a new message of the same kind carrying different data.
// The consumer uses it to re-encode a decoded payload when forwarding it to the
// dead letter topic.
package stream
