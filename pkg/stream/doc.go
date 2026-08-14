// Package stream defines the vocabulary the Kafka packages share: the records
// that travel over the wire, the payloads they carry, and the two interfaces a
// driver implements.
//
// # The driver seam
//
// Reader and Writer are the whole contract between a Kafka client library and
// the rest of this repository:
//
//	type Reader interface {
//		Poll(ctx context.Context) ([]Record, error)
//		Commit(ctx context.Context, records ...Record) error
//		Close() error
//	}
//
//	type Writer interface {
//		Produce(ctx context.Context, record Record) error
//		Flush(ctx context.Context) error
//		Close() error
//	}
//
// They speak Record and Header, declared here, so no driver type reaches the
// public API. pkg/stream/kafka implements both on top of franz-go and is the
// only package that imports a client library; pkg/stream/consumer takes a
// Reader and pkg/stream/dispatcher takes a Writer, and neither knows which
// driver is underneath. Changing drivers is a rewrite of one package.
//
// A Record carries the coordinates of a message - topic, partition, offset,
// leader epoch - along with its key, value, headers and timestamp. Commit takes
// Records rather than offsets so that the driver, not the consumer loop, works
// out which offset a processed record commits to.
//
// # Payloads
//
// A Message knows how to serialize itself, how to decode a payload back into a
// Go value, and how to name its own encoding. Two implementations ship here:
//
//	stream.NewJSONMessage(order)   // encodes with encoding/json
//	stream.NewTextMessage("plain") // passes bytes through unchanged
//
// The encoding travels with the message. Dispatch writes the Type of the
// message into the DEVKIT_CONTENT_TYPE header, and on the way back
// NewMessageType reads that header off the Record to pick the matching
// implementation. A message that arrives without the header, or with a value
// nobody recognises, yields ErrUnknownMessageType - which is what makes the
// consumer route it to the dead letter topic as raw text instead of guessing.
//
// NewWithData produces a new message of the same kind carrying different data.
// The consumer uses it to re-encode a decoded payload when forwarding it to the
// dead letter topic.
package stream
