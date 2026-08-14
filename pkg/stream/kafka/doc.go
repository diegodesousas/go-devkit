// Package kafka implements the stream transport seams on top of franz-go.
//
// It is the only package in this repository that imports a Kafka client
// library. Everything driver-specific - connection, authentication, TLS,
// timeouts, offset handling - lives here, and pkg/stream/consumer and
// pkg/stream/dispatcher depend only on stream.Reader and stream.Writer. That
// boundary is what makes a future driver change a rewrite of one package
// rather than a break in the public API.
//
// A reader joins a consumer group; a writer produces:
//
//	reader, err := kafka.NewReader("devkit-billing", []string{"orders"},
//		kafka.WithBrokers("localhost:9092"),
//	)
//
//	writer, err := kafka.NewWriter(kafka.WithBrokers("localhost:9092"))
//
// The same options configure both, so brokers, SASL and TLS are declared once.
//
// The group id is a positional parameter rather than an option because there
// is no safe default. A group with no committed offsets starts from the
// beginning of the topic, so inheriting a silently different group id would
// reprocess everything.
//
// Auto-commit is disabled. Offsets are committed by pkg/stream/consumer once a
// handler has succeeded, which is what keeps at-least-once delivery honest.
package kafka
