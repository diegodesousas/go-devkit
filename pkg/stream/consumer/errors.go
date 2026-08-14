package consumer

import "github.com/pkg/errors"

// Errors returned when a Consumer cannot be built.
var (
	// ErrNoReader means New was called without a stream.Reader. There is
	// nothing to consume from and no default worth guessing.
	ErrNoReader = errors.New("consumer: a reader is required")

	// ErrNoDeadLetterDispatcher means New was called without a dispatcher for
	// the dead letter topic. It is required even for a handler that never
	// fails: an undecodable payload takes the same route, and finding out at
	// that point would mean a nil panic in the middle of resolving a record.
	ErrNoDeadLetterDispatcher = errors.New("consumer: a dead letter dispatcher is required")

	// ErrNoHandler means New was called without a Handler. There is no default
	// behaviour for a record - dropping or dead-lettering everything would
	// both be wrong.
	ErrNoHandler = errors.New("consumer: a handler is required")
)

var (
	// ErrDeadLetterUnavailable means a record could not be processed and could
	// not be published to the dead letter topic either.
	//
	// The consumer halts that partition: it leaves the record uncommitted and
	// stops processing anything more from it, for the rest of the process's
	// life. Nothing is lost - the offset never moves past the record, so the
	// next process redelivers it - but the partition does not recover on its
	// own when the dead letter topic comes back, because a running consumer
	// cannot tell that it has. Other partitions keep going.
	//
	// Run returns this error once there are no others left: with every
	// partition halted the loop would poll, drop and commit nothing, forever.
	// Restarting is the recovery path, and it redelivers everything the halted
	// partitions never committed.
	ErrDeadLetterUnavailable = errors.New("consumer: dead letter topic unavailable")
)
