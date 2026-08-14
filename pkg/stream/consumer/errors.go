package consumer

import "github.com/pkg/errors"

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
	ErrDeadLetterUnavailable = errors.New("consumer: dead letter topic unavailable")
)
