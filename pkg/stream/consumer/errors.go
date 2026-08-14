package consumer

import "github.com/pkg/errors"

var (
	// ErrDeadLetterUnavailable means a record could not be processed and could
	// not be published to the dead letter topic either.
	//
	// The consumer stops committing that partition and leaves the record
	// uncommitted, so nothing is lost and the partition resumes once the dead
	// letter topic accepts writes again. Other partitions keep going.
	ErrDeadLetterUnavailable = errors.New("consumer: dead letter topic unavailable")
)
