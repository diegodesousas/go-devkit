package stream

import "github.com/pkg/errors"

var (
	// Happens when trying to produce a message and the broker is unavailable
	ErrProcessMessageTimedOut = errors.New("processing message timed out")
)
