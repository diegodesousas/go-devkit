package stream

import "github.com/pkg/errors"

var (
	// ErrProcessMessageTimedOut is reported by the dispatcher when the broker
	// does not acknowledge a produced message within message.timeout.ms. It
	// separates an unreachable or overloaded broker from a message the broker
	// actively rejected.
	ErrProcessMessageTimedOut = errors.New("processing message timed out")

	// ErrReaderClosed is returned by Reader.Poll after the reader has been
	// closed. It ends a consumer loop without being treated as a broker
	// failure.
	ErrReaderClosed = errors.New("stream: reader is closed")
)
