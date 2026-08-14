package stream

import "context"

// Reader consumes records from a topic as a member of a consumer group.
//
// Poll returns a batch because that is what the underlying clients deliver, and
// because a batch is what lets a consumer process partitions concurrently.
// An empty slice with a nil error means the poll timed out with nothing
// available, which is normal and not a failure.
//
// Commit acknowledges the given records. Implementations pick, per partition,
// the record with the highest LeaderEpoch and, within that epoch, the highest
// Offset - so passing every processed record is correct and passing only the
// last one per partition is an optimisation. Records must be handed back as the
// Reader produced them: a rebuilt Record loses its LeaderEpoch, and a driver
// that reads that field can take the loss for a stale commit and rewind.
//
// Offsets are never committed automatically: the caller decides when a record
// counts as processed.
type Reader interface {
	Poll(ctx context.Context) ([]Record, error)
	Commit(ctx context.Context, records ...Record) error
	Close() error
}

// Writer publishes records to a topic.
//
// Produce is synchronous: it returns once the broker has acknowledged the
// record or reported a failure. Cancelling ctx is best-effort once a record is
// in flight - an implementation may let it finish rather than abort it.
//
// Flush waits for anything still buffered. Close releases the connection; call
// Flush first unless losing buffered records is acceptable.
type Writer interface {
	Produce(ctx context.Context, record Record) error
	Flush(ctx context.Context) error
	Close() error
}
