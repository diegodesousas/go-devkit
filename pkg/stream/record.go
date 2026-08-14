package stream

import "time"

// Header is one key/value pair carried alongside a Record. Kafka does not
// interpret headers; this package uses them to name the payload encoding and
// to carry the trace context.
type Header struct {
	Key   string
	Value []byte
}

// Record is one message on the wire, independent of any Kafka driver.
//
// It is the type the Reader and Writer seams speak, which is what keeps the
// driver out of the public API: swapping the client library changes how a
// Record is built, not what callers see.
//
// When producing, Partition, Offset, LeaderEpoch and Timestamp are left unset -
// the broker and the driver assign them.
type Record struct {
	Topic     string
	Partition int32
	Offset    int64

	// LeaderEpoch is an opaque value the driver stamps on a Record it read,
	// and that Reader.Commit hands back to it untouched. It carries no meaning
	// for callers and must not be invented: a driver uses it to tell whether
	// the partition changed hands between the read and the commit, and a value
	// it did not issue can make it decide the committed offset is stale and
	// rewind. Pass back the Record the Reader gave you, and leave the field
	// alone when producing.
	LeaderEpoch int32

	Key       []byte
	Value     []byte
	Headers   []Header
	Timestamp time.Time
}

// Header returns the value of the first header with the given key, and whether
// such a header exists. A nil value and false mean the header is absent.
func (r Record) Header(key string) ([]byte, bool) {
	for _, header := range r.Headers {
		if header.Key == key {
			return header.Value, true
		}
	}

	return nil, false
}
