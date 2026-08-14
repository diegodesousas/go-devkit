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
// When producing, Partition, Offset and Timestamp are left unset - the broker
// and the driver assign them.
type Record struct {
	Topic     string
	Partition int32
	Offset    int64
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
