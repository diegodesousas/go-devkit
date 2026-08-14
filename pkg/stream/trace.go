package stream

// HeaderCarrier adapts a set of record headers to the TextMapWriter and
// TextMapReader interfaces the tracer uses, so a span context can ride along
// with a message and the consumer can pick the trace back up.
type HeaderCarrier map[string]string

// Set records one trace key. It satisfies tracer.TextMapWriter.
func (c HeaderCarrier) Set(key, val string) {
	c[key] = val
}

// ForeachKey iterates the carrier. It satisfies tracer.TextMapReader.
func (c HeaderCarrier) ForeachKey(handler func(key, val string) error) error {
	for key, val := range c {
		if err := handler(key, val); err != nil {
			return err
		}
	}

	return nil
}

// Headers converts the carrier into record headers.
func (c HeaderCarrier) Headers() []Header {
	headers := make([]Header, 0, len(c))
	for key, val := range c {
		headers = append(headers, Header{Key: key, Value: []byte(val)})
	}

	return headers
}

// NewHeaderCarrier builds a carrier from the headers of a consumed record, for
// extracting the span context the producer injected.
func NewHeaderCarrier(headers []Header) HeaderCarrier {
	carrier := make(HeaderCarrier, len(headers))
	for _, header := range headers {
		carrier[header.Key] = string(header.Value)
	}

	return carrier
}
