// Package encoding puts a JSON serializer behind an interface.
//
//	s := encoding.NewJSONSerializer()
//
//	data, err := s.Serialize(order)
//	if err != nil {
//		return err
//	}
//
//	var decoded Order
//	err = s.Deserialize(data, &decoded)
//
// The value of the indirection is substitutability: code that takes a
// JSONSerializer can be handed a fake in tests, or a different encoding later,
// without changing its signature.
//
// The implementation is github.com/goccy/go-json, which is a drop-in
// replacement for encoding/json - same struct tags, same semantics.
//
// No other package in this repository consumes it yet: pkg/cache marshals with
// goccy directly and pkg/stream uses the standard library.
package encoding
