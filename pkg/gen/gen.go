package gen

import (
	"strconv"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/oklog/ulid/v2"
)

// StringGenerator produces a new string on every call. It is a function type so
// that a caller can inject a deterministic generator in tests where production
// code uses a random one.
type StringGenerator func() string

// UUIDGenerator returns a generator of random UUIDv4 strings.
func UUIDGenerator() StringGenerator {
	return func() string {
		return uuid.New().String()
	}
}

// ULIDGenerator returns a generator of ULID strings, which sort
// lexicographically in creation order.
func ULIDGenerator() StringGenerator {
	return func() string {
		return ulid.Make().String()
	}
}

// SequenceGenerator returns a generator counting from "1". It is safe for
// concurrent use, but each generator keeps its own counter and restarts with
// the process, so the values are unique only within one generator's lifetime -
// use it in tests, not as a source of durable identifiers.
func SequenceGenerator() StringGenerator {
	var sequence atomic.Int64

	return func() string {
		return strconv.FormatInt(sequence.Add(1), 10)
	}
}
