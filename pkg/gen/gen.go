package gen

import (
	"strconv"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/oklog/ulid/v2"
)

type StringGenerator func() string

func UUIDGenerator() StringGenerator {
	return func() string {
		return uuid.New().String()
	}
}

func ULIDGenerator() StringGenerator {
	return func() string {
		return ulid.Make().String()
	}
}

func SequenceGenerator() StringGenerator {
	var sequence atomic.Int64

	return func() string {
		return strconv.FormatInt(sequence.Add(1), 10)
	}
}
