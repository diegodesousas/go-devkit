package gen

import (
	"fmt"

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
	var sequence = 0

	return func() string {
		sequence = sequence + 1

		return fmt.Sprintf("%d", sequence)
	}
}
