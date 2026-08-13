package gen_test

import (
	"testing"

	"github.com/diegodesousas/go-devkit/pkg/gen"
	"github.com/google/uuid"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/assert"
)

func TestUUIDGenerator(t *testing.T) {
	randID := gen.UUIDGenerator()
	_, err := uuid.Parse(randID())
	assert.NoError(t, err)
}

func TestULIDGenerator(t *testing.T) {
	randID := gen.ULIDGenerator()
	_, err := ulid.Parse(randID())
	assert.NoError(t, err)
}

func TestSequenceGenerator(t *testing.T) {
	generator := gen.SequenceGenerator()

	assert.Equal(t, generator(), "1")
	assert.Equal(t, generator(), "2")
	assert.Equal(t, generator(), "3")
}
