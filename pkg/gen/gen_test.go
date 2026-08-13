package gen_test

import (
	"sync"
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

func TestSequenceGeneratorIsConcurrencySafe(t *testing.T) {
	var (
		expectedGoroutines = 50
		expectedPerRoutine = 100
		expectedTotal      = expectedGoroutines * expectedPerRoutine
	)

	generator := gen.SequenceGenerator()

	results := make(chan string, expectedTotal)
	waitGroup := &sync.WaitGroup{}

	for i := 0; i < expectedGoroutines; i++ {
		waitGroup.Add(1)

		go func() {
			defer waitGroup.Done()

			for j := 0; j < expectedPerRoutine; j++ {
				results <- generator()
			}
		}()
	}

	waitGroup.Wait()
	close(results)

	seen := make(map[string]bool, expectedTotal)
	for result := range results {
		assert.False(t, seen[result], "sequence generator returned %s twice", result)
		seen[result] = true
	}

	assert.Len(t, seen, expectedTotal)
}
