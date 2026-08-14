package gen_test

import (
	"fmt"

	"github.com/diegodesousas/go-devkit/pkg/gen"
)

// SequenceGenerator counts from one, which makes it the generator to inject
// when a test needs to assert on the identifier that was produced.
func ExampleSequenceGenerator() {
	next := gen.SequenceGenerator()

	fmt.Println(next())
	fmt.Println(next())
	fmt.Println(next())

	// Output:
	// 1
	// 2
	// 3
}

// Each generator keeps its own counter, so two of them do not share a sequence.
func ExampleSequenceGenerator_independentCounters() {
	first := gen.SequenceGenerator()
	second := gen.SequenceGenerator()

	fmt.Println(first(), first(), second())

	// Output:
	// 1 2 1
}

// UUIDGenerator produces a random identifier, so only its shape is predictable.
func ExampleUUIDGenerator() {
	id := gen.UUIDGenerator()()

	fmt.Println(len(id))

	// Output:
	// 36
}
