package shutdown_test

import (
	"context"
	"fmt"
	"time"

	"github.com/diegodesousas/go-devkit/pkg/shutdown"
	"github.com/pkg/errors"
)

// Steps run in the order they are given, all under the same deadline.
func ExampleGraceful() {
	err := shutdown.Graceful(context.Background(), 5*time.Second,
		func(context.Context) error {
			fmt.Println("http server stopped")
			return nil
		},
		func(context.Context) error {
			fmt.Println("database closed")
			return nil
		},
	)

	fmt.Println("error:", err)

	// Output:
	// http server stopped
	// database closed
	// error: <nil>
}

// A failing step does not prevent the next ones from running; its error is
// returned once every step ran.
func ExampleGraceful_failingStep() {
	err := shutdown.Graceful(context.Background(), 5*time.Second,
		func(context.Context) error {
			return errors.New("http server did not stop")
		},
		func(context.Context) error {
			fmt.Println("database closed")
			return nil
		},
	)

	fmt.Println("error:", err)

	// Output:
	// database closed
	// error: http server did not stop
}
