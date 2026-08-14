package dispatcher_test

import (
	"context"
	"fmt"

	"github.com/diegodesousas/go-devkit/pkg/stream"
	"github.com/diegodesousas/go-devkit/pkg/stream/dispatcher"
	"github.com/diegodesousas/go-devkit/pkg/stream/kafka"
	"github.com/pkg/errors"
)

type orderPlaced struct {
	ID    string `json:"id"`
	Total int    `json:"total"`
}

// Requires a Kafka broker, so this example is compiled but not run.
func ExampleNew() {
	writer, err := kafka.NewWriter(kafka.WithBrokers("localhost:9092"))
	if err != nil {
		panic(err)
	}

	d := dispatcher.New(writer)
	defer func() { _ = d.Close(context.Background()) }()

	order := orderPlaced{ID: "42", Total: 250}

	err = d.Dispatch(context.Background(), "orders", order.ID, stream.NewJSONMessage(order))
	if err != nil {
		panic(err)
	}
}

// Dispatch blocks until the broker confirms the message, so a delivery timeout
// is reported as an error rather than discovered later.
func ExampleDispatcher_Dispatch_timeout() {
	writer, err := kafka.NewWriter(kafka.WithBrokers("localhost:9092"))
	if err != nil {
		panic(err)
	}

	d := dispatcher.New(writer)
	defer func() { _ = d.Close(context.Background()) }()

	order := orderPlaced{ID: "42", Total: 250}

	err = d.Dispatch(context.Background(), "orders", order.ID, stream.NewJSONMessage(order))

	switch {
	case errors.Is(err, stream.ErrProcessMessageTimedOut):
		// The broker never acknowledged. The message may or may not have
		// landed, so retrying has to be safe.
		fmt.Println("delivery timed out")
	case err != nil:
		fmt.Println("delivery failed:", err)
	}
}
