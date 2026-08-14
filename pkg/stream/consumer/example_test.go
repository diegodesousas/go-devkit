package consumer_test

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/diegodesousas/go-devkit/pkg/log"
	"github.com/diegodesousas/go-devkit/pkg/stream/consumer"
	"github.com/diegodesousas/go-devkit/pkg/stream/dispatcher"
	"github.com/diegodesousas/go-devkit/pkg/stream/kafka"
	"github.com/pkg/errors"
)

type orderPlaced struct {
	ID    string `json:"id"`
	Total int    `json:"total"`
}

// errChargeUnavailable stands for a downstream failure worth retrying, as
// opposed to a malformed message, which is not.
var errChargeUnavailable = errors.New("charge service unavailable")

type orderHandler struct{}

func (orderHandler) Handle(ctx context.Context, o orderPlaced) error {
	log.Info(ctx, "charging order", log.NewField("order-id", o.ID))
	return nil
}

// Requires a Kafka broker, so this example is compiled but not run.
func ExampleNew() {
	writer, err := kafka.NewWriter(kafka.WithBrokers("localhost:9092"))
	if err != nil {
		panic(err)
	}

	// The dispatcher is what publishes to the dead letter topic, so a
	// consumer needs one even when the handler never fails.
	dlt := dispatcher.New(writer)
	defer func() { _ = dlt.Close(context.Background()) }()

	reader, err := kafka.NewReader("devkit-billing", []string{"orders"},
		kafka.WithBrokers("localhost:9092"),
	)
	if err != nil {
		panic(err)
	}

	c, err := consumer.New[orderPlaced](reader, dlt, orderHandler{},
		// Only errors listed here are retried; anything else goes straight
		// to the dead letter topic.
		consumer.WithRetry[orderPlaced](consumer.NewConfigRetry(
			consumer.WithRetryableErrors(errChargeUnavailable),
			consumer.WithInitialInterval(time.Second),
			consumer.WithMaxElapsedTime(30*time.Second),
		)),
	)
	if err != nil {
		panic(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Run blocks until ctx is cancelled or the reader fails.
	if err := c.Run(ctx); err != nil {
		panic(err)
	}
}
