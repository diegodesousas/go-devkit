package main

import (
	"context"
	"fmt"
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

type Message struct {
	ID     string `json:"id"`
	Test   bool   `json:"test"`
	Amount int    `json:"amount"`
}

func (m Message) String() string {
	return fmt.Sprintf("ID: %s Test: %t Amount %d", m.ID, m.Test, m.Amount)
}

var RetryableError = errors.New("retryable error")

// {"id": "123", "test": true, "amount": 100}
type basicHandler struct{}

func (b basicHandler) Handle(ctx context.Context, content Message) error {
	log.Infof(ctx, "Processing Message: %s.", content)
	defer log.Info(ctx, "Message processed.")
	time.Sleep(time.Second * 5)

	if content.ID == "00" {
		return errors.New("non-retryable")
	}

	if content.Amount > 500 {
		return RetryableError
	}

	return nil
}

func main() {
	ctx := context.Background()

	writer, err := kafka.NewWriter(
		kafka.WithBrokers("localhost:9092"),
		kafka.WithClientID("devkit-example-consumer"),
	)
	if err != nil {
		log.Fatal(ctx, err.Error())
	}

	// The dispatcher is what publishes to the dead letter topic, so a
	// consumer needs one even when the handler never fails.
	d := dispatcher.New(writer)
	defer func() {
		if err := d.Close(context.Background()); err != nil {
			log.Error(ctx, err)
		}
	}()

	reader, err := kafka.NewReader("devkit-basic-consumer", []string{"externaldb.public.bet-events"},
		kafka.WithBrokers("localhost:9092"),
	)
	if err != nil {
		log.Fatal(ctx, err.Error())
	}

	logger := log.New(log.WithLevel(log.DebugLevel))

	c, err := consumer.New[Message](reader, d, basicHandler{},
		consumer.WithLogger[Message](logger),
		// Only errors listed here are retried; anything else goes straight
		// to the dead letter topic.
		consumer.WithRetry[Message](consumer.NewConfigRetry(
			consumer.WithRetryableErrors(RetryableError),
			consumer.WithInitialInterval(time.Second),
			consumer.WithMaxElapsedTime(time.Second*5),
			consumer.WithMaxInterval(time.Second*2),
		)),
	)
	if err != nil {
		log.Fatal(ctx, err.Error())
	}

	runCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := c.Run(runCtx); err != nil {
		log.Fatal(ctx, err.Error())
	}
}
