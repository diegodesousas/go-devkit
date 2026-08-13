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
type basicHandler[T Message] struct{}

func newTopicHandler() *basicHandler[Message] {
	return &basicHandler[Message]{}
}

func (b basicHandler[T]) Topic() string {
	return "externaldb.public.bet-events"
}

func (b basicHandler[T]) ID() string {
	return "basic-consumer"
}

func (b basicHandler[T]) ShouldSkip(content Message) bool {
	return false
}

func (b basicHandler[T]) Handle(ctx context.Context, content Message) error {
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

func (b basicHandler[T]) ConfigRetry() consumer.ConfigRetry {
	return consumer.ConfigRetry{
		RetryableErrors: []error{
			RetryableError,
		},
		InitialInterval: time.Second,
		MaxElapsedTime:  time.Second * 5,
		MaxInterval:     time.Second * 2,
	}
}

func main() {
	ctx := context.Background()

	dispatcherClient, err := dispatcher.NewClient(
		dispatcher.WithBootstrapServers("localhost:9092"),
	)
	if err != nil {
		log.Fatal(ctx, err.Error())
	}

	d := dispatcher.New(dispatcherClient)
	defer d.Shutdown()

	f := consumer.NewFactory(
		consumer.WithBootstrapServer("localhost:9092"),
	)

	handler := newTopicHandler()

	logger := log.New(log.WithLevel(log.DebugLevel))

	c, err := consumer.New[Message](d, f, handler, consumer.WithLogger(logger))
	if err != nil {
		log.Fatal(ctx, err.Error())
	}

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)

	shutdown, err := c.Run()
	if err != nil {
		return
	}

	go func() {
		err := <-c.ListenShutdown()

		interrupt <- syscall.SIGTERM
		log.Info(ctx, "internal finish with err "+err.Error())
	}()

	<-interrupt

	shutdown()
	log.Info(ctx, "shutdown complete")
}
