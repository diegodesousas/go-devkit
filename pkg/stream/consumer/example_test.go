package consumer_test

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

type orderPlaced struct {
	ID    string `json:"id"`
	Total int    `json:"total"`
}

// errChargeUnavailable stands for a downstream failure worth retrying, as
// opposed to a malformed message, which is not.
var errChargeUnavailable = errors.New("charge service unavailable")

type orderHandler struct{}

func (orderHandler) ID() string    { return "billing" }
func (orderHandler) Topic() string { return "orders" }

// ShouldSkip runs before Handle and acknowledges the message without processing
// it, which is how a consumer ignores traffic it does not care about.
func (orderHandler) ShouldSkip(o orderPlaced) bool { return o.Total == 0 }

func (orderHandler) Handle(ctx context.Context, o orderPlaced) error {
	log.Info(ctx, "charging order", log.NewField("order-id", o.ID))
	return nil
}

// Only errors listed here are retried; anything else goes straight to the dead
// letter topic.
func (orderHandler) ConfigRetry() consumer.ConfigRetry {
	return consumer.NewConfigRetry(
		consumer.WithRetryableErrors(errChargeUnavailable),
		consumer.WithInitialInterval(time.Second),
		consumer.WithMaxInterval(5*time.Second),
		consumer.WithMaxElapsedTime(30*time.Second),
	)
}

// Requires a Kafka broker, so this example is compiled but not run.
func ExampleNew() {
	client, err := dispatcher.NewClient(dispatcher.WithBootstrapServers("localhost:9092"))
	if err != nil {
		panic(err)
	}

	// The dispatcher is what publishes to the dead letter topic, so a consumer
	// needs one even when the handler never fails.
	d := dispatcher.New(client)
	defer d.Shutdown()

	factory := consumer.NewFactory(consumer.WithBootstrapServer("localhost:9092"))

	c, err := consumer.New[orderPlaced](d, factory, orderHandler{})
	if err != nil {
		panic(err)
	}

	shutdown, err := c.Run() // returns immediately; the loop runs in the background
	if err != nil {
		panic(err)
	}

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)

	// The loop reports its own failures here, so a broker error stops the
	// process rather than leaving it running and consuming nothing.
	go func() {
		if err := <-c.ListenShutdown(); err != nil {
			fmt.Println("consumer stopped:", err)
			interrupt <- syscall.SIGTERM
		}
	}()

	<-interrupt

	shutdown() // waits for the in-flight message, then closes the client
}
