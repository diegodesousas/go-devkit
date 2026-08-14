package kafka_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/diegodesousas/go-devkit/pkg/stream"
	"github.com/diegodesousas/go-devkit/pkg/stream/kafka"
	"github.com/stretchr/testify/assert"
)

func TestNewWriter_NoBrokers(t *testing.T) {
	writer, err := kafka.NewWriter(kafka.WithClientID("billing"))

	assert.Nil(t, writer)
	assert.ErrorIs(t, err, kafka.ErrNoBrokers)
}

func TestWriter_Produce_DeliveryTimeout(t *testing.T) {
	expectedTimeout := time.Second

	// A listener that accepts connections and never answers is what keeps a
	// produced record unacknowledged until the delivery timeout expires. A
	// closed port would fail the dial instead, which is a different error.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	assert.Nil(t, err)

	defer func() { _ = listener.Close() }()

	go func() {
		// The accepted connections are kept referenced: a collected net.Conn
		// is closed by its finalizer, which would answer the client with EOF.
		var conns []net.Conn

		for {
			conn, err := listener.Accept()
			if err != nil {
				for _, open := range conns {
					_ = open.Close()
				}

				return
			}

			conns = append(conns, conn)
		}
	}()

	writer, err := kafka.NewWriter(
		kafka.WithBrokers(listener.Addr().String()),
		kafka.WithProduceTimeout(expectedTimeout),
	)
	assert.Nil(t, err)

	defer func() { _ = writer.Close() }()

	err = writer.Produce(context.Background(), stream.Record{
		Topic:   "orders",
		Key:     []byte("order-1"),
		Value:   []byte(`{"id":"order-1"}`),
		Headers: []stream.Header{{Key: "content-type", Value: []byte("application/json")}},
	})

	assert.ErrorIs(t, err, stream.ErrProcessMessageTimedOut)
}
