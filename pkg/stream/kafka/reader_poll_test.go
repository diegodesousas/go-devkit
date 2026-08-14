package kafka_test

import (
	"context"
	"net"
	"testing"

	"github.com/diegodesousas/go-devkit/pkg/stream"
	"github.com/diegodesousas/go-devkit/pkg/stream/kafka"
	"github.com/stretchr/testify/assert"
)

// newBlackHoleBroker starts a TCP listener that accepts connections and never
// answers them. kgo.NewClient does not dial synchronously, so a reader can be
// built - and closed - against this stub without ever completing a real Kafka
// handshake, which is what lets Poll's shutdown paths be exercised offline.
func newBlackHoleBroker(t *testing.T) net.Listener {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	assert.Nil(t, err)

	go func() {
		// Accepted connections are kept referenced: a collected net.Conn is
		// closed by its finalizer, which would answer the client with EOF.
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

	return listener
}

func TestReader_Poll_ContextCancelled(t *testing.T) {
	listener := newBlackHoleBroker(t)
	defer func() { _ = listener.Close() }()

	reader, err := kafka.NewReader("billing", []string{"orders"}, kafka.WithBrokers(listener.Addr().String()))
	assert.Nil(t, err)

	defer func() { _ = reader.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	records, err := reader.Poll(ctx)

	assert.Nil(t, records)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestReader_Poll_ClosedClient(t *testing.T) {
	listener := newBlackHoleBroker(t)
	defer func() { _ = listener.Close() }()

	reader, err := kafka.NewReader("billing", []string{"orders"}, kafka.WithBrokers(listener.Addr().String()))
	assert.Nil(t, err)
	assert.Nil(t, reader.Close())

	records, err := reader.Poll(context.Background())

	assert.Nil(t, records)
	assert.ErrorIs(t, err, stream.ErrReaderClosed)
}
