package dispatcher

import (
    "context"

    "github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"
    "github.com/confluentinc/confluent-kafka-go/kafka"
    "github.com/diegodesousas/go-devkit/pkg/stream"
    "github.com/pkg/errors"
)

const (
    defaultFlushTimeoutMs int = 1000
)

type Dispatcher interface {
    Dispatch(ctx context.Context, topic string, key string, content stream.Message) error
    Shutdown()
}

type dispatcher struct {
    client         Client
    flushTimeoutMs int
}

func NewDispatcher(client Client) Dispatcher {
    return &dispatcher{
        client:         client,
        flushTimeoutMs: defaultFlushTimeoutMs,
    }
}

func (d dispatcher) Shutdown() {
    d.client.Flush(d.flushTimeoutMs)
    d.client.Close()
}

func (d dispatcher) Dispatch(ctx context.Context, topic string, key string, content stream.Message) error {
    span, ctx := tracer.StartSpanFromContext(ctx, "stream.dispatcher")
    defer span.Finish()

    span.SetTag("topic", topic)
    span.SetTag("key", key)

    payload, err := content.Serialize()
    if err != nil {
        return errors.Wrap(err, "dispatcher")
    }

    message := &kafka.Message{
        TopicPartition: kafka.TopicPartition{
            Topic:     &topic,
            Partition: kafka.PartitionAny,
        },
        Value: payload,
        Key:   []byte(key),
        Headers: []kafka.Header{
            {
                Key:   stream.ContentTypeHeaderKey,
                Value: []byte(content.Type()),
            },
        },
    }

    deliveryChan := make(chan kafka.Event, 1)
    defer close(deliveryChan)

    if err := d.client.Produce(message, deliveryChan); err != nil {
        return errors.Wrap(err, "dispatcher")
    }

    e := <-deliveryChan

    switch ev := e.(type) {
    case *kafka.Message:
        if ev.TopicPartition.Error == nil {
            return nil
        }

        return d.handleTopicPartitionError(ev.TopicPartition.Error)

    case *kafka.Error:
        return errors.Wrap(ev, "dispatcher kafka error")

    default:
        return errors.Errorf("dispatcher unexpected error: %s", ev)
    }
}

func (d dispatcher) handleTopicPartitionError(err error) error {
    wrapMessage := "dispatcher delivery error"

    if kafkaErr, ok := err.(kafka.Error); ok {
        switch kafkaErr.Code() {
        case kafka.ErrMsgTimedOut:
            return errors.Wrap(stream.ErrProcessMessageTimedOut, wrapMessage)
        }
    }

    return errors.Wrap(err, wrapMessage)
}
