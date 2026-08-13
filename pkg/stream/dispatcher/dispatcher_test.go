package dispatcher_test

import (
    "bytes"
    "context"
    "testing"

    "github.com/pkg/errors"

    "github.com/confluentinc/confluent-kafka-go/kafka"
    "github.com/diegodesousas/go-devkit/pkg/stream"
    "github.com/diegodesousas/go-devkit/pkg/stream/dispatcher"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/mock"
)

func assertKafkaMessage(message *kafka.Message, expectedTopic string, expectedKey, expectedPayload []byte) bool {
    topic := *message.TopicPartition.Topic

    return expectedTopic == topic &&
        message.TopicPartition.Partition == kafka.PartitionAny &&
        bytes.Equal(message.Key, expectedKey) &&
        bytes.Equal(expectedPayload, message.Value) &&
        message.Headers[0].Key == "DEVKIT_CONTENT_TYPE" &&
        bytes.Equal(message.Headers[0].Value, []byte("text"))
}

func Test_Dispatcher_Dispatch_Success(t *testing.T) {
    expectedTopic := "topic-test"
    expectedKey := []byte("key-test")
    expectedMessage := "test message"
    expectedPayload := []byte(expectedMessage)

    var expectedDeliveryChan chan kafka.Event
    done := make(chan struct{}, 1)

    clientMock := &ClientDispatcherClientMock{}
    clientMock.
        On(
            "Produce",
            mock.MatchedBy(func(message *kafka.Message) bool {

                return assertKafkaMessage(message, expectedTopic, expectedKey, expectedPayload)
            }),
            mock.MatchedBy(func(deliveryChan chan kafka.Event) bool {
                expectedDeliveryChan = deliveryChan
                done <- struct{}{}

                return true
            }),
        ).
        Return(nil).
        Once()

    d := dispatcher.New(clientMock)

    go func() {
        <-done

        expectedDeliveryChan <- &kafka.Message{}
    }()

    err := d.Dispatch(context.Background(), expectedTopic, string(expectedKey), stream.NewTextMessage(expectedMessage))

    assert.Nil(t, err)

    clientMock.AssertExpectations(t)
}

func Test_Dispatcher_Dispatch_Delivery_MsgTimedOutError(t *testing.T) {
    expectedTopic := "topic-test"
    expectedKey := []byte("key-test")
    expectedMessage := "test message"
    expectedPayload := []byte(expectedMessage)

    var expectedDeliveryChan chan kafka.Event
    done := make(chan struct{}, 1)

    clientMock := &ClientDispatcherClientMock{}
    clientMock.
        On(
            "Produce",
            mock.MatchedBy(func(message *kafka.Message) bool {

                return assertKafkaMessage(message, expectedTopic, expectedKey, expectedPayload)
            }),
            mock.MatchedBy(func(deliveryChan chan kafka.Event) bool {
                expectedDeliveryChan = deliveryChan
                done <- struct{}{}

                return true
            }),
        ).
        Return(nil).
        Once()

    d := dispatcher.New(clientMock)

    go func() {
        <-done

        expectedDeliveryChan <- &kafka.Message{
            TopicPartition: kafka.TopicPartition{
                Error: kafka.NewError(kafka.ErrMsgTimedOut, "test", false),
            },
        }
    }()

    err := d.Dispatch(context.Background(), expectedTopic, string(expectedKey), stream.NewTextMessage(expectedMessage))

    assert.ErrorIs(t, err, stream.ErrProcessMessageTimedOut)

    clientMock.AssertExpectations(t)
}

func Test_Dispatcher_Dispatch_Delivery_UnhandledError(t *testing.T) {
    expectedTopic := "topic-test"
    expectedKey := []byte("key-test")
    expectedMessage := "test message"
    expectedPayload := []byte(expectedMessage)

    var expectedDeliveryChan chan kafka.Event
    done := make(chan struct{}, 1)

    clientMock := &ClientDispatcherClientMock{}
    clientMock.
        On(
            "Produce",
            mock.MatchedBy(func(message *kafka.Message) bool {

                return assertKafkaMessage(message, expectedTopic, expectedKey, expectedPayload)
            }),
            mock.MatchedBy(func(deliveryChan chan kafka.Event) bool {
                expectedDeliveryChan = deliveryChan
                done <- struct{}{}

                return true
            }),
        ).
        Return(nil).
        Once()

    d := dispatcher.New(clientMock)

    go func() {
        <-done

        expectedDeliveryChan <- &kafka.Message{
            TopicPartition: kafka.TopicPartition{
                Error: errors.New("unexpected delivery error"),
            },
        }
    }()

    err := d.Dispatch(context.Background(), expectedTopic, string(expectedKey), stream.NewTextMessage(expectedMessage))

    assert.EqualError(t, err, "dispatcher delivery error: unexpected delivery error")

    clientMock.AssertExpectations(t)
}

func Test_Dispatcher_Dispatch_Kafka_Error(t *testing.T) {
    expectedTopic := "topic-test"
    expectedKey := []byte("key-test")
    expectedMessage := "test message"
    expectedPayload := []byte(expectedMessage)

    var expectedDeliveryChan chan kafka.Event
    done := make(chan struct{}, 1)

    clientMock := &ClientDispatcherClientMock{}
    clientMock.
        On(
            "Produce",
            mock.MatchedBy(func(message *kafka.Message) bool {

                return assertKafkaMessage(message, expectedTopic, expectedKey, expectedPayload)

            }),
            mock.MatchedBy(func(deliveryChan chan kafka.Event) bool {
                expectedDeliveryChan = deliveryChan
                done <- struct{}{}

                return true
            }),
        ).
        Return(nil).
        Once()

    d := dispatcher.New(clientMock)

    go func() {
        <-done

        err := kafka.NewError(kafka.ErrApplication, "test", false)
        expectedDeliveryChan <- &err
    }()

    err := d.Dispatch(context.Background(), expectedTopic, string(expectedKey), stream.NewTextMessage(expectedMessage))

    assert.EqualError(t, err, "dispatcher kafka error: test")

    clientMock.AssertExpectations(t)
}

func Test_Dispatcher_Dispatch_Delivery_Unexpected_Error(t *testing.T) {
    expectedTopic := "topic-test"
    expectedKey := []byte("key-test")
    expectedMessage := "test message"
    expectedPayload := []byte(expectedMessage)

    var expectedDeliveryChan chan kafka.Event
    done := make(chan struct{}, 1)

    clientMock := &ClientDispatcherClientMock{}
    clientMock.
        On(
            "Produce",
            mock.MatchedBy(func(message *kafka.Message) bool {

                return assertKafkaMessage(message, expectedTopic, expectedKey, expectedPayload)

            }),
            mock.MatchedBy(func(deliveryChan chan kafka.Event) bool {
                expectedDeliveryChan = deliveryChan
                done <- struct{}{}

                return true
            }),
        ).
        Return(nil).
        Once()

    d := dispatcher.New(clientMock)

    go func() {
        <-done

        expectedDeliveryChan <- &kafka.OffsetsCommitted{
            Error: errors.New("unexpected err"),
        }
    }()

    err := d.Dispatch(context.Background(), expectedTopic, string(expectedKey), stream.NewTextMessage(expectedMessage))

    assert.EqualError(t, err, "dispatcher unexpected error: OffsetsCommitted (unexpected err, [])")

    clientMock.AssertExpectations(t)
}

func Test_Dispatcher_Dispatch_Client_Produce_Error(t *testing.T) {
    expectedTopic := "topic-test"
    expectedKey := []byte("key-test")
    expectedMessage := "test message"

    clientMock := &ClientDispatcherClientMock{}
    clientMock.
        On("Produce", mock.Anything, mock.Anything).
        Return(errors.New("unexpected error")).
        Once()

    d := dispatcher.New(clientMock)

    err := d.Dispatch(context.Background(), expectedTopic, string(expectedKey), stream.NewTextMessage(expectedMessage))

    assert.EqualError(t, err, "dispatcher: unexpected error")

    clientMock.AssertExpectations(t)
}

func Test_Dispatcher_Dispatch_Json_Marshaller_Error(t *testing.T) {
    expectedTopic := "topic-test"
    expectedKey := "key-test"

    clientMock := &ClientDispatcherClientMock{}

    d := dispatcher.New(clientMock)

    err := d.Dispatch(context.Background(), expectedTopic, expectedKey, stream.NewJSONMessage(make(chan string)))

    assert.EqualError(t, err, "dispatcher: json: unsupported type: chan string")

    clientMock.AssertExpectations(t)
}

func Test_Dispatcher_Shutdown_Success(t *testing.T) {
    clientMock := &ClientDispatcherClientMock{}
    clientMock.
        On("Flush", 1000).
        Return(0).
        Once().
        On("Close").
        Once()

    d := dispatcher.New(clientMock)

    d.Shutdown()

    clientMock.AssertExpectations(t)
}
