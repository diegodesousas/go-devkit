package consumer_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/diegodesousas/go-devkit/pkg/gen"

	"github.com/diegodesousas/go-devkit/pkg/log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/diegodesousas/go-devkit/pkg/stream"
	"github.com/diegodesousas/go-devkit/pkg/stream/consumer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestConsumer_ProcessMessageSuccessfully(t *testing.T) {
	var (
		expectedGroupID                   = "devkit-group-id"
		expectedHandlerID                 = "group-id"
		expectedTopic                     = "test-topic"
		expectedDefaultReadMessageTimeout = time.Millisecond * 100
		expectedContentMessage            = "test message content"
		expectedKafkaMessage              = &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Error: nil,
			},
			Value: []byte(expectedContentMessage),
			Headers: []kafka.Header{
				{
					Key:   "DEVKIT_CONTENT_TYPE",
					Value: []byte("text"),
				},
			},
		}
	)

	dispatcherMock := &DispatcherMock{}

	clientMock := &ClientMock{}
	clientMock.
		On("Subscribe", expectedTopic, mock.Anything).
		Return(nil).
		Once().
		On("ReadMessage", expectedDefaultReadMessageTimeout).
		Return(expectedKafkaMessage, nil).
		Once().
		On("Close").
		Return(nil).
		Once().
		On("CommitMessage", expectedKafkaMessage).
		Return([]kafka.TopicPartition{}, nil).
		Once()

	factoryMock := &FactoryMock{}
	factoryMock.
		On("New", expectedGroupID).
		Return(clientMock, nil).
		Once()

	handlerMock := &HandlerTextMock[string]{}
	handlerMock.
		On("ID").
		Return(expectedHandlerID).
		Once().
		On("Topic").
		Return(expectedTopic).
		Once().
		On("ShouldSkip", expectedContentMessage).
		Return(false).
		Once().
		On("Handle", mock.Anything, expectedContentMessage).
		Run(func(args mock.Arguments) {
			time.Sleep(10 * time.Millisecond)
		}).
		Return(nil).
		Once()

	c, err := consumer.NewConsumer[string](dispatcherMock, factoryMock, handlerMock)
	assert.Nil(t, err)

	shutdown, err := c.Run()
	assert.Nil(t, err)

	time.Sleep(time.Millisecond * 5)
	shutdown()
	assert.Nil(t, err)

	dispatcherMock.AssertExpectations(t)
	clientMock.AssertExpectations(t)
	factoryMock.AssertExpectations(t)
	handlerMock.AssertExpectations(t)
}

func TestConsumer_ProcessMessageSuccessfullyWithSkip(t *testing.T) {
	var (
		expectedGroupID                   = "devkit-group-id"
		expectedHandlerID                 = "group-id"
		expectedTopic                     = "test-topic"
		expectedDefaultReadMessageTimeout = time.Millisecond * 100
		expectedContentMessage            = "test message content"
		expectedKafkaMessage              = &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Error: nil,
			},
			Value: []byte(expectedContentMessage),
			Headers: []kafka.Header{
				{
					Key:   "DEVKIT_CONTENT_TYPE",
					Value: []byte("text"),
				},
			},
		}
	)

	dispatcherMock := &DispatcherMock{}

	clientMock := &ClientMock{}
	clientMock.
		On("Subscribe", expectedTopic, mock.Anything).
		Return(nil).
		Once().
		On("ReadMessage", expectedDefaultReadMessageTimeout).
		Return(expectedKafkaMessage, nil).
		Once().
		On("Close").
		Return(nil).
		Once().
		On("CommitMessage", expectedKafkaMessage).
		Return([]kafka.TopicPartition{}, nil).
		Once()

	factoryMock := &FactoryMock{}
	factoryMock.
		On("New", expectedGroupID).
		Return(clientMock, nil).
		Once()

	handlerMock := &HandlerTextMock[string]{}
	handlerMock.
		On("ID").
		Return(expectedHandlerID).
		Once().
		On("Topic").
		Return(expectedTopic).
		Once().
		On("ShouldSkip", expectedContentMessage).
		Run(func(args mock.Arguments) {
			time.Sleep(10 * time.Millisecond)
		}).
		Return(true).
		Once()

	c, err := consumer.NewConsumer[string](dispatcherMock, factoryMock, handlerMock)
	assert.Nil(t, err)

	shutdown, err := c.Run()
	assert.Nil(t, err)

	time.Sleep(time.Millisecond * 5)
	shutdown()
	assert.Nil(t, err)

	dispatcherMock.AssertExpectations(t)
	clientMock.AssertExpectations(t)
	factoryMock.AssertExpectations(t)
	handlerMock.AssertExpectations(t)
}

func TestConsumer_ProcessMessageSuccessfullyWithRetry(t *testing.T) {
	var (
		retryableErr = errors.New("retryable error")
		configRetry  = consumer.ConfigRetry{
			RetryableErrors: []error{
				retryableErr,
			},
			InitialInterval: time.Millisecond * 100,
			MaxElapsedTime:  time.Millisecond * 300,
			MaxInterval:     time.Millisecond * 100,
		}

		expectedGroupID                   = "devkit-group-id"
		expectedHandlerID                 = "group-id"
		expectedTopic                     = "test-topic"
		expectedDefaultReadMessageTimeout = time.Millisecond * 100
		expectedContentMessage            = "test message content"
		expectedKafkaMessage              = &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Error: nil,
			},
			Value: []byte(expectedContentMessage),
			Headers: []kafka.Header{
				{
					Key:   "DEVKIT_CONTENT_TYPE",
					Value: []byte("text"),
				},
			},
		}
	)

	dispatcherMock := &DispatcherMock{}

	clientMock := &ClientMock{}
	clientMock.
		On("Subscribe", expectedTopic, mock.Anything).
		Return(nil).
		Once().
		On("ReadMessage", expectedDefaultReadMessageTimeout).
		Return(expectedKafkaMessage, nil).
		Once().
		On("Close").
		Return(nil).
		Once().
		On("CommitMessage", expectedKafkaMessage).
		Return([]kafka.TopicPartition{}, nil).
		Once()

	factoryMock := &FactoryMock{}
	factoryMock.
		On("New", expectedGroupID).
		Return(clientMock, nil).
		Once()

	var isRetry = false
	handlerMock := &HandlerTextMock[string]{}
	handlerMock.
		On("ID").
		Return(expectedHandlerID).
		Once().
		On("Topic").
		Return(expectedTopic).
		Once().
		On("ConfigRetry").
		Return(configRetry).
		Twice().
		On("ShouldSkip", expectedContentMessage).
		Return(false).
		Once().
		On("Handle",
			mock.Anything,
			mock.MatchedBy(
				func(content string) bool {
					return content == expectedContentMessage && !isRetry
				},
			),
		).
		Run(func(args mock.Arguments) {
			time.Sleep(100 * time.Millisecond)
			isRetry = true
		}).
		Return(retryableErr).
		Once().
		On("Handle",
			mock.Anything,
			mock.MatchedBy(
				func(content string) bool {
					return content == expectedContentMessage && isRetry
				},
			),
		).
		Run(func(args mock.Arguments) {
			time.Sleep(10 * time.Millisecond)
		}).
		Return(nil).
		Once()

	c, err := consumer.NewConsumer[string](dispatcherMock, factoryMock, handlerMock)
	assert.Nil(t, err)

	shutdown, err := c.Run()
	assert.Nil(t, err)

	time.Sleep(time.Millisecond * 5)
	shutdown()

	dispatcherMock.AssertExpectations(t)
	clientMock.AssertExpectations(t)
	factoryMock.AssertExpectations(t)
	handlerMock.AssertExpectations(t)
}

func TestConsumer_ProcessMessageFailedRetry(t *testing.T) {
	var (
		retryableErr = errors.New("retryable error")
		configRetry  = consumer.ConfigRetry{
			RetryableErrors: []error{
				retryableErr,
			},
			InitialInterval: time.Millisecond * 10,
			MaxElapsedTime:  time.Millisecond * 30,
			MaxInterval:     time.Millisecond * 10,
		}

		expectedGroupID                   = "devkit-group-id"
		expectedHandlerID                 = "group-id"
		expectedTopic                     = "test-topic"
		expectedTopicDLT                  = "test-topic-dlt"
		expectedDefaultReadMessageTimeout = time.Millisecond * 100
		expectedContentMessage            = "test message content"
		expectedTypeMessageDLT            = stream.NewTextMessage("test message content")
		expectedKey                       = "test-key"
		expectedKeyDLT                    = "test-key"
		expectedKafkaMessage              = &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Error: nil,
			},
			Key:   []byte(expectedKey),
			Value: []byte(expectedContentMessage),
			Headers: []kafka.Header{
				{
					Key:   "DEVKIT_CONTENT_TYPE",
					Value: []byte("text"),
				},
			},
		}
	)

	dispatcherMock := &DispatcherMock{}
	dispatcherMock.On("Dispatch", mock.Anything, expectedTopicDLT, expectedKeyDLT, expectedTypeMessageDLT).
		Return(nil).
		Once()

	clientMock := &ClientMock{}
	clientMock.
		On("Subscribe", expectedTopic, mock.Anything).
		Return(nil).
		Once().
		On("ReadMessage", expectedDefaultReadMessageTimeout).
		Return(expectedKafkaMessage, nil).
		Once().
		On("Close").
		Return(nil).
		Once().
		On("CommitMessage", expectedKafkaMessage).
		Return([]kafka.TopicPartition{}, nil).
		Once()

	factoryMock := &FactoryMock{}
	factoryMock.
		On("New", expectedGroupID).
		Return(clientMock, nil).
		Once()

	handlerMock := &HandlerTextMock[string]{}
	handlerMock.
		On("ID").
		Return(expectedHandlerID).
		Once().
		On("Topic").
		Return(expectedTopic).
		Once().
		On("ConfigRetry").
		Return(configRetry).
		Twice().
		On("ShouldSkip", expectedContentMessage).
		Return(false).
		Once().
		On("Handle", mock.Anything, expectedContentMessage).
		Run(func(args mock.Arguments) {
			time.Sleep(10 * time.Millisecond)
		}).
		Return(retryableErr).
		Times(3)

	c, err := consumer.NewConsumer[string](dispatcherMock, factoryMock, handlerMock)
	assert.Nil(t, err)

	shutdown, err := c.Run()
	assert.Nil(t, err)

	time.Sleep(time.Millisecond * 5)
	shutdown()

	dispatcherMock.AssertExpectations(t)
	clientMock.AssertExpectations(t)
	factoryMock.AssertExpectations(t)
	handlerMock.AssertExpectations(t)
}

func TestConsumer_ProcessMessageFailedRetry_DispatchToDeadLetterFails(t *testing.T) {
	var (
		retryableErr = errors.New("retryable error")
		configRetry  = consumer.ConfigRetry{
			RetryableErrors: []error{
				retryableErr,
			},
			InitialInterval: time.Millisecond * 10,
			MaxElapsedTime:  time.Millisecond * 30,
			MaxInterval:     time.Millisecond * 10,
		}

		expectedErr                       = errors.New("unexpected dispatcher error")
		expectedGroupID                   = "devkit-group-id"
		expectedHandlerID                 = "group-id"
		expectedTopic                     = "test-topic"
		expectedTopicDLT                  = "test-topic-dlt"
		expectedDefaultReadMessageTimeout = time.Millisecond * 100
		expectedContentMessage            = "test message content"
		expectedTypeMessageDLT            = stream.NewTextMessage("test message content")
		expectedKey                       = "test-key"
		expectedKeyDLT                    = "test-key"
		expectedKafkaMessage              = &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Error: nil,
			},
			Key:   []byte(expectedKey),
			Value: []byte(expectedContentMessage),
			Headers: []kafka.Header{
				{
					Key:   "DEVKIT_CONTENT_TYPE",
					Value: []byte("text"),
				},
			},
		}
	)

	dispatcherMock := &DispatcherMock{}
	dispatcherMock.On("Dispatch", mock.Anything, expectedTopicDLT, expectedKeyDLT, expectedTypeMessageDLT).
		Return(errors.New("unexpected dispatcher error")).
		Once()

	clientMock := &ClientMock{}
	clientMock.
		On("Subscribe", expectedTopic, mock.Anything).
		Return(nil).
		Once().
		On("ReadMessage", expectedDefaultReadMessageTimeout).
		Return(expectedKafkaMessage, nil).
		Once().
		On("Close").
		Return(nil).
		Once()

	factoryMock := &FactoryMock{}
	factoryMock.
		On("New", expectedGroupID).
		Return(clientMock, nil).
		Once()

	handlerMock := &HandlerTextMock[string]{}
	handlerMock.
		On("ID").
		Return(expectedHandlerID).
		Once().
		On("Topic").
		Return(expectedTopic).
		Once().
		On("ConfigRetry").
		Return(configRetry).
		Twice().
		On("ShouldSkip", expectedContentMessage).
		Return(false).
		Once().
		On("Handle", mock.Anything, expectedContentMessage).
		Run(func(args mock.Arguments) {
			time.Sleep(10 * time.Millisecond)
		}).
		Return(retryableErr).
		Times(3)

	c, err := consumer.NewConsumer[string](dispatcherMock, factoryMock, handlerMock)
	assert.Nil(t, err)

	shutdown, err := c.Run()
	assert.Nil(t, err)

	err = <-c.ListenShutdown()
	assert.Equal(t, expectedErr, err)

	shutdown()

	dispatcherMock.AssertExpectations(t)
	clientMock.AssertExpectations(t)
	factoryMock.AssertExpectations(t)
	handlerMock.AssertExpectations(t)
}

func TestConsumer_ProcessMessageFailedWithoutRetry(t *testing.T) {
	var (
		retryableErr    = errors.New("retryable error")
		nonRetryableErr = errors.New("non-retryable error")

		configRetry = consumer.ConfigRetry{
			RetryableErrors: []error{
				retryableErr,
			},
			InitialInterval: time.Millisecond * 10,
			MaxElapsedTime:  time.Millisecond * 30,
			MaxInterval:     time.Millisecond * 10,
		}

		expectedGroupID                   = "devkit-group-id"
		expectedHandlerID                 = "group-id"
		expectedTopic                     = "test-topic"
		expectedTopicDLT                  = "test-topic-dlt"
		expectedDefaultReadMessageTimeout = time.Millisecond * 100
		expectedContentMessage            = "test message content"
		expectedTypeMessageDLT            = stream.NewTextMessage("test message content")
		expectedKey                       = "test-key"
		expectedKeyDLT                    = "test-key"
		expectedKafkaMessage              = &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Error: nil,
			},
			Key:   []byte(expectedKey),
			Value: []byte(expectedContentMessage),
			Headers: []kafka.Header{
				{
					Key:   "DEVKIT_CONTENT_TYPE",
					Value: []byte("text"),
				},
			},
		}
	)

	dispatcherMock := &DispatcherMock{}
	dispatcherMock.On("Dispatch", mock.Anything, expectedTopicDLT, expectedKeyDLT, expectedTypeMessageDLT).
		Return(nil).
		Once()

	clientMock := &ClientMock{}
	clientMock.
		On("Subscribe", expectedTopic, mock.Anything).
		Return(nil).
		Once().
		On("ReadMessage", expectedDefaultReadMessageTimeout).
		Return(expectedKafkaMessage, nil).
		Once().
		On("Close").
		Return(nil).
		Once().
		On("CommitMessage", expectedKafkaMessage).
		Return([]kafka.TopicPartition{}, nil).
		Once()

	factoryMock := &FactoryMock{}
	factoryMock.
		On("New", expectedGroupID).
		Return(clientMock, nil).
		Once()

	handlerMock := &HandlerTextMock[string]{}
	handlerMock.
		On("ID").
		Return(expectedHandlerID).
		Once().
		On("Topic").
		Return(expectedTopic).
		Once().
		On("ConfigRetry").
		Return(configRetry).
		Once().
		On("ShouldSkip", expectedContentMessage).
		Return(false).
		Once().
		On("Handle", mock.Anything, expectedContentMessage).
		Run(func(args mock.Arguments) {
			time.Sleep(10 * time.Millisecond)
		}).
		Return(nonRetryableErr).
		Once()

	c, err := consumer.NewConsumer[string](dispatcherMock, factoryMock, handlerMock)
	assert.Nil(t, err)

	shutdown, err := c.Run()
	assert.Nil(t, err)

	time.Sleep(time.Millisecond * 5)
	shutdown()

	dispatcherMock.AssertExpectations(t)
	clientMock.AssertExpectations(t)
	factoryMock.AssertExpectations(t)
	handlerMock.AssertExpectations(t)
}

func TestConsumer_ProcessMessageFailedWithoutRetry_DispatchToDeadLetterFails(t *testing.T) {
	var (
		retryableErr    = errors.New("retryable error")
		nonRetryableErr = errors.New("non-retryable error")

		configRetry = consumer.ConfigRetry{
			RetryableErrors: []error{
				retryableErr,
			},
			InitialInterval: time.Millisecond * 10,
			MaxElapsedTime:  time.Millisecond * 30,
			MaxInterval:     time.Millisecond * 10,
		}

		expectedErr                       = errors.New("unexpected dispatcher error")
		expectedGroupID                   = "devkit-group-id"
		expectedHandlerID                 = "group-id"
		expectedTopic                     = "test-topic"
		expectedTopicDLT                  = "test-topic-dlt"
		expectedDefaultReadMessageTimeout = time.Millisecond * 100
		expectedContentMessage            = "test message content"
		expectedTypeMessageDLT            = stream.NewTextMessage("test message content")
		expectedKey                       = "test-key"
		expectedKeyDLT                    = "test-key"
		expectedKafkaMessage              = &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Error: nil,
			},
			Key:   []byte(expectedKey),
			Value: []byte(expectedContentMessage),
			Headers: []kafka.Header{
				{
					Key:   "DEVKIT_CONTENT_TYPE",
					Value: []byte("text"),
				},
			},
		}
	)

	dispatcherMock := &DispatcherMock{}
	dispatcherMock.On("Dispatch", mock.Anything, expectedTopicDLT, expectedKeyDLT, expectedTypeMessageDLT).
		Return(errors.New("unexpected dispatcher error")).
		Once()

	clientMock := &ClientMock{}
	clientMock.
		On("Subscribe", expectedTopic, mock.Anything).
		Return(nil).
		Once().
		On("ReadMessage", expectedDefaultReadMessageTimeout).
		Return(expectedKafkaMessage, nil).
		Once().
		On("Close").
		Return(nil).
		Once()

	factoryMock := &FactoryMock{}
	factoryMock.
		On("New", expectedGroupID).
		Return(clientMock, nil).
		Once()

	handlerMock := &HandlerTextMock[string]{}
	handlerMock.
		On("ID").
		Return(expectedHandlerID).
		Once().
		On("Topic").
		Return(expectedTopic).
		Once().
		On("ConfigRetry").
		Return(configRetry).
		Once().
		On("ShouldSkip", expectedContentMessage).
		Return(false).
		Once().
		On("Handle", mock.Anything, expectedContentMessage).
		Run(func(args mock.Arguments) {
			time.Sleep(10 * time.Millisecond)

		}).
		Return(nonRetryableErr).
		Once()

	c, err := consumer.NewConsumer[string](dispatcherMock, factoryMock, handlerMock)
	assert.Nil(t, err)

	shutdown, err := c.Run()
	assert.Nil(t, err)

	err = <-c.ListenShutdown()
	assert.Equal(t, expectedErr, err)

	shutdown()

	dispatcherMock.AssertExpectations(t)
	clientMock.AssertExpectations(t)
	factoryMock.AssertExpectations(t)
	handlerMock.AssertExpectations(t)
}

func TestConsumer_ProcessMessageFailed_NotAbleToDefineMessageType(t *testing.T) {
	var (
		expectedGroupID                   = "devkit-group-id"
		expectedHandlerID                 = "group-id"
		expectedTopic                     = "test-topic"
		expectedTopicDLT                  = "test-topic-dlt"
		expectedDefaultReadMessageTimeout = time.Millisecond * 100
		expectedContentMessage            = "test message content"
		expectedTypeMessageDLT            = stream.NewTextMessage("test message content")
		expectedKey                       = "test-key"
		expectedKeyDLT                    = "test-key"
		expectedKafkaMessage              = &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Error: nil,
			},
			Key:     []byte(expectedKey),
			Value:   []byte(expectedContentMessage),
			Headers: []kafka.Header{}, // without devkit header key
		}
	)

	dispatcherMock := &DispatcherMock{}
	dispatcherMock.On("Dispatch", mock.Anything, expectedTopicDLT, expectedKeyDLT, expectedTypeMessageDLT).
		Return(nil).
		Run(func(args mock.Arguments) {
			time.Sleep(time.Millisecond * 10)
		}).
		Once()

	clientMock := &ClientMock{}
	clientMock.
		On("Subscribe", expectedTopic, mock.Anything).
		Return(nil).
		Once().
		On("ReadMessage", expectedDefaultReadMessageTimeout).
		Return(expectedKafkaMessage, nil).
		Once().
		On("Close").
		Return(nil).
		Once().
		On("CommitMessage", expectedKafkaMessage).
		Return([]kafka.TopicPartition{}, nil).
		Once()

	factoryMock := &FactoryMock{}
	factoryMock.
		On("New", expectedGroupID).
		Return(clientMock, nil).
		Once()

	handlerMock := &HandlerTextMock[string]{}
	handlerMock.
		On("ID").
		Return(expectedHandlerID).
		Once().
		On("Topic").
		Return(expectedTopic).
		Once()

	c, err := consumer.NewConsumer[string](dispatcherMock, factoryMock, handlerMock)
	assert.Nil(t, err)

	shutdown, err := c.Run()
	assert.Nil(t, err)

	time.Sleep(time.Millisecond * 5)
	shutdown()

	dispatcherMock.AssertExpectations(t)
	clientMock.AssertExpectations(t)
	factoryMock.AssertExpectations(t)
	handlerMock.AssertExpectations(t)
}

func TestConsumer_ProcessMessageFailed_NotAbleToDefineMessageType_DispatchToDeadLetterFails(t *testing.T) {
	var (
		expectedErr                       = errors.New("unexpected dispatcher error")
		expectedGroupID                   = "devkit-group-id"
		expectedHandlerID                 = "group-id"
		expectedTopic                     = "test-topic"
		expectedTopicDLT                  = "test-topic-dlt"
		expectedDefaultReadMessageTimeout = time.Millisecond * 100
		expectedContentMessage            = "test message content"
		expectedTypeMessageDLT            = stream.NewTextMessage("test message content")
		expectedKey                       = "test-key"
		expectedKeyDLT                    = "test-key"
		expectedKafkaMessage              = &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Error: nil,
			},
			Key:     []byte(expectedKey),
			Value:   []byte(expectedContentMessage),
			Headers: []kafka.Header{}, // without devkit header key
		}
	)

	dispatcherMock := &DispatcherMock{}
	dispatcherMock.On("Dispatch", mock.Anything, expectedTopicDLT, expectedKeyDLT, expectedTypeMessageDLT).
		Return(errors.New("unexpected dispatcher error")).
		Once()

	clientMock := &ClientMock{}
	clientMock.
		On("Subscribe", expectedTopic, mock.Anything).
		Return(nil).
		Once().
		On("ReadMessage", expectedDefaultReadMessageTimeout).
		Return(expectedKafkaMessage, nil).
		Once().
		On("Close").
		Return(nil).
		Once()

	factoryMock := &FactoryMock{}
	factoryMock.
		On("New", expectedGroupID).
		Return(clientMock, nil).
		Once()

	handlerMock := &HandlerTextMock[string]{}
	handlerMock.
		On("ID").
		Return(expectedHandlerID).
		Once().
		On("Topic").
		Return(expectedTopic).
		Once()

	c, err := consumer.NewConsumer[string](dispatcherMock, factoryMock, handlerMock)
	assert.Nil(t, err)

	shutdown, err := c.Run()
	assert.Nil(t, err)

	err = <-c.ListenShutdown()
	assert.Equal(t, expectedErr, err)

	shutdown()

	dispatcherMock.AssertExpectations(t)
	clientMock.AssertExpectations(t)
	factoryMock.AssertExpectations(t)
	handlerMock.AssertExpectations(t)
}

func TestConsumer_ProcessMessageFailed_DeserializeError(t *testing.T) {
	var (
		expectedGroupID                   = "devkit-group-id"
		expectedHandlerID                 = "group-id"
		expectedTopic                     = "test-topic"
		expectedTopicDLT                  = "test-topic-dlt"
		expectedDefaultReadMessageTimeout = time.Millisecond * 100
		expectedContentMessage            = `{"test": "test", "invalid_json_field": "test"}`
		expectedTypeMessageDLT            = stream.NewTextMessage(`{"test": "test", "invalid_json_field": "test"}`)
		expectedKey                       = "test-key"
		expectedKeyDLT                    = "test-key"
		expectedKafkaMessage              = &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Error: nil,
			},
			Key:   []byte(expectedKey),
			Value: []byte(expectedContentMessage),
			Headers: []kafka.Header{
				{
					Key:   "DEVKIT_CONTENT_TYPE",
					Value: []byte("json"),
				},
			},
		}
	)

	dispatcherMock := &DispatcherMock{}
	dispatcherMock.On("Dispatch", mock.Anything, expectedTopicDLT, expectedKeyDLT, expectedTypeMessageDLT).
		Return(nil).
		Run(func(args mock.Arguments) {
			time.Sleep(time.Millisecond * 10)
		}).
		Once()

	clientMock := &ClientMock{}
	clientMock.
		On("Subscribe", expectedTopic, mock.Anything).
		Return(nil).
		Once().
		On("ReadMessage", expectedDefaultReadMessageTimeout).
		Return(expectedKafkaMessage, nil).
		Once().
		On("Close").
		Return(nil).
		Once().
		On("CommitMessage", expectedKafkaMessage).
		Return([]kafka.TopicPartition{}, nil).
		Once()

	factoryMock := &FactoryMock{}
	factoryMock.
		On("New", expectedGroupID).
		Return(clientMock, nil).
		Once()

	handlerMock := &HandlerJsonMock[jsonMessageTest]{}
	handlerMock.
		On("ID").
		Return(expectedHandlerID).
		Once().
		On("Topic").
		Return(expectedTopic).
		Once()

	c, err := consumer.NewConsumer[jsonMessageTest](dispatcherMock, factoryMock, handlerMock)
	assert.Nil(t, err)

	shutdown, err := c.Run()
	assert.Nil(t, err)

	time.Sleep(time.Millisecond * 5)
	shutdown()

	dispatcherMock.AssertExpectations(t)
	clientMock.AssertExpectations(t)
	factoryMock.AssertExpectations(t)
	handlerMock.AssertExpectations(t)
}

func TestConsumer_ProcessMessageFailed_DeserializeError_DispatchToDeadLetterFails(t *testing.T) {
	var (
		expectedErr                       = errors.New("unexpected dispatcher error")
		expectedGroupID                   = "devkit-group-id"
		expectedHandlerID                 = "group-id"
		expectedTopic                     = "test-topic"
		expectedTopicDLT                  = "test-topic-dlt"
		expectedDefaultReadMessageTimeout = time.Millisecond * 100
		expectedContentMessage            = `{"test": "test", "invalid_json_field": "test"}`
		expectedTypeMessageDLT            = stream.NewTextMessage(`{"test": "test", "invalid_json_field": "test"}`)
		expectedKey                       = "test-key"
		expectedKeyDLT                    = "test-key"
		expectedKafkaMessage              = &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Error: nil,
			},
			Key:   []byte(expectedKey),
			Value: []byte(expectedContentMessage),
			Headers: []kafka.Header{
				{
					Key:   "DEVKIT_CONTENT_TYPE",
					Value: []byte("json"),
				},
			},
		}
	)

	dispatcherMock := &DispatcherMock{}
	dispatcherMock.On("Dispatch", mock.Anything, expectedTopicDLT, expectedKeyDLT, expectedTypeMessageDLT).
		Return(errors.New("unexpected dispatcher error")).
		Once()

	clientMock := &ClientMock{}
	clientMock.
		On("Subscribe", expectedTopic, mock.Anything).
		Return(nil).
		Once().
		On("ReadMessage", expectedDefaultReadMessageTimeout).
		Return(expectedKafkaMessage, nil).
		Once().
		On("Close").
		Return(nil).
		Once()

	factoryMock := &FactoryMock{}
	factoryMock.
		On("New", expectedGroupID).
		Return(clientMock, nil).
		Once()

	handlerMock := &HandlerJsonMock[jsonMessageTest]{}
	handlerMock.
		On("ID").
		Return(expectedHandlerID).
		Once().
		On("Topic").
		Return(expectedTopic).
		Once()

	c, err := consumer.NewConsumer[jsonMessageTest](dispatcherMock, factoryMock, handlerMock)
	assert.Nil(t, err)

	shutdown, err := c.Run()
	assert.Nil(t, err)

	err = <-c.ListenShutdown()
	assert.Equal(t, expectedErr, err)

	shutdown()

	dispatcherMock.AssertExpectations(t)
	clientMock.AssertExpectations(t)
	factoryMock.AssertExpectations(t)
	handlerMock.AssertExpectations(t)
}

func TestConsumer_ReadMessage_HandleKafkaTimeoutError(t *testing.T) {
	var (
		expectedGroupID                   = "devkit-group-id"
		expectedHandlerID                 = "group-id"
		expectedTopic                     = "test-topic"
		expectedDefaultReadMessageTimeout = time.Millisecond * 100
	)

	dispatcherMock := &DispatcherMock{}

	clientMock := &ClientMock{}
	clientMock.
		On("Subscribe", expectedTopic, mock.Anything).
		Return(nil).
		Once().
		On("ReadMessage", expectedDefaultReadMessageTimeout).
		Run(func(args mock.Arguments) {
			time.Sleep(time.Millisecond * 10)
		}).
		Return(&kafka.Message{}, kafka.NewError(kafka.ErrTimedOut, "", false)).
		Once().
		On("Close").
		Return(nil).
		Once()

	factoryMock := &FactoryMock{}
	factoryMock.
		On("New", expectedGroupID).
		Return(clientMock, nil).
		Once()

	handlerMock := &HandlerTextMock[string]{}
	handlerMock.
		On("ID").
		Return(expectedHandlerID).
		Once().
		On("Topic").
		Return(expectedTopic).
		Once()

	c, err := consumer.NewConsumer[string](dispatcherMock, factoryMock, handlerMock)
	assert.Nil(t, err)

	shutdown, err := c.Run()
	assert.Nil(t, err)

	time.Sleep(time.Millisecond * 5)
	shutdown()

	dispatcherMock.AssertExpectations(t)
	clientMock.AssertExpectations(t)
	factoryMock.AssertExpectations(t)
	handlerMock.AssertExpectations(t)
}

func TestConsumer_ReadMessage_HandleKafkaError(t *testing.T) {
	var (
		expectedErr                       = kafka.NewError(kafka.ErrConflict, "", false)
		expectedGroupID                   = "devkit-group-id"
		expectedHandlerID                 = "group-id"
		expectedTopic                     = "test-topic"
		expectedDefaultReadMessageTimeout = time.Millisecond * 100
	)

	dispatcherMock := &DispatcherMock{}

	clientMock := &ClientMock{}
	clientMock.
		On("Subscribe", expectedTopic, mock.Anything).
		Return(nil).
		Once().
		On("ReadMessage", expectedDefaultReadMessageTimeout).
		Return(&kafka.Message{}, expectedErr).
		Once().
		On("Close").
		Return(nil).
		Once()

	factoryMock := &FactoryMock{}
	factoryMock.
		On("New", expectedGroupID).
		Return(clientMock, nil).
		Once()

	handlerMock := &HandlerTextMock[string]{}
	handlerMock.
		On("ID").
		Return(expectedHandlerID).
		Once().
		On("Topic").
		Return(expectedTopic).
		Once()

	c, err := consumer.NewConsumer[string](dispatcherMock, factoryMock, handlerMock)
	assert.Nil(t, err)

	shutdown, err := c.Run()
	assert.Nil(t, err)

	err = <-c.ListenShutdown()
	assert.Equal(t, expectedErr, err)

	shutdown()

	dispatcherMock.AssertExpectations(t)
	clientMock.AssertExpectations(t)
	factoryMock.AssertExpectations(t)
	handlerMock.AssertExpectations(t)
}

func TestConsumer_ReadMessage_HandleNonKafkaError(t *testing.T) {
	var (
		expectedErr                       = errors.New("unexpected non kafka error")
		expectedGroupID                   = "devkit-group-id"
		expectedHandlerID                 = "group-id"
		expectedTopic                     = "test-topic"
		expectedDefaultReadMessageTimeout = time.Millisecond * 100
	)

	dispatcherMock := &DispatcherMock{}

	clientMock := &ClientMock{}
	clientMock.
		On("Subscribe", expectedTopic, mock.Anything).
		Return(nil).
		Once().
		On("ReadMessage", expectedDefaultReadMessageTimeout).
		Return(&kafka.Message{}, expectedErr).
		Once().
		On("Close").
		Return(nil).
		Once()

	factoryMock := &FactoryMock{}
	factoryMock.
		On("New", expectedGroupID).
		Return(clientMock, nil).
		Once()

	handlerMock := &HandlerTextMock[string]{}
	handlerMock.
		On("ID").
		Return(expectedHandlerID).
		Once().
		On("Topic").
		Return(expectedTopic).
		Once()

	c, err := consumer.NewConsumer[string](dispatcherMock, factoryMock, handlerMock)
	assert.Nil(t, err)

	shutdown, err := c.Run()
	assert.Nil(t, err)

	err = <-c.ListenShutdown()
	assert.Equal(t, expectedErr, err)

	shutdown()

	dispatcherMock.AssertExpectations(t)
	clientMock.AssertExpectations(t)
	factoryMock.AssertExpectations(t)
	handlerMock.AssertExpectations(t)
}

func TestConsumer_CommitMessageError(t *testing.T) {
	var (
		expectedErr                       = errors.New("unexpected commit message error")
		expectedGroupID                   = "devkit-group-id"
		expectedHandlerID                 = "group-id"
		expectedTopic                     = "test-topic"
		expectedDefaultReadMessageTimeout = time.Millisecond * 100
		expectedContentMessage            = "test message content"
		expectedKafkaMessage              = &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Error: nil,
			},
			Value: []byte(expectedContentMessage),
			Headers: []kafka.Header{
				{
					Key:   "DEVKIT_CONTENT_TYPE",
					Value: []byte("text"),
				},
			},
		}
	)

	dispatcherMock := &DispatcherMock{}

	clientMock := &ClientMock{}
	clientMock.
		On("Subscribe", expectedTopic, mock.Anything).
		Return(nil).
		Once().
		On("ReadMessage", expectedDefaultReadMessageTimeout).
		Return(expectedKafkaMessage, nil).
		Once().
		On("Close").
		Return(nil).
		Once().
		On("CommitMessage", expectedKafkaMessage).
		Return([]kafka.TopicPartition{}, errors.New("unexpected commit message error")).
		Once()

	factoryMock := &FactoryMock{}
	factoryMock.
		On("New", expectedGroupID).
		Return(clientMock, nil).
		Once()

	handlerMock := &HandlerTextMock[string]{}
	handlerMock.
		On("ID").
		Return(expectedHandlerID).
		Once().
		On("Topic").
		Return(expectedTopic).
		Once().
		On("ShouldSkip", expectedContentMessage).
		Return(false).
		Once().
		On("Handle", mock.Anything, expectedContentMessage).
		Return(nil).
		Once()

	c, err := consumer.NewConsumer[string](dispatcherMock, factoryMock, handlerMock)
	assert.Nil(t, err)

	shutdown, err := c.Run()
	assert.Nil(t, err)

	err = <-c.ListenShutdown()
	assert.Equal(t, expectedErr, err)

	shutdown()

	dispatcherMock.AssertExpectations(t)
	clientMock.AssertExpectations(t)
	factoryMock.AssertExpectations(t)
	handlerMock.AssertExpectations(t)
}

func TestConsumer_FactoryError(t *testing.T) {
	var (
		expectedErr       = errors.New("unexpected factory error")
		expectedGroupID   = "devkit-group-id"
		expectedHandlerID = "group-id"
		expectedTopic     = "topic"
	)

	dispatcherMock := &DispatcherMock{}

	clientMock := &ClientMock{}

	factoryMock := &FactoryMock{}
	factoryMock.
		On("New", expectedGroupID).
		Return(clientMock, errors.New("unexpected factory error")).
		Once()

	handlerMock := &HandlerTextMock[string]{}
	handlerMock.
		On("ID").
		Return(expectedHandlerID).
		Once().
		On("Topic").
		Return(expectedTopic).
		Once()

	c, err := consumer.NewConsumer[string](dispatcherMock, factoryMock, handlerMock)
	assert.Error(t, expectedErr, err)
	assert.Nil(t, c)

	dispatcherMock.AssertExpectations(t)
	clientMock.AssertExpectations(t)
	factoryMock.AssertExpectations(t)
	handlerMock.AssertExpectations(t)
}

func TestConsumer_SubscribeError(t *testing.T) {
	var (
		expectedGroupID   = "devkit-group-id"
		expectedTopic     = "test-topic"
		expectedHandlerID = "group-id"

		expectedErr = errors.New("unexpected subscribe err")
	)

	dispatcherMock := &DispatcherMock{}

	clientMock := &ClientMock{}
	clientMock.
		On("Subscribe", expectedTopic, mock.Anything).
		Return(errors.New("unexpected subscribe err")).
		Once()

	factoryMock := &FactoryMock{}
	factoryMock.
		On("New", expectedGroupID).
		Return(clientMock, nil).
		Once()

	handlerMock := &HandlerTextMock[string]{}
	handlerMock.
		On("ID").
		Return(expectedHandlerID).
		Once().
		On("Topic").
		Return(expectedTopic).
		Once()

	c, err := consumer.NewConsumer[string](dispatcherMock, factoryMock, handlerMock)
	assert.Nil(t, err)

	shutdown, err := c.Run()
	assert.Nil(t, shutdown)
	assert.Error(t, expectedErr, err)

	dispatcherMock.AssertExpectations(t)
	clientMock.AssertExpectations(t)
	factoryMock.AssertExpectations(t)
	handlerMock.AssertExpectations(t)
}

type logHandler[T string] struct{}

func (l logHandler[T]) ID() string {
	return "log-handler"
}

func (l logHandler[T]) Topic() string {
	return "log-handler"
}

func (l logHandler[T]) ShouldSkip(content string) bool {
	return false
}

func (l logHandler[T]) Handle(ctx context.Context, content string) error {
	log.Info(ctx, "log-test")

	time.Sleep(time.Millisecond * 2)
	return nil
}

func (l logHandler[T]) ConfigRetry() consumer.ConfigRetry {
	return consumer.ConfigRetry{}
}

func TestConsumer_DefaultLogger_WithTraceID(t *testing.T) {
	var (
		expectedGroupID                   = "devkit-log-handler"
		expectedTopic                     = "log-handler"
		expectedDefaultReadMessageTimeout = time.Millisecond * 100
		expectedContentMessage            = "test message content"
		expectedKafkaMessage              = &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Error: nil,
			},
			Value: []byte(expectedContentMessage),
			Headers: []kafka.Header{
				{
					Key:   "DEVKIT_CONTENT_TYPE",
					Value: []byte("text"),
				},
			},
		}
	)

	dispatcherMock := &DispatcherMock{}

	committed := make(chan struct{}, 2)

	clientMock := &ClientMock{}
	clientMock.
		On("Subscribe", expectedTopic, mock.Anything).
		Return(nil).
		Once().
		On("ReadMessage", expectedDefaultReadMessageTimeout).
		Return(expectedKafkaMessage, nil).
		Twice().
		// Any poll past the two expected messages answers as an idle broker
		// would, so the consumer looping once more while shutting down is
		// harmless instead of exhausting the mock.
		On("ReadMessage", expectedDefaultReadMessageTimeout).
		Return(&kafka.Message{}, kafka.NewError(kafka.ErrTimedOut, "", false)).
		On("Close").
		Return(nil).
		Once().
		On("CommitMessage", expectedKafkaMessage).
		Run(func(mock.Arguments) {
			committed <- struct{}{}
		}).
		Return([]kafka.TopicPartition{}, nil).
		Twice()

	factoryMock := &FactoryMock{}
	factoryMock.
		On("New", expectedGroupID).
		Return(clientMock, nil).
		Once()

	testOutput := bytes.NewBuffer(nil)
	testLogger := log.New(
		log.WithOutput(testOutput),
	)

	c, err := consumer.NewConsumer[string](
		dispatcherMock,
		factoryMock,
		logHandler[string]{},
		consumer.WithLogger(testLogger),
		consumer.WithStringGenerator(gen.SequenceGenerator()),
	)
	assert.Nil(t, err)

	shutdown, err := c.Run()
	assert.Nil(t, err)

	waitForCalls(t, committed, 2)
	shutdown()
	assert.Nil(t, err)

	assert.Contains(t, testOutput.String(), "trace-id=1")
	assert.Contains(t, testOutput.String(), "trace-id=2")
	assert.Contains(t, testOutput.String(), "level=info msg=log-test")
	assert.Contains(t, testOutput.String(), "level=info msg=log-test")

	dispatcherMock.AssertExpectations(t)
	clientMock.AssertExpectations(t)
	factoryMock.AssertExpectations(t)
}
