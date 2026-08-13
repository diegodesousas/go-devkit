package consumer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/diegodesousas/go-devkit/pkg/gen"
	"github.com/diegodesousas/go-devkit/pkg/log"
	"github.com/diegodesousas/go-devkit/pkg/stream"
	"github.com/diegodesousas/go-devkit/pkg/stream/dispatcher"
	"github.com/pkg/errors"
)

const (
	pollInterval     = time.Millisecond * 100
	deadLetterSuffix = "dlt"
	uniqueNamePrefix = "devkit"
	loggerTraceKey   = "trace-id"
)

type Shutdown func()

type Consumer interface {
	Run() (Shutdown, error)
	ListenShutdown() <-chan error
}

type defaultConsumer[T any] struct {
	client           Client
	dispatcher       dispatcher.Dispatcher
	handler          Handler[T]
	shutdownStarted  chan struct{}
	shutdownFinished chan struct{}
	shutdownListener chan error
	logger           log.Logger
	stringGenerator  gen.StringGenerator
	groupID          string
	topic            string
}

func New[T any](
	dispatcher dispatcher.Dispatcher,
	factory Factory,
	handler Handler[T],
	options ...Option,
) (Consumer, error) {
	groupID := handler.ID()
	topic := handler.Topic()

	uniqueName := fmt.Sprintf("%s-%s", uniqueNamePrefix, groupID)

	client, err := factory.New(uniqueName)
	if err != nil {
		return nil, err
	}

	s := settings{
		logger:          log.New(),
		stringGenerator: gen.UUIDGenerator(),
	}

	for _, option := range options {
		s = option(s)
	}

	return &defaultConsumer[T]{
		groupID:          groupID,
		topic:            topic,
		handler:          handler,
		client:           client,
		dispatcher:       dispatcher,
		shutdownStarted:  make(chan struct{}, 1),
		shutdownFinished: make(chan struct{}, 1),
		shutdownListener: make(chan error, 1),
		logger: s.logger.WithFields(
			log.NewField("consumer-group", groupID),
			log.NewField("consumer-topic", topic),
		),
		stringGenerator: s.stringGenerator,
	}, nil
}

func (c defaultConsumer[T]) Run() (Shutdown, error) {
	if err := c.client.Subscribe(c.topic, nil); err != nil {
		return nil, err
	}

	go func() {
		waitGroup := &sync.WaitGroup{}

		for {
			select {
			case <-c.shutdownStarted:
				waitGroup.Wait()
				c.shutdownFinished <- struct{}{}
				return
			default:
				waitGroup.Add(1)
				if err := c.readMessage(waitGroup); err != nil {
					go c.startingShutdownInternally(err)

					return
				}
			}
		}
	}()

	return func() {
		c.shutdown()
	}, nil
}

func (c defaultConsumer[T]) startingShutdownInternally(err error) {
	c.shutdownListener <- err

	<-c.shutdownStarted

	c.shutdownFinished <- struct{}{}
}

func (c defaultConsumer[T]) ListenShutdown() <-chan error {
	return c.shutdownListener
}

func (c defaultConsumer[T]) processMessage(ctx context.Context, message *kafka.Message) error {
	typeMessage, err := stream.NewMessageType(message)
	if err != nil {
		return c.dispatchToDeadLetterTopicAsText(ctx, message)
	}

	var content T

	if err := typeMessage.Deserialize(message.Value, &content); err != nil {
		return c.dispatchToDeadLetterTopicAsText(ctx, message)
	}

	if c.handler.ShouldSkip(content) {
		log.Debug(ctx, "message skipped")
		return nil
	}

	log.Debug(ctx, "running handler")
	err = c.handler.Handle(ctx, content)
	if err == nil {
		log.Debug(ctx, "handler execution successful")
		return nil
	}

	configRetry := c.handler.ConfigRetry()

	log.Error(ctx, err)

	if !c.shouldRetry(err, configRetry.RetryableErrors) {
		return c.dispatchToDeadLetterTopic(ctx, content, message, typeMessage)
	}

	if err = c.runRetry(ctx, content); err != nil {
		return c.dispatchToDeadLetterTopic(ctx, content, message, typeMessage)
	}

	log.Info(ctx, "handler execution successful with retry")

	return nil
}

func (c defaultConsumer[T]) shouldRetry(err error, list []error) bool {
	for _, currentErr := range list {
		if errors.Is(err, currentErr) {
			return true
		}
	}

	return false
}

func (c defaultConsumer[T]) runRetry(ctx context.Context, content T) error {
	attempts := 0
	operation := func() error {
		attempts = attempts + 1
		log.Debug(ctx, "retrying execution", log.NewField("attempt", attempts))
		return c.handler.Handle(ctx, content)
	}

	configRetry := c.handler.ConfigRetry()

	backOff := backoff.NewExponentialBackOff()

	time.Sleep(configRetry.InitialInterval)

	backOff.InitialInterval = configRetry.InitialInterval
	backOff.MaxElapsedTime = configRetry.MaxElapsedTime
	backOff.MaxInterval = configRetry.MaxInterval

	if err := backoff.Retry(operation, backOff); err != nil {
		return err
	}

	return nil
}

func (c defaultConsumer[T]) dispatchToDeadLetterTopic(
	ctx context.Context,
	content T,
	message *kafka.Message,
	typeMessage stream.Message,
) error {
	topicDLT := c.deadLetterTopic()

	if err := c.dispatcher.Dispatch(ctx, topicDLT, string(message.Key), typeMessage.NewWithData(content)); err != nil {
		return err
	}

	log.Warn(ctx, "message sent to dead letter topic", log.WarningTypeCritical)

	return nil
}

func (c defaultConsumer[T]) deadLetterTopic() string {
	return fmt.Sprintf("%s-%s", c.topic, deadLetterSuffix)
}

func (c defaultConsumer[T]) dispatchToDeadLetterTopicAsText(
	ctx context.Context,
	message *kafka.Message,
) error {
	topicDLT := c.deadLetterTopic()

	txtMessage := stream.NewTextMessage(string(message.Value))

	if err := c.dispatcher.Dispatch(ctx, topicDLT, string(message.Key), txtMessage); err != nil {
		return err
	}

	log.Warn(ctx, "message sent to dead letter topic", log.WarningTypeCritical)

	return nil
}

func (c defaultConsumer[T]) readMessage(group *sync.WaitGroup) error {
	defer group.Done()

	msg, err := c.client.ReadMessage(pollInterval)
	if err != nil {
		var kafkaErr kafka.Error
		if errors.As(err, &kafkaErr) && kafkaErr.Code() == kafka.ErrTimedOut {
			return nil
		}

		return err
	}

	ctx := log.WithLogger(
		context.Background(),
		c.createLogger().WithFields(
			log.NewField("message-offset", msg.TopicPartition.Offset),
			log.NewField("message-partition", msg.TopicPartition.Partition),
		),
	)

	log.Debug(ctx, "starting read message")

	if err := c.processMessage(ctx, msg); err != nil {
		log.Error(ctx, errors.Wrap(err, "error when processing message"))
		return err
	}

	if _, err := c.client.CommitMessage(msg); err != nil {
		log.Error(ctx, errors.Wrap(err, "error when trying to commit message"))
		return err
	}

	log.Debug(ctx, "message committed")

	return nil
}

func (c defaultConsumer[T]) createLogger() log.Logger {
	return c.logger.WithFields(
		log.NewField(
			loggerTraceKey,
			c.stringGenerator(),
		),
	)
}

func (c defaultConsumer[T]) shutdown() {
	defer func() { _ = c.client.Close() }()

	c.shutdownStarted <- struct{}{}

	<-c.shutdownFinished
}
