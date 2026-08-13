package consumer_test

import (
	"context"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"

	"github.com/diegodesousas/go-devkit/pkg/stream"
	"github.com/diegodesousas/go-devkit/pkg/stream/consumer"
	"github.com/stretchr/testify/mock"
)

type DispatcherMock struct {
	mock.Mock
}

func (d *DispatcherMock) Dispatch(ctx context.Context, topic string, key string, content stream.Message) error {
	arguments := d.Called(ctx, topic, key, content)

	return arguments.Error(0)
}

func (d *DispatcherMock) Shutdown() {
	_ = d.Called()
}

type FactoryMock struct {
	mock.Mock
}

func (f *FactoryMock) New(groupID string) (consumer.Client, error) {
	arguments := f.Called(groupID)

	return arguments.Get(0).(consumer.Client), arguments.Error(1)
}

type ClientMock struct {
	mock.Mock
}

func (c *ClientMock) Subscribe(topic string, cb kafka.RebalanceCb) error {
	arguments := c.Called(topic, cb)

	return arguments.Error(0)
}

func (c *ClientMock) ReadMessage(timeout time.Duration) (*kafka.Message, error) {
	arguments := c.Called(timeout)

	return arguments.Get(0).(*kafka.Message), arguments.Error(1)
}

func (c *ClientMock) CommitMessage(m *kafka.Message) ([]kafka.TopicPartition, error) {
	arguments := c.Called(m)

	return arguments.Get(0).([]kafka.TopicPartition), arguments.Error(1)
}

func (c *ClientMock) Close() error {
	arguments := c.Called()

	return arguments.Error(0)
}

type HandlerTextMock[T string] struct {
	mock.Mock
}

func (h *HandlerTextMock[T]) ID() string {
	arguments := h.Called()

	return arguments.String(0)
}

func (h *HandlerTextMock[T]) Topic() string {
	arguments := h.Called()

	return arguments.String(0)
}

func (h *HandlerTextMock[T]) ShouldSkip(content string) bool {
	arguments := h.Called(content)

	return arguments.Bool(0)
}

func (h *HandlerTextMock[T]) Handle(ctx context.Context, content string) error {
	arguments := h.Called(ctx, content)

	return arguments.Error(0)
}

func (h *HandlerTextMock[T]) ConfigRetry() consumer.ConfigRetry {
	arguments := h.Called()

	return arguments.Get(0).(consumer.ConfigRetry)
}

type jsonMessageTest struct {
	Test             string        `json:"test"`
	InvalidJsonField chan struct{} `json:"invalid_json_field"`
}

type HandlerJsonMock[T jsonMessageTest] struct {
	mock.Mock
}

func (h *HandlerJsonMock[T]) ID() string {
	arguments := h.Called()

	return arguments.String(0)
}

func (h *HandlerJsonMock[T]) Topic() string {
	arguments := h.Called()

	return arguments.String(0)
}

func (h *HandlerJsonMock[T]) ShouldSkip(content jsonMessageTest) bool {
	arguments := h.Called(content)

	return arguments.Bool(0)
}

func (h *HandlerJsonMock[T]) Handle(ctx context.Context, content jsonMessageTest) error {
	arguments := h.Called(ctx, content)

	return arguments.Error(0)
}

func (h *HandlerJsonMock[T]) ConfigRetry() consumer.ConfigRetry {
	arguments := h.Called()

	return arguments.Get(0).(consumer.ConfigRetry)
}
