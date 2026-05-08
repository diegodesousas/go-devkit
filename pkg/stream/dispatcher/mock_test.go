package dispatcher_test

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/mock"
)

type ClientDispatcherClientMock struct {
	mock.Mock
}

func (c *ClientDispatcherClientMock) Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error {
	arguments := c.Called(msg, deliveryChan)

	return arguments.Error(0)
}

func (c *ClientDispatcherClientMock) Close() {
	c.Called()
}

func (c *ClientDispatcherClientMock) Flush(timeoutMs int) int {
	arguments := c.Called(timeoutMs)

	return arguments.Int(0)
}
