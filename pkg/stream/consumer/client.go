package consumer

import (
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Client interface {
	Subscribe(topic string, cb kafka.RebalanceCb) error
	ReadMessage(timeout time.Duration) (*kafka.Message, error)
	CommitMessage(m *kafka.Message) ([]kafka.TopicPartition, error)
	Close() error
}

type Factory interface {
	New(groupID string) (Client, error)
}

type clientSettings struct {
	bootstrapServer  string
	sessionTimeoutMs int
	offsetReset      string
	autoCommit       bool
}

type factory struct {
	settings clientSettings
}

func (f factory) New(groupID string) (Client, error) {
	cfg := &kafka.ConfigMap{
		"group.id":           groupID,
		"client.id":          groupID,
		"bootstrap.servers":  f.settings.bootstrapServer,
		"session.timeout.ms": f.settings.sessionTimeoutMs,
		"auto.offset.reset":  f.settings.offsetReset,
		"enable.auto.commit": f.settings.autoCommit,
	}

	return kafka.NewConsumer(cfg)
}

func NewFactory(options ...ClientOption) Factory {
	s := clientSettings{
		sessionTimeoutMs: 45000,
		offsetReset:      "earliest",
		autoCommit:       false,
	}

	for _, option := range options {
		s = option(s)
	}

	return &factory{
		settings: s,
	}
}
