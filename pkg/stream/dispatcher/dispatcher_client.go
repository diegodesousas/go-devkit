package dispatcher

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	defaultLogLevel         = 6
	defaultMessageTimeoutMs = 1000
	defaultMaxInFlight      = 1
	defaultAcks             = "all"
)

type Client interface {
	Produce(msg *kafka.Message, deliveryChan chan kafka.Event) error
	Close()
	Flush(timeoutMs int) int
}

type dispatcherSettings struct {
	bootstrapServers string
	logLevel         int
	messageTimeoutMs int
}

type Option func(settings dispatcherSettings) dispatcherSettings

func WithBootstrapServers(hosts string) Option {
	return func(settings dispatcherSettings) dispatcherSettings {
		settings.bootstrapServers = hosts
		return settings
	}
}

func WithLogLevel(logLevel int) Option {
	return func(settings dispatcherSettings) dispatcherSettings {
		settings.logLevel = logLevel
		return settings
	}
}

func WithDispatchTimeoutMs(interval int) Option {
	return func(settings dispatcherSettings) dispatcherSettings {
		settings.messageTimeoutMs = interval
		return settings
	}
}

type dispatcherClient struct {
	*kafka.Producer
}

func NewClient(options ...Option) (Client, error) {
	settings := dispatcherSettings{
		logLevel:         defaultLogLevel,
		messageTimeoutMs: defaultMessageTimeoutMs,
	}

	for _, option := range options {
		settings = option(settings)
	}

	config := &kafka.ConfigMap{
		"bootstrap.servers":  settings.bootstrapServers,
		"log_level":          settings.logLevel,
		"message.timeout.ms": settings.messageTimeoutMs,
		"acks":               defaultAcks,
		"max.in.flight":      defaultMaxInFlight,
	}

	producer, err := kafka.NewProducer(config)
	if err != nil {
		return nil, err
	}

	return &dispatcherClient{
		producer,
	}, nil
}
