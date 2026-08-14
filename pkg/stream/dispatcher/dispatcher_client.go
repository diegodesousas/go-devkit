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

// Client is the Kafka producer driver a Dispatcher writes through.
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

// Option configures the producer built by NewClient.
type Option func(settings dispatcherSettings) dispatcherSettings

// WithBootstrapServers sets the broker list, as a comma-separated
// "host:port" string.
func WithBootstrapServers(hosts string) Option {
	return func(settings dispatcherSettings) dispatcherSettings {
		settings.bootstrapServers = hosts
		return settings
	}
}

// WithLogLevel sets the librdkafka syslog level. Defaults to 6 (informational).
func WithLogLevel(logLevel int) Option {
	return func(settings dispatcherSettings) dispatcherSettings {
		settings.logLevel = logLevel
		return settings
	}
}

// WithDispatchTimeoutMs sets message.timeout.ms, the deadline for a produced
// message to be acknowledged. Defaults to 1000, far below the librdkafka
// default of 300000, so a brief broker hiccup surfaces as
// stream.ErrProcessMessageTimedOut rather than being ridden out.
func WithDispatchTimeoutMs(interval int) Option {
	return func(settings dispatcherSettings) dispatcherSettings {
		settings.messageTimeoutMs = interval
		return settings
	}
}

type dispatcherClient struct {
	*kafka.Producer
}

// NewClient returns a Kafka producer configured for ordering and durability:
// acks=all with a single in-flight request, so messages to a partition cannot
// be reordered by a retry.
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
