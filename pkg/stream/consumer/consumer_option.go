package consumer

import (
	"github.com/diegodesousas/go-devkit/pkg/gen"
	"github.com/diegodesousas/go-devkit/pkg/log"
)

// deadLetterSuffix is appended to the source topic when no dead letter topic
// is configured.
const deadLetterSuffix = "dlt"

type settings[T any] struct {
	logger          log.Logger
	stringGenerator gen.StringGenerator
	skip            func(T) bool
	retry           ConfigRetry
	deadLetterTopic string
}

// Option configures a Consumer built by New.
type Option[T any] func(s settings[T]) settings[T]

// WithLogger sets the base logger. The Consumer derives from it per record,
// adding the topic, partition, offset and a trace id. Defaults to log.New().
func WithLogger[T any](logger log.Logger) Option[T] {
	return func(s settings[T]) settings[T] {
		s.logger = logger

		return s
	}
}

// WithStringGenerator sets the source of per-record trace ids. Defaults to
// gen.UUIDGenerator; inject gen.SequenceGenerator in tests.
func WithStringGenerator[T any](generator gen.StringGenerator) Option[T] {
	return func(s settings[T]) settings[T] {
		s.stringGenerator = generator

		return s
	}
}

// WithSkip installs a predicate consulted before the handler. Returning true
// commits the record without processing it.
func WithSkip[T any](skip func(T) bool) Option[T] {
	return func(s settings[T]) settings[T] {
		s.skip = skip

		return s
	}
}

// WithRetry sets the retry policy. Without it, no error is retried and the
// first failure sends the record to the dead letter topic.
func WithRetry[T any](retry ConfigRetry) Option[T] {
	return func(s settings[T]) settings[T] {
		s.retry = retry

		return s
	}
}

// WithDeadLetterTopic overrides the dead letter topic name. Defaults to the
// source topic of the record suffixed with "-dlt".
func WithDeadLetterTopic[T any](topic string) Option[T] {
	return func(s settings[T]) settings[T] {
		s.deadLetterTopic = topic

		return s
	}
}
