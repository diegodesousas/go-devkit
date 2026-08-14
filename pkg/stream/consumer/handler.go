package consumer

import (
	"context"
	"time"
)

// Handler is the user code a Consumer drives.
//
// ID names the consumer group, Topic the topic to read. ShouldSkip is consulted
// before Handle and lets a message be acknowledged without processing.
// ConfigRetry declares which errors are worth retrying and how long to keep
// trying; it is consulted on every failure, so it must be cheap.
//
// Handle receives a context carrying a logger scoped to the message. Returning
// nil commits the offset.
type Handler[T any] interface {
	ID() string
	Topic() string
	ShouldSkip(content T) bool
	Handle(ctx context.Context, content T) error
	ConfigRetry() ConfigRetry
}

// ConfigRetry is a handler retry policy.
//
// Only errors matching RetryableErrors under errors.Is are retried; anything
// else goes straight to the dead letter topic. The remaining fields configure
// the exponential backoff, and a retry sequence that exceeds MaxElapsedTime is
// abandoned to the dead letter topic as well.
//
// The zero value retries nothing. Build one with NewConfigRetry or as a
// literal.
type ConfigRetry struct {
	RetryableErrors []error
	InitialInterval time.Duration
	MaxElapsedTime  time.Duration
	MaxInterval     time.Duration
}

type configRetrySettings struct {
	retryableErrors []error
	initialInterval time.Duration
	maxElapsedTime  time.Duration
	maxInterval     time.Duration
}

// ConfigRetryOption configures a ConfigRetry built by NewConfigRetry.
type ConfigRetryOption func(retrySettings configRetrySettings) configRetrySettings

// WithRetryableErrors sets the errors worth retrying. They are matched with
// errors.Is, so sentinels survive wrapping.
func WithRetryableErrors(errs ...error) ConfigRetryOption {
	return func(retrySettings configRetrySettings) configRetrySettings {
		retrySettings.retryableErrors = errs

		return retrySettings
	}
}

// WithInitialInterval sets the delay before the first retry.
func WithInitialInterval(initialInterval time.Duration) ConfigRetryOption {
	return func(retrySettings configRetrySettings) configRetrySettings {
		retrySettings.initialInterval = initialInterval

		return retrySettings
	}
}

// WithMaxElapsedTime caps how long retrying may go on before the message is
// sent to the dead letter topic.
//
// The consumer does not poll while retrying, so a value beyond the broker
// session timeout gets the consumer evicted from its group.
func WithMaxElapsedTime(maxElapsedTime time.Duration) ConfigRetryOption {
	return func(retrySettings configRetrySettings) configRetrySettings {
		retrySettings.maxElapsedTime = maxElapsedTime

		return retrySettings
	}
}

// WithMaxInterval caps the delay between retries as the backoff grows.
func WithMaxInterval(maxInterval time.Duration) ConfigRetryOption {
	return func(retrySettings configRetrySettings) configRetrySettings {
		retrySettings.maxInterval = maxInterval

		return retrySettings
	}
}

// NewConfigRetry builds a ConfigRetry from options.
func NewConfigRetry(options ...ConfigRetryOption) ConfigRetry {
	var configRetry configRetrySettings

	for _, option := range options {
		configRetry = option(configRetry)
	}

	return ConfigRetry{
		RetryableErrors: configRetry.retryableErrors,
		InitialInterval: configRetry.initialInterval,
		MaxElapsedTime:  configRetry.maxElapsedTime,
		MaxInterval:     configRetry.maxInterval,
	}
}
