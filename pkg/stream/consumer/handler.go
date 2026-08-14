package consumer

import (
	"context"
	"time"
)

// Handler is the user code a Consumer drives.
//
// It has one method. Everything the previous contract demanded - the group id,
// the topic, whether to skip, the retry policy - moved to where it belongs:
// the group and topic are parameters of the reader, which is what actually
// subscribes, and skipping and retrying are policy, configured with options.
//
// Handle receives a context carrying a logger scoped to the record and the
// trace continued from the producer. The context is cancelled when the
// consumer shuts down, so a long handler should honour it.
//
// Handle is called concurrently - one goroutine per partition in the batch
// being processed, serial within each - so a handler holding state must be
// safe for concurrent use.
//
// Returning nil marks the record processed and allows its offset to be
// committed.
type Handler[T any] interface {
	Handle(ctx context.Context, content T) error
}

// ConfigRetry is a handler retry policy.
//
// Only errors matching RetryableErrors under errors.Is are retried; anything
// else goes straight to the dead letter topic. The remaining fields configure
// the exponential backoff, and a retry sequence that exceeds MaxElapsedTime is
// abandoned to the dead letter topic as well.
//
// A zero MaxElapsedTime is not "no limit": the backoff applies its own default
// of 15 minutes. Set it explicitly - see WithMaxElapsedTime.
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

// WithInitialInterval sets the first interval of the backoff, from which the
// delays grow exponentially.
//
// It is not a delay before the first retry: the backoff waits between attempts,
// so the first retry follows the failure immediately.
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
// session timeout gets the consumer evicted from its group. Leaving it unset
// does not avoid that: the backoff then falls back to its own default of 15
// minutes, which is beyond any session timeout. A policy that retries anything
// should say how long for.
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
