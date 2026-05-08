package consumer

import (
	"context"
	"time"
)

type Handler[T any] interface {
	ID() string
	Topic() string
	ShouldSkip(content T) bool
	Handle(ctx context.Context, content T) error
	ConfigRetry() ConfigRetry
}

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

type ConfigRetryOption func(retrySettings configRetrySettings) configRetrySettings

func WithRetryableErrors(errs ...error) ConfigRetryOption {
	return func(retrySettings configRetrySettings) configRetrySettings {
		retrySettings.retryableErrors = errs

		return retrySettings
	}
}

func WithInitialInterval(initialInterval time.Duration) ConfigRetryOption {
	return func(retrySettings configRetrySettings) configRetrySettings {
		retrySettings.initialInterval = initialInterval

		return retrySettings
	}
}

func WithMaxElapsedTime(maxElapsedTime time.Duration) ConfigRetryOption {
	return func(retrySettings configRetrySettings) configRetrySettings {
		retrySettings.maxElapsedTime = maxElapsedTime

		return retrySettings
	}
}

func WithMaxInterval(maxInterval time.Duration) ConfigRetryOption {
	return func(retrySettings configRetrySettings) configRetrySettings {
		retrySettings.maxInterval = maxInterval

		return retrySettings
	}
}

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
