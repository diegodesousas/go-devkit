package consumer_test

import (
    "errors"
    "testing"
    "time"

    "github.com/diegodesousas/go-devkit/pkg/stream/consumer"
    "github.com/stretchr/testify/assert"
)

func TestNewConfigRetry(t *testing.T) {
    type args struct {
        options []consumer.ConfigRetryOption
    }
    tests := []struct {
        name     string
        args     args
        expected consumer.ConfigRetry
    }{
        {
            name: "Success with retryable errors",
            args: args{
                options: []consumer.ConfigRetryOption{
                    consumer.WithRetryableErrors(
                        errors.New("expected error 1"),
                        errors.New("expected error 2"),
                    ),
                },
            },
            expected: consumer.ConfigRetry{
                RetryableErrors: []error{
                    errors.New("expected error 1"),
                    errors.New("expected error 2"),
                },
            },
        },
        {
            name: "Success with initial interval",
            args: args{
                options: []consumer.ConfigRetryOption{
                    consumer.WithInitialInterval(time.Second),
                },
            },
            expected: consumer.ConfigRetry{
                InitialInterval: time.Second,
            },
        },
        {
            name: "Success with max elapsed time",
            args: args{
                options: []consumer.ConfigRetryOption{
                    consumer.WithMaxElapsedTime(time.Minute),
                },
            },
            expected: consumer.ConfigRetry{
                MaxElapsedTime: time.Minute,
            },
        },
        {
            name: "Success with max interval",
            args: args{
                options: []consumer.ConfigRetryOption{
                    consumer.WithMaxInterval(time.Hour),
                },
            },
            expected: consumer.ConfigRetry{
                MaxInterval: time.Hour,
            },
        },
    }
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            assert.Equal(t, tt.expected, consumer.NewConfigRetry(tt.args.options...))
        })
    }
}
