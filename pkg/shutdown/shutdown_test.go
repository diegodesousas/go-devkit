package shutdown_test

import (
	"context"
	"testing"
	"time"

	"github.com/diegodesousas/go-devkit/pkg/shutdown"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestGraceful(t *testing.T) {
	var (
		errServer   = errors.New("server shutdown failed")
		errDatabase = errors.New("database close failed")
	)

	tests := []struct {
		name       string
		stepErrs   []error
		wantErr    assert.ErrorAssertionFunc
		wantCalled []int
	}{
		{
			name:       "runs every step in order",
			stepErrs:   []error{nil, nil, nil},
			wantErr:    assert.NoError,
			wantCalled: []int{0, 1, 2},
		},
		{
			name:     "keeps running remaining steps after a failure",
			stepErrs: []error{errServer, nil},
			wantErr: func(t assert.TestingT, err error, _ ...any) bool {
				return assert.ErrorIs(t, err, errServer)
			},
			wantCalled: []int{0, 1},
		},
		{
			name:     "joins errors from every failing step",
			stepErrs: []error{errServer, errDatabase},
			wantErr: func(t assert.TestingT, err error, _ ...any) bool {
				return assert.ErrorIs(t, err, errServer) && assert.ErrorIs(t, err, errDatabase)
			},
			wantCalled: []int{0, 1},
		},
		{
			name:       "no steps is a no-op",
			stepErrs:   nil,
			wantErr:    assert.NoError,
			wantCalled: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var called []int
			steps := make([]shutdown.Step, len(tt.stepErrs))
			for i, stepErr := range tt.stepErrs {
				steps[i] = func(context.Context) error {
					called = append(called, i)
					return stepErr
				}
			}

			err := shutdown.Graceful(context.Background(), time.Second, steps...)

			tt.wantErr(t, err)
			assert.Equal(t, tt.wantCalled, called)
		})
	}
}

func TestGracefulStepsShareTheTimeout(t *testing.T) {
	var (
		expectedTimeout = 50 * time.Millisecond
		hasDeadline     bool
		laterStepRan    bool
	)

	start := time.Now()
	err := shutdown.Graceful(context.Background(), expectedTimeout,
		func(ctx context.Context) error {
			_, hasDeadline = ctx.Deadline()

			<-ctx.Done() // simulates a server that never finishes draining
			return ctx.Err()
		},
		func(context.Context) error {
			laterStepRan = true
			return nil
		},
	)

	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.True(t, hasDeadline, "step context must have a deadline")
	assert.True(t, laterStepRan, "later steps must still run after a timeout")
	assert.Less(t, time.Since(start), time.Second, "shutdown must not hang past the timeout")
}
