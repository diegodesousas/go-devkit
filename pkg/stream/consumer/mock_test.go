package consumer_test

import (
	"context"

	"github.com/diegodesousas/go-devkit/pkg/stream"
	"github.com/stretchr/testify/mock"
)

type readerMock struct {
	mock.Mock
}

func (r *readerMock) Poll(ctx context.Context) ([]stream.Record, error) {
	args := r.Called(ctx)

	records, _ := args.Get(0).([]stream.Record)

	return records, args.Error(1)
}

func (r *readerMock) Commit(ctx context.Context, records ...stream.Record) error {
	args := r.Called(ctx, records)

	return args.Error(0)
}

func (r *readerMock) Close() error {
	args := r.Called()

	return args.Error(0)
}

type dispatcherMock struct {
	mock.Mock
}

func (d *dispatcherMock) Dispatch(ctx context.Context, topic, key string, content stream.Message) error {
	args := d.Called(ctx, topic, key, content)

	return args.Error(0)
}

func (d *dispatcherMock) Close(ctx context.Context) error {
	args := d.Called(ctx)

	return args.Error(0)
}

type handlerMock struct {
	mock.Mock
}

func (h *handlerMock) Handle(ctx context.Context, content string) error {
	args := h.Called(ctx, content)

	return args.Error(0)
}
