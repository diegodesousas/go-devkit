package dispatcher_test

import (
	"context"

	"github.com/diegodesousas/go-devkit/pkg/stream"
	"github.com/stretchr/testify/mock"
)

type writerMock struct {
	mock.Mock
}

func (w *writerMock) Produce(ctx context.Context, record stream.Record) error {
	args := w.Called(ctx, record)

	return args.Error(0)
}

func (w *writerMock) Flush(ctx context.Context) error {
	args := w.Called(ctx)

	return args.Error(0)
}

func (w *writerMock) Close() error {
	args := w.Called()

	return args.Error(0)
}
