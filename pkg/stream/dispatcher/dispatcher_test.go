package dispatcher_test

import (
	"context"
	"testing"

	"github.com/diegodesousas/go-devkit/pkg/stream"
	"github.com/diegodesousas/go-devkit/pkg/stream/dispatcher"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestDispatch_Success(t *testing.T) {
	var (
		expectedTopic = "orders"
		expectedKey   = "order-1"
	)

	writer := &writerMock{}
	writer.
		On("Produce", mock.Anything, mock.MatchedBy(func(record stream.Record) bool {
			contentType, ok := record.Header(stream.ContentTypeHeaderKey)

			return record.Topic == expectedTopic &&
				string(record.Key) == expectedKey &&
				ok && string(contentType) == "json"
		})).
		Return(nil).
		Once()

	d := dispatcher.New(writer)

	err := d.Dispatch(context.Background(), expectedTopic, expectedKey, stream.NewJSONMessage(map[string]string{"id": "1"}))

	assert.Nil(t, err)
	writer.AssertExpectations(t)
}

func TestDispatch_SerializeError(t *testing.T) {
	writer := &writerMock{}

	d := dispatcher.New(writer)

	// A channel cannot be marshalled to JSON.
	err := d.Dispatch(context.Background(), "orders", "key", stream.NewJSONMessage(make(chan int)))

	assert.NotNil(t, err)
	writer.AssertNotCalled(t, "Produce", mock.Anything, mock.Anything)
}

func TestDispatch_ProduceError(t *testing.T) {
	expectedErr := errors.New("broker down")

	writer := &writerMock{}
	writer.On("Produce", mock.Anything, mock.Anything).Return(expectedErr).Once()

	d := dispatcher.New(writer)

	err := d.Dispatch(context.Background(), "orders", "key", stream.NewJSONMessage(map[string]string{}))

	assert.ErrorIs(t, err, expectedErr)
	writer.AssertExpectations(t)
}

func TestDispatch_InjectsTraceHeaders(t *testing.T) {
	var captured stream.Record

	writer := &writerMock{}
	writer.
		On("Produce", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			captured = args.Get(1).(stream.Record)
		}).
		Return(nil).
		Once()

	d := dispatcher.New(writer)

	err := d.Dispatch(context.Background(), "orders", "key", stream.NewJSONMessage(map[string]string{}))

	assert.Nil(t, err)
	// Beyond the content type, the span context must ride along so the
	// consumer can continue the trace.
	assert.Greater(t, len(captured.Headers), 1)
}

func TestClose(t *testing.T) {
	writer := &writerMock{}
	writer.On("Flush", mock.Anything).Return(nil).Once()
	writer.On("Close").Return(nil).Once()

	d := dispatcher.New(writer)

	assert.Nil(t, d.Close(context.Background()))
	writer.AssertExpectations(t)
}

func TestClose_FlushError(t *testing.T) {
	expectedErr := errors.New("flush failed")

	writer := &writerMock{}
	writer.On("Flush", mock.Anything).Return(expectedErr).Once()
	writer.On("Close").Return(nil).Once()

	d := dispatcher.New(writer)

	err := d.Close(context.Background())

	// Close still closes the writer, but reports what was lost.
	assert.ErrorIs(t, err, expectedErr)
	writer.AssertExpectations(t)
}
