package stream_test

import (
	"testing"
	"time"

	"github.com/diegodesousas/go-devkit/pkg/stream"
	"github.com/stretchr/testify/assert"
)

func TestRecord_Header(t *testing.T) {
	record := stream.Record{
		Topic:     "orders",
		Partition: 3,
		Offset:    42,
		Key:       []byte("order-1"),
		Value:     []byte(`{"id":"1"}`),
		Headers: []stream.Header{
			{Key: "DEVKIT_CONTENT_TYPE", Value: []byte("json")},
			{Key: "x-trace", Value: []byte("abc")},
		},
		Timestamp: time.Unix(1700000000, 0),
	}

	tests := []struct {
		name      string
		key       string
		wantValue []byte
		wantFound bool
	}{
		{
			name:      "returns the value of an existing header",
			key:       "DEVKIT_CONTENT_TYPE",
			wantValue: []byte("json"),
			wantFound: true,
		},
		{
			name:      "finds a header that is not the first",
			key:       "x-trace",
			wantValue: []byte("abc"),
			wantFound: true,
		},
		{
			name:      "reports a missing header",
			key:       "absent",
			wantValue: nil,
			wantFound: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, found := record.Header(tt.key)

			assert.Equal(t, tt.wantFound, found)
			assert.Equal(t, tt.wantValue, value)
		})
	}
}

func TestRecord_Header_NoHeaders(t *testing.T) {
	record := stream.Record{Topic: "orders"}

	value, found := record.Header("DEVKIT_CONTENT_TYPE")

	assert.False(t, found)
	assert.Nil(t, value)
}
