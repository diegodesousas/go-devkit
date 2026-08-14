package kafka

import (
	"testing"
	"time"

	"github.com/diegodesousas/go-devkit/pkg/stream"
	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestFromKgoRecord(t *testing.T) {
	timestamp := time.Unix(1700000000, 0)

	kgoRecord := &kgo.Record{
		Topic:     "orders",
		Partition: 3,
		Offset:    42,
		Key:       []byte("order-1"),
		Value:     []byte(`{"id":"1"}`),
		Timestamp: timestamp,
		Headers: []kgo.RecordHeader{
			{Key: stream.ContentTypeHeaderKey, Value: []byte("json")},
		},
	}

	record := fromKgoRecord(kgoRecord)

	assert.Equal(t, "orders", record.Topic)
	assert.Equal(t, int32(3), record.Partition)
	assert.Equal(t, int64(42), record.Offset)
	assert.Equal(t, []byte("order-1"), record.Key)
	assert.Equal(t, []byte(`{"id":"1"}`), record.Value)
	assert.Equal(t, timestamp, record.Timestamp)

	contentType, ok := record.Header(stream.ContentTypeHeaderKey)
	assert.True(t, ok)
	assert.Equal(t, []byte("json"), contentType)
}

func TestNewReader_Validation(t *testing.T) {
	tests := []struct {
		name    string
		groupID string
		topics  []string
		options []Option
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name:    "empty group id",
			groupID: "",
			topics:  []string{"orders"},
			options: []Option{WithBrokers("localhost:9092")},
			wantErr: func(t assert.TestingT, err error, i ...any) bool {
				return assert.ErrorIs(t, err, ErrNoGroupID)
			},
		},
		{
			name:    "no topics",
			groupID: "billing",
			topics:  nil,
			options: []Option{WithBrokers("localhost:9092")},
			wantErr: func(t assert.TestingT, err error, i ...any) bool {
				return assert.ErrorIs(t, err, ErrNoTopics)
			},
		},
		{
			name:    "no brokers",
			groupID: "billing",
			topics:  []string{"orders"},
			options: nil,
			wantErr: func(t assert.TestingT, err error, i ...any) bool {
				return assert.ErrorIs(t, err, ErrNoBrokers)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader, err := NewReader(tt.groupID, tt.topics, tt.options...)

			assert.Nil(t, reader)
			tt.wantErr(t, err)
		})
	}
}
