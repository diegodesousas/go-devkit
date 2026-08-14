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
		Topic:       "orders",
		Partition:   3,
		Offset:      42,
		LeaderEpoch: 7,
		Key:         []byte("order-1"),
		Value:       []byte(`{"id":"1"}`),
		Timestamp:   timestamp,
		Headers: []kgo.RecordHeader{
			{Key: stream.ContentTypeHeaderKey, Value: []byte("json")},
		},
	}

	record := fromKgoRecord(kgoRecord)

	assert.Equal(t, "orders", record.Topic)
	assert.Equal(t, int32(3), record.Partition)
	assert.Equal(t, int64(42), record.Offset)
	assert.Equal(t, int32(7), record.LeaderEpoch)
	assert.Equal(t, []byte("order-1"), record.Key)
	assert.Equal(t, []byte(`{"id":"1"}`), record.Value)
	assert.Equal(t, timestamp, record.Timestamp)

	contentType, ok := record.Header(stream.ContentTypeHeaderKey)
	assert.True(t, ok)
	assert.Equal(t, []byte("json"), contentType)
}

// The leader epoch must survive the round trip out of the driver and back into
// a commit. Dropping it leaves the zero value on the wire, and epoch 0 is a
// real epoch that every partition which has ever elected a new leader is long
// past - Kafka answers such a commit by rewinding the group to where epoch 0
// ended, so the consumer reprocesses everything since the first election.
func TestToCommitRecords(t *testing.T) {
	records := []stream.Record{
		{Topic: "orders", Partition: 3, Offset: 42, LeaderEpoch: 7},
		{Topic: "orders", Partition: 4, Offset: 1, LeaderEpoch: 0},
	}

	kgoRecords := toCommitRecords(records)

	assert.Len(t, kgoRecords, 2)

	assert.Equal(t, "orders", kgoRecords[0].Topic)
	assert.Equal(t, int32(3), kgoRecords[0].Partition)
	assert.Equal(t, int64(42), kgoRecords[0].Offset)
	assert.Equal(t, int32(7), kgoRecords[0].LeaderEpoch)

	assert.Equal(t, int32(4), kgoRecords[1].Partition)
	assert.Equal(t, int64(1), kgoRecords[1].Offset)
	assert.Equal(t, int32(0), kgoRecords[1].LeaderEpoch)
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
