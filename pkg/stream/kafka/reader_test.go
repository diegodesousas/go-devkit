package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/diegodesousas/go-devkit/pkg/stream"
	"github.com/pkg/errors"
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

// fetchWithError builds a fetch carrying one partition error, which is the
// shape franz-go injects its informational errors in.
func fetchWithError(topic string, partition int32, err error) kgo.Fetches {
	return kgo.Fetches{
		{
			Topics: []kgo.FetchTopic{
				{
					Topic: topic,
					Partitions: []kgo.FetchPartition{
						{Partition: partition, Err: err},
					},
				},
			},
		},
	}
}

// Not every error franz-go puts in a fetch is a reason to stop consuming. It
// injects some of them purely to inform - the client has already recovered by
// the time the caller sees them - and returning those from Poll kills a healthy
// consumer loop. ErrDataLoss is the one that matters most here: an epoch the
// broker has moved past makes franz-go reset the cursor and report it, so
// treating it as fatal turns a rewind into a rewind plus a crash.
func TestReportFetchErrors(t *testing.T) {
	fatalErr := errors.New("broker unreachable")

	tests := []struct {
		name    string
		fetches kgo.Fetches
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name:    "no errors at all",
			fetches: kgo.Fetches{},
			wantErr: func(t assert.TestingT, err error, i ...any) bool {
				return assert.Nil(t, err)
			},
		},
		{
			name:    "data loss is informational",
			fetches: fetchWithError("orders", 0, &kgo.ErrDataLoss{Topic: "orders", Partition: 0, ConsumedTo: 10, ResetTo: 4}),
			wantErr: func(t assert.TestingT, err error, i ...any) bool {
				return assert.Nil(t, err)
			},
		},
		{
			name:    "a lost group session is informational",
			fetches: fetchWithError("orders", 0, &kgo.ErrGroupSession{Err: fatalErr}),
			wantErr: func(t assert.TestingT, err error, i ...any) bool {
				return assert.Nil(t, err)
			},
		},
		{
			name:    "a cancelled context is answered by Poll itself",
			fetches: fetchWithError("orders", 0, context.Canceled),
			wantErr: func(t assert.TestingT, err error, i ...any) bool {
				return assert.Nil(t, err)
			},
		},
		{
			name:    "anything else ends the poll",
			fetches: fetchWithError("orders", 3, fatalErr),
			wantErr: func(t assert.TestingT, err error, i ...any) bool {
				return assert.ErrorIs(t, err, fatalErr) &&
					assert.Contains(t, err.Error(), "orders/3")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.wantErr(t, reportFetchErrors(context.Background(), tt.fetches))
		})
	}
}

// One bad partition must not cost the caller the records every other partition
// delivered in the same fetch: fetches.Err() reports the first error across all
// of them, and discarding the batch on it drops good records that would then be
// redelivered - or, if the error keeps coming back, never processed at all.
func TestReportFetchErrors_KeepsScanningPastAnInformationalOne(t *testing.T) {
	expectedErr := errors.New("not authorized")

	fetches := kgo.Fetches{
		{
			Topics: []kgo.FetchTopic{
				{
					Topic: "orders",
					Partitions: []kgo.FetchPartition{
						{Partition: 0, Err: &kgo.ErrDataLoss{Topic: "orders", Partition: 0}},
						{Partition: 1, Err: expectedErr},
					},
				},
			},
		},
	}

	err := reportFetchErrors(context.Background(), fetches)

	assert.ErrorIs(t, err, expectedErr)
	assert.Contains(t, err.Error(), "orders/1")
}
