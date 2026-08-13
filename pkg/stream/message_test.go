package stream_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"

	"github.com/diegodesousas/go-devkit/pkg/stream"
	"github.com/stretchr/testify/assert"
)

func TestMessage_Type(t *testing.T) {
	type expectations struct {
		messageType string
	}

	tests := []struct {
		name         string
		message      stream.Message
		expectations expectations
	}{
		{
			name:    "Text type",
			message: stream.NewTextMessage("test"),
			expectations: expectations{
				messageType: "text",
			},
		},
		{
			name:    "JSON message type",
			message: stream.NewJSONMessage("test"),
			expectations: expectations{
				messageType: "json",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expectations.messageType, tt.message.Type())
		})
	}
}

func TestMessage_Serialize(t *testing.T) {
	type expectations struct {
		serializedData []byte
		err            error
	}

	tests := []struct {
		name         string
		message      stream.Message
		expectations expectations
	}{
		{
			name:    "Text serialization success",
			message: stream.NewTextMessage("test"),
			expectations: expectations{
				serializedData: []byte("test"),
			},
		},
		{
			name:    "JSON serialization success",
			message: stream.NewJSONMessage(`{"test": "test"}`),
			expectations: expectations{
				serializedData: []byte(`"{\"test\": \"test\"}"`),
			},
		},
		{
			name:    "JSON serialization error",
			message: stream.NewJSONMessage(make(chan string)),
			expectations: expectations{
				serializedData: nil,
				err:            &json.UnsupportedTypeError{Type: reflect.TypeOf(make(chan string))},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			serialized, err := tt.message.Serialize()

			assert.Equal(t, tt.expectations.serializedData, serialized)
			assert.Equal(t, tt.expectations.err, err)
		})
	}
}

func TestTextMessage_Deserialize_Success(t *testing.T) {
	message := stream.NewTextMessage("")

	input := []byte("test")
	output := ""

	err := message.Deserialize(input, &output)

	assert.Equal(t, "test", output)
	assert.Nil(t, err)
}

func TestTextMessage_Deserialize_Error(t *testing.T) {
	message := stream.NewTextMessage("")

	input := []byte("test")
	output := "without pointer"

	err := message.Deserialize(input, output)

	assert.Equal(t, "without pointer", output)
	assert.Equal(t, stream.ErrStringDeserialization, err)
}

func TestJSONMessage_Deserialize_Success(t *testing.T) {
	type Test struct {
		Number int `json:"number"`
	}

	message := stream.NewJSONMessage(nil)

	var output Test
	input := []byte(`{"number": 10}`)

	err := message.Deserialize(input, &output)

	expectedOutput := Test{
		Number: 10,
	}

	assert.Equal(t, expectedOutput, output)
	assert.Nil(t, err)
}

func TestJSONMessage_Deserialize_Error(t *testing.T) {
	message := stream.NewJSONMessage(nil)

	output := make(chan string)

	input := []byte(`{"number": 10}`)

	err := message.Deserialize(input, output)

	expectedErr := &json.InvalidUnmarshalError{
		Type: reflect.TypeOf(make(chan string)),
	}

	assert.Equal(t, expectedErr, err)
}

func TestTextMessage_NewWithData_Success(t *testing.T) {
	message := stream.NewTextMessage("")

	data := "test"

	newMessage := message.NewWithData(data)

	assert.IsType(t, message, newMessage)
}

func TestTextMessage_NewWithData_NonStringContent(t *testing.T) {
	// The dead letter path builds a text message out of whatever the handler
	// was given, so non-string content has to render readably rather than as
	// a %!s(int=42) verb error.
	tests := []struct {
		name     string
		data     any
		expected string
	}{
		{
			name:     "int",
			data:     42,
			expected: "42",
		},
		{
			name:     "bool",
			data:     true,
			expected: "true",
		},
		{
			name:     "struct",
			data:     struct{ Name string }{Name: "devkit"},
			expected: "{devkit}",
		},
		{
			name:     "string",
			data:     "already text",
			expected: "already text",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			newMessage := stream.NewTextMessage("").NewWithData(tt.data)

			payload, err := newMessage.Serialize()
			assert.Nil(t, err)
			assert.Equal(t, tt.expected, string(payload))
		})
	}
}

func TestJSONMessage_NewWithData_Success(t *testing.T) {
	message := stream.NewJSONMessage(nil)

	data := "json string"

	newMessage := message.NewWithData(data)

	assert.IsType(t, message, newMessage)
}

func TestNewMessageType(t *testing.T) {
	type args struct {
		kafkaMessage *kafka.Message
	}

	type expectations struct {
		message stream.Message
		err     error
	}

	tests := []struct {
		name         string
		args         args
		expectations expectations
	}{
		{
			name: "Text Message Success",
			args: args{
				kafkaMessage: &kafka.Message{
					Headers: []kafka.Header{
						{
							Key:   "DEVKIT_CONTENT_TYPE",
							Value: []byte("text"),
						},
					},
				},
			},
			expectations: expectations{
				message: stream.NewTextMessage(""),
				err:     nil,
			},
		},
		{
			name: "Json Message Success",
			args: args{
				kafkaMessage: &kafka.Message{
					Headers: []kafka.Header{
						{
							Key:   "DEVKIT_CONTENT_TYPE",
							Value: []byte("json"),
						},
					},
				},
			},
			expectations: expectations{
				message: stream.NewJSONMessage(nil),
				err:     nil,
			},
		},
		{
			name: "Unknown Type Error",
			args: args{
				kafkaMessage: &kafka.Message{
					Headers: []kafka.Header{
						{
							Key:   "DEVKIT_CONTENT_TYPE",
							Value: []byte("unknown"),
						},
					},
				},
			},
			expectations: expectations{
				message: nil,
				err:     stream.ErrUnknownMessageType,
			},
		},
		{
			name: "Header Without Type Key Error",
			args: args{
				kafkaMessage: &kafka.Message{
					Headers: []kafka.Header{},
				},
			},
			expectations: expectations{
				message: nil,
				err:     stream.ErrUnknownMessageType,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			message, err := stream.NewMessageType(tt.args.kafkaMessage)

			assert.Equal(t, tt.expectations.err, err)
			assert.IsType(t, tt.expectations.message, message)
		})
	}
}
