package stream

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
)

const (
	jsonType     = "json"
	textType     = "text"
	postgresType = "postgresql"

	// ContentTypeHeaderKey is the Kafka header naming the encoding of a
	// message. The dispatcher writes it on produce and NewMessageType reads
	// it on consume.
	ContentTypeHeaderKey = "DEVKIT_CONTENT_TYPE"
)

// Errors returned when a payload cannot be interpreted.
var (
	// ErrUnknownMessageType is returned by NewMessageType when the message
	// carries no ContentTypeHeaderKey, or names an encoding this package does
	// not implement.
	ErrUnknownMessageType = errors.New("stream: unknown message type")

	// ErrStringDeserialization is returned when a text message is decoded
	// into a destination that is not a *string.
	ErrStringDeserialization = errors.New("stream: string serialization error")
)

// Message is a payload that knows its own encoding.
//
// Type names the encoding and is written to the DEVKIT_CONTENT_TYPE header on
// dispatch, which is how NewMessageType picks the matching implementation on
// the way back. Deserialize decodes a payload into input, which must be a
// pointer. NewWithData returns a message of the same kind carrying data.
type Message interface {
	Type() string
	Serialize() ([]byte, error)
	Deserialize(payload []byte, input any) error
	NewWithData(data any) Message
}

type jsonMessage struct {
	Body any
}

// NewJSONMessage returns a Message that encodes body as JSON.
func NewJSONMessage(body any) Message {
	return jsonMessage{
		Body: body,
	}
}

func (j jsonMessage) Type() string {
	return jsonType
}

func (j jsonMessage) Serialize() ([]byte, error) {
	data, err := json.Marshal(j.Body)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (j jsonMessage) Deserialize(payload []byte, input any) error {
	if err := json.Unmarshal(payload, input); err != nil {
		return err
	}

	return nil
}

func (j jsonMessage) NewWithData(data any) Message {
	return NewJSONMessage(data)
}

type textMessage struct {
	Body string
}

// NewTextMessage returns a Message that carries body as raw bytes.
//
// Its Deserialize only accepts a *string destination and returns
// ErrStringDeserialization for anything else.
func NewTextMessage(body string) Message {
	return textMessage{
		Body: body,
	}
}

func (t textMessage) Type() string {
	return textType
}

func (t textMessage) Serialize() ([]byte, error) {
	return []byte(t.Body), nil
}

func (t textMessage) Deserialize(payload []byte, input any) error {
	s, ok := input.(*string)
	if !ok {
		return ErrStringDeserialization
	}

	*s = string(payload)

	return nil
}

func (t textMessage) NewWithData(data any) Message {
	return NewTextMessage(fmt.Sprintf("%v", data))
}

// NewMessageType selects the Message implementation matching the
// ContentTypeHeaderKey header of a record.
//
// It returns ErrUnknownMessageType when the header is absent or names an
// encoding this package does not implement - which is what makes the consumer
// route the record to the dead letter topic instead of guessing.
func NewMessageType(record Record) (Message, error) {
	contentType, ok := record.Header(ContentTypeHeaderKey)
	if !ok {
		return nil, ErrUnknownMessageType
	}

	switch string(contentType) {
	case jsonType, postgresType:
		return NewJSONMessage(record.Value), nil
	case textType:
		return NewTextMessage(string(record.Value)), nil
	}

	return nil, ErrUnknownMessageType
}
