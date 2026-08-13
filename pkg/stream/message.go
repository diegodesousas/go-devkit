package stream

import (
	"encoding/json"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
)

const (
	jsonType             = "json"
	textType             = "text"
	postgresType         = "postgresql"
	ContentTypeHeaderKey = "DEVKIT_CONTENT_TYPE"
)

var (
	ErrUnknownMessageType    = errors.New("stream: unknown message type")
	ErrStringDeserialization = errors.New("stream: string serialization error")
)

type Message interface {
	Type() string
	Serialize() ([]byte, error)
	Deserialize(payload []byte, input any) error
	NewWithData(data any) Message
}

type jsonMessage struct {
	Body any
}

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

func NewMessageType(message *kafka.Message) (Message, error) {
	for _, header := range message.Headers {
		if header.Key == ContentTypeHeaderKey {
			switch string(header.Value) {
			case jsonType, postgresType:
				return NewJSONMessage(message.Value), nil
			case textType:
				return NewTextMessage(string(message.Value)), nil
			}
		}
	}

	return nil, ErrUnknownMessageType
}
