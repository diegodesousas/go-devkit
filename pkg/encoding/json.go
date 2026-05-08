package encoding

import (
	"github.com/goccy/go-json"
)

type JsonSerializer interface {
	Serialize(v any) ([]byte, error)
	Deserialize(data []byte, v any) error
}

type jsonSerializer struct{}

func NewJsonSerializer() JsonSerializer {
	return jsonSerializer{}
}

func (j jsonSerializer) Serialize(v any) ([]byte, error) {
	marshal, err := json.Marshal(v)
	if err != nil {
		return marshal, err
	}

	return marshal, nil
}

func (j jsonSerializer) Deserialize(data []byte, v any) error {
	if err := json.Unmarshal(data, v); err != nil {
		return err
	}

	return nil
}
