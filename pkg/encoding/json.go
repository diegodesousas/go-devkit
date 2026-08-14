package encoding

import (
	"github.com/goccy/go-json"
)

// JSONSerializer converts Go values to JSON and back. Deserialize takes a
// pointer to the destination, as encoding/json does.
type JSONSerializer interface {
	Serialize(v any) ([]byte, error)
	Deserialize(data []byte, v any) error
}

type jsonSerializer struct{}

// NewJSONSerializer returns a JSONSerializer backed by github.com/goccy/go-json,
// which honours the same struct tags as encoding/json.
func NewJSONSerializer() JSONSerializer {
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
