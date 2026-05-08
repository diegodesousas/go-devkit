package encoding_test

import (
    "reflect"
    "testing"

    "github.com/diegodesousas/go-devkit/pkg/encoding"
    "github.com/goccy/go-json"
    "github.com/stretchr/testify/assert"
)

func Test_JsonSerializer_Serialize(t *testing.T) {
    type args struct {
        value any
    }

    type expected struct {
        err            error
        serializedData []byte
    }

    tests := []struct {
        name     string
        args     args
        expected expected
    }{
        {
            name: "Success",
            args: args{
                value: struct {
                    Text    string  `json:"text"`
                    Number  float64 `json:"number"`
                    Boolean bool    `json:"boolean"`
                }{
                    Text:    "test",
                    Number:  9999,
                    Boolean: true,
                },
            },
            expected: expected{
                err:            nil,
                serializedData: []byte(`{"text":"test","number":9999,"boolean":true}`),
            },
        },

        {
            name: "Error",
            args: args{
                value: struct {
                    Channel chan string `json:"channel"`
                }{
                    Channel: make(chan string, 10),
                },
            },
            expected: expected{
                err: &json.UnsupportedTypeError{
                    Type: reflect.TypeOf(make(chan string, 10)),
                },
                serializedData: nil,
            },
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            serializer := encoding.NewJsonSerializer()

            serializedData, err := serializer.Serialize(tt.args.value)

            assert.Equal(t, tt.expected.err, err)
            assert.Equal(t, tt.expected.serializedData, serializedData)
        })
    }
}

func Test_JsonSerializer_Deserialize_Success(t *testing.T) {
    type test struct {
        Text    string  `json:"text"`
        Number  float64 `json:"number"`
        Boolean bool    `json:"boolean"`
    }

    serializer := encoding.NewJsonSerializer()

    value := []byte(`{"text":"test","number":9999,"boolean":true}`)
    target := test{}

    deserializedData := test{
        Text:    "test",
        Number:  9999,
        Boolean: true,
    }

    err := serializer.Deserialize(value, &target)

    assert.Nil(t, err)
    assert.Equal(t, deserializedData, target)
}

func Test_JsonSerializer_Deserialize_Error(t *testing.T) {
    type test struct {
        Text    string  `json:"text"`
        Number  float64 `json:"number"`
        Boolean bool    `json:"boolean"`
    }

    serializer := encoding.NewJsonSerializer()

    value := []byte(`{"boolean":999}`)
    target := test{}

    deserializedData := test{}

    err := serializer.Deserialize(value, &target)

    assert.IsType(t, err, &json.SyntaxError{})
    assert.Equal(t, deserializedData, target)
}
