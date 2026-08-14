package stream_test

import (
	"fmt"

	"github.com/diegodesousas/go-devkit/pkg/stream"
	"github.com/pkg/errors"
)

type orderPlaced struct {
	ID    string `json:"id"`
	Total int    `json:"total"`
}

func ExampleNewJSONMessage() {
	msg := stream.NewJSONMessage(orderPlaced{ID: "42", Total: 250})

	payload, err := msg.Serialize()
	if err != nil {
		panic(err)
	}

	fmt.Println(msg.Type())
	fmt.Println(string(payload))

	var decoded orderPlaced
	if err := msg.Deserialize(payload, &decoded); err != nil {
		panic(err)
	}
	fmt.Println(decoded.ID, decoded.Total)

	// Output:
	// json
	// {"id":"42","total":250}
	// 42 250
}

// A text message carries bytes unchanged, and only decodes into a *string.
func ExampleNewTextMessage() {
	msg := stream.NewTextMessage("plain payload")

	payload, err := msg.Serialize()
	if err != nil {
		panic(err)
	}
	fmt.Println(msg.Type(), string(payload))

	var s string
	if err := msg.Deserialize(payload, &s); err != nil {
		panic(err)
	}
	fmt.Println(s)

	var wrongType int
	fmt.Println(errors.Is(msg.Deserialize(payload, &wrongType), stream.ErrStringDeserialization))

	// Output:
	// text plain payload
	// plain payload
	// true
}
