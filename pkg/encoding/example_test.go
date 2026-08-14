package encoding_test

import (
	"fmt"

	"github.com/diegodesousas/go-devkit/pkg/encoding"
)

type product struct {
	Name  string `json:"name"`
	Price int    `json:"price"`
}

func ExampleNewJSONSerializer() {
	s := encoding.NewJSONSerializer()

	data, err := s.Serialize(product{Name: "keyboard", Price: 300})
	if err != nil {
		panic(err)
	}
	fmt.Println(string(data))

	var decoded product
	if err := s.Deserialize(data, &decoded); err != nil {
		panic(err)
	}
	fmt.Println(decoded.Name, decoded.Price)

	// Output:
	// {"name":"keyboard","price":300}
	// keyboard 300
}
