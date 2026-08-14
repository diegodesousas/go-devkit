package mapper_test

import (
	"fmt"

	"github.com/diegodesousas/go-devkit/pkg/mapper"
	"github.com/pkg/errors"
)

// Currency is a named string type, which is what Mapper keys on. The compiler
// then rejects a lookup with a plain string from some other domain.
type Currency string

func ExampleNew() {
	rates := mapper.New[Currency, float64]()
	rates.Set("BRL", 1).Set("USD", 5.4)

	rate, err := rates.Get("USD")
	if err != nil {
		panic(err)
	}
	fmt.Println("USD:", rate)

	_, err = rates.Get("JPY")
	fmt.Println("JPY:", errors.Is(err, mapper.ErrValueNotFound))

	// Output:
	// USD: 5.4
	// JPY: true
}

// Set chains, but the value it returns shares the backing map with the
// receiver - it is not a copy, so a write through one is visible through both.
func ExampleMapper_setSharesState() {
	first := mapper.New[Currency, float64]()
	second := first.Set("BRL", 1)

	second.Set("USD", 5.4)

	rate, err := first.Get("USD")
	fmt.Println(rate, err)

	// Output:
	// 5.4 <nil>
}
