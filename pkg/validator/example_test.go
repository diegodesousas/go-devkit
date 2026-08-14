package validator_test

import (
	"context"
	"fmt"

	"github.com/diegodesousas/go-devkit/pkg/validator"
	"github.com/pkg/errors"
)

type order struct {
	ID    string
	Total int
}

func ExampleNew() {
	v := validator.New(
		func(ctx context.Context, o order) error {
			if validator.IsEmpty(o.ID) {
				return validator.NewRequiredError("id")
			}
			return nil
		},
		func(ctx context.Context, o order) error {
			if o.Total < 0 {
				return validator.NewInvalidError("total", o.Total)
			}
			return nil
		},
	)

	fmt.Println(v.Validate(context.Background(), order{ID: "abc", Total: 10}))
	fmt.Println(v.Validate(context.Background(), order{Total: 10}))

	// Output:
	// <nil>
	// err validator: attribute id is required
}

// Rules run in order and Validate stops at the first failure, so a payload with
// two problems reports only the first.
func ExampleValidator_stopsAtFirstFailure() {
	v := validator.New(
		func(ctx context.Context, o order) error {
			return validator.NewRequiredError("id")
		},
		func(ctx context.Context, o order) error {
			return validator.NewInvalidError("total", o.Total)
		},
	)

	fmt.Println(v.Validate(context.Background(), order{Total: -1}))

	// Output:
	// err validator: attribute id is required
}

// Validate wraps the rule error, so recovering the code needs errors.As rather
// than a type assertion.
func ExampleError() {
	v := validator.New(func(ctx context.Context, o order) error {
		return validator.NewNotFoundError("order")
	})

	err := v.Validate(context.Background(), order{})

	var vErr validator.Error
	if errors.As(err, &vErr) {
		fmt.Println("code:", vErr.Code)
		fmt.Println("message:", vErr.Message)
	}

	// Output:
	// code: not_found
	// message: order not found
}
