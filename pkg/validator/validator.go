package validator

import (
	"context"

	"github.com/pkg/errors"
)

// Rule checks one property of data and returns nil when it holds. Returning one
// of the errors from NewRequiredError, NewInvalidError or NewNotFoundError gives
// the failure a machine-readable ErrorCode.
type Rule[T any] func(ctx context.Context, data T) error

// Validator runs a fixed list of rules against a value of type T.
type Validator[T any] interface {
	Validate(ctx context.Context, data T) error
}

type validator[T any] struct {
	rules []Rule[T]
}

// New returns a Validator that applies rules in the order given.
func New[T any](rules ...Rule[T]) Validator[T] {
	return &validator[T]{
		rules: rules,
	}
}

// Validate runs the rules in order and returns the first failure, wrapped, or
// nil when every rule passes. Later rules do not run once one has failed, so a
// single call reports one problem rather than all of them.
func (v validator[T]) Validate(ctx context.Context, data T) error {
	for _, rule := range v.rules {
		if err := rule(ctx, data); err != nil {
			return errors.Wrap(err, "err validator")
		}
	}

	return nil
}
