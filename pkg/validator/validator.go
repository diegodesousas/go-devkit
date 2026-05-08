package validator

import (
	"context"

	"github.com/pkg/errors"
)

type Rule[T any] func(ctx context.Context, data T) error

type Validator[T any] interface {
	Validate(ctx context.Context, data T) error
}

type validator[T any] struct {
	rules []Rule[T]
}

func New[T any](rules ...Rule[T]) Validator[T] {
	return &validator[T]{
		rules: rules,
	}
}

func (v validator[T]) Validate(ctx context.Context, data T) error {
	for _, rule := range v.rules {
		if err := rule(ctx, data); err != nil {
			return errors.Wrap(err, "err validator")
		}
	}

	return nil
}
