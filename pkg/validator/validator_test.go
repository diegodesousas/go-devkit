package validator_test

import (
    "context"
    "testing"

    "github.com/diegodesousas/go-devkit/pkg/validator"
    "github.com/pkg/errors"
    "github.com/stretchr/testify/assert"
)

func Test_Validator_New(t *testing.T) {
    rule := func(ctx context.Context, data string) error {
        return nil
    }

    validator := validator.New(rule)

    assert.NotNil(t, validator)
}

func Test_Validator_Validate_Pass(t *testing.T) {
    rule := func(ctx context.Context, data string) error {
        return nil
    }

    validator := validator.New(rule)

    err := validator.Validate(context.Background(), "test")

    assert.NoError(t, err)
}

func Test_Validator_Validate_Fail(t *testing.T) {
    rule := func(ctx context.Context, data string) error {
        return errors.New("validation failed")
    }

    validator := validator.New(rule)

    err := validator.Validate(context.Background(), "test")

    assert.EqualError(t, err, "err validator: validation failed")
}
