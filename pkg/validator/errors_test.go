package validator_test

import (
	"testing"

	"github.com/diegodesousas/go-devkit/pkg/validator"
	"github.com/stretchr/testify/assert"
)

func Test_Validator_Errors(t *testing.T) {
	tests := []struct {
		name string
		err  validator.Error
		want validator.Error
	}{
		{
			name: "Required Error",
			err:  validator.NewRequiredError("abc"),
			want: validator.Error{
				Code:    "required",
				Message: "attribute abc is required",
			},
		},
		{
			name: "Invalid Error",
			err:  validator.NewInvalidError("abc", "def"),
			want: validator.Error{
				Code:    "invalid",
				Message: "value def for attribute abc is invalid",
			},
		},
		{
			name: "Not Found Error",
			err:  validator.NewNotFoundError("abc"),
			want: validator.Error{
				Code:    "not_found",
				Message: "abc not found",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.err)
			assert.Equal(t, tt.want.Error(), tt.err.Error())
		})
	}
}
