package validator_test

import (
    "testing"

    "github.com/diegodesousas/go-devkit/pkg/validator"
    "github.com/stretchr/testify/assert"
)

func Test_Validator_IsEmpty(t *testing.T) {
    tests := []struct {
        name string
        got  bool
        want bool
    }{
        {
            name: "True",
            got:  validator.IsEmpty(""),
            want: true,
        },
        {
            name: "False",
            got:  validator.IsEmpty("abc"),
            want: false,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            assert.Equal(t, tt.want, tt.got)
        })
    }
}

func Test_Validator_ContainsKey(t *testing.T) {
    tests := []struct {
        name string
        got  bool
        want bool
    }{
        {
            name: "True",
            got:  validator.ContainsKey(map[string]string{"abc": "def"}, "abc"),
            want: true,
        },
        {
            name: "False",
            got:  validator.ContainsKey(map[string]string{"abc": "def"}, "def"),
            want: false,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            assert.Equal(t, tt.want, tt.got)
        })
    }
}
