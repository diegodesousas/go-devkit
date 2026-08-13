package validator

import (
	"fmt"
)

type ErrorCode string

const (
	CodeRequired ErrorCode = "required"
	CodeInvalid  ErrorCode = "invalid"
	CodeNotFound ErrorCode = "not_found"
)

type Error struct {
	Code    ErrorCode
	Message string
}

func (v Error) Error() string {
	return v.Message
}

func NewRequiredError(fieldName string) Error {
	return Error{
		Code:    CodeRequired,
		Message: fmt.Sprintf("attribute %s is required", fieldName),
	}
}

func NewInvalidError(fieldName string, value any) Error {
	return Error{
		Code:    CodeInvalid,
		Message: fmt.Sprintf("value %v for attribute %s is invalid", value, fieldName),
	}
}

func NewNotFoundError(entity string) Error {
	return Error{
		Code:    CodeNotFound,
		Message: fmt.Sprintf("%s not found", entity),
	}
}
