package validator

import (
	"fmt"
)

type ErrorCode string

const (
	RequiredError ErrorCode = "required"
	InvalidError  ErrorCode = "invalid"
	NotFoundError ErrorCode = "not_found"
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
		Code:    RequiredError,
		Message: fmt.Sprintf("attribute %s is required", fieldName),
	}
}

func NewInvalidError(fieldName string, value any) Error {
	return Error{
		Code:    InvalidError,
		Message: fmt.Sprintf("value %s for attribute %s is invalid", value, fieldName),
	}
}

func NewNotFoundError(entity string) Error {
	return Error{
		Code:    NotFoundError,
		Message: fmt.Sprintf("%s not found", entity),
	}
}
