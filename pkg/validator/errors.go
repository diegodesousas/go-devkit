package validator

import (
	"fmt"
)

// ErrorCode classifies a validation failure so that callers can branch on the
// kind of problem without parsing the message.
type ErrorCode string

// The classes of validation failure this package produces.
const (
	// CodeRequired means a mandatory attribute was absent or empty.
	CodeRequired ErrorCode = "required"
	// CodeInvalid means an attribute was present but its value is not
	// acceptable.
	CodeInvalid ErrorCode = "invalid"
	// CodeNotFound means a referenced entity does not exist.
	CodeNotFound ErrorCode = "not_found"
)

// Error is a validation failure carrying both a human-readable message and the
// ErrorCode that classifies it. It implements error.
//
// Validate wraps rule errors, so recover this type with errors.As rather than a
// type assertion.
type Error struct {
	Code    ErrorCode
	Message string
}

func (v Error) Error() string {
	return v.Message
}

// NewRequiredError reports that fieldName is mandatory but was not supplied.
func NewRequiredError(fieldName string) Error {
	return Error{
		Code:    CodeRequired,
		Message: fmt.Sprintf("attribute %s is required", fieldName),
	}
}

// NewInvalidError reports that value is not acceptable for fieldName.
func NewInvalidError(fieldName string, value any) Error {
	return Error{
		Code:    CodeInvalid,
		Message: fmt.Sprintf("value %v for attribute %s is invalid", value, fieldName),
	}
}

// NewNotFoundError reports that the named entity does not exist.
func NewNotFoundError(entity string) Error {
	return Error{
		Code:    CodeNotFound,
		Message: fmt.Sprintf("%s not found", entity),
	}
}
