package log

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
)

var (
	WarningTypeCritical = NewField("warning-type", "critical")
)

type stackTracer interface {
	StackTrace() errors.StackTrace
}

func NewField(key string, value any) Field {
	return Field{
		Key:   key,
		Value: value,
	}
}

func Info(ctx context.Context, msg string, fields ...Field) {
	FromContext(ctx).Info(msg, fields...)
}

// Infof formats according to a format specifier and logs the result at info
// level. The signature ends in "format string, args ...any" so that go vet
// recognises it as a printf wrapper and checks the verbs against the
// arguments. Use Info with fmt.Sprintf when the entry also needs fields.
func Infof(ctx context.Context, format string, args ...any) {
	FromContext(ctx).Info(fmt.Sprintf(format, args...))
}

func Error(ctx context.Context, err error, fields ...Field) {
	if err == nil {
		return
	}

	var message string
	var tracer stackTracer
	if errors.As(err, &tracer) {
		message = fmt.Sprintf("%s: %+v", err, tracer.StackTrace())
	} else {
		message = err.Error()
	}

	FromContext(ctx).Error(message, fields...)
}

func Debug(ctx context.Context, msg string, fields ...Field) {
	FromContext(ctx).Debug(msg, fields...)
}

// Debugf formats according to a format specifier and logs the result at debug
// level. See Infof for why fields are not accepted here.
func Debugf(ctx context.Context, format string, args ...any) {
	FromContext(ctx).Debug(fmt.Sprintf(format, args...))
}

func Warn(ctx context.Context, msg string, fields ...Field) {
	FromContext(ctx).Warn(msg, fields...)
}

// Warnf formats according to a format specifier and logs the result at warn
// level. See Infof for why fields are not accepted here.
func Warnf(ctx context.Context, format string, args ...any) {
	FromContext(ctx).Warn(fmt.Sprintf(format, args...))
}

func Fatal(ctx context.Context, msg string, fields ...Field) {
	FromContext(ctx).Fatal(msg, fields...)
}

// Fatalf formats according to a format specifier and logs the result at fatal
// level. See Infof for why fields are not accepted here.
func Fatalf(ctx context.Context, format string, args ...any) {
	FromContext(ctx).Fatal(fmt.Sprintf(format, args...))
}

func FatalError(ctx context.Context, err error, fields ...Field) {
	FromContext(ctx).Fatal(err.Error(), fields...)
}
