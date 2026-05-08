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

func Infof(ctx context.Context, msg string, printfParams []any, fields ...Field) {
	FromContext(ctx).Info(fmt.Sprintf(msg, printfParams...), fields...)
}

func Error(ctx context.Context, err error, fields ...Field) {
	if err == nil {
		return
	}

	var message string
	if tracer, ok := err.(stackTracer); ok {
		message = fmt.Sprintf("%s: %+v", err, tracer.StackTrace())
	} else {
		message = err.Error()
	}

	FromContext(ctx).Error(message, fields...)
}

func Debug(ctx context.Context, msg string, fields ...Field) {
	FromContext(ctx).Debug(msg, fields...)
}

func Debugf(ctx context.Context, msg string, printfParams []any, fields ...Field) {
	FromContext(ctx).Debug(fmt.Sprintf(msg, printfParams...), fields...)
}

func Warn(ctx context.Context, msg string, fields ...Field) {
	FromContext(ctx).Warn(msg, fields...)
}

func Warnf(ctx context.Context, msg string, printfParams []any, fields ...Field) {
	FromContext(ctx).Warn(fmt.Sprintf(msg, printfParams...), fields...)
}

func Fatal(ctx context.Context, msg string, fields ...Field) {
	FromContext(ctx).Fatal(msg, fields...)
}

func Fatalf(ctx context.Context, msg string, printfParams []any, fields ...Field) {
	FromContext(ctx).Fatal(fmt.Sprintf(msg, printfParams...), fields...)
}

func FatalError(ctx context.Context, err error, fields ...Field) {
	FromContext(ctx).Fatal(err.Error(), fields...)
}
