package log_test

import (
    "context"
    stderrors "errors"
    "strings"
    "testing"

    "github.com/diegodesousas/go-devkit/pkg/log"
    "github.com/pkg/errors"
    "github.com/stretchr/testify/mock"
)

func TestInfoSuccess(t *testing.T) {
    loggerMock := &log.LoggerMock{}
    loggerMock.
        On(
            "Info",
            "info test",
            log.Field{Key: "key-field", Value: "value-field"},
        ).
        Once()

    ctx := log.WithLogger(context.Background(), loggerMock)

    log.Info(
        ctx,
        "info test",
        log.NewField("key-field", "value-field"),
    )

    loggerMock.AssertExpectations(t)
}

func TestInfofSuccess(t *testing.T) {
    loggerMock := &log.LoggerMock{}
    loggerMock.
        On(
            "Info",
            "info test 1 true param sprintf",
            log.Field{Key: "key-field", Value: "value-field"},
        ).
        Once()

    ctx := log.WithLogger(context.Background(), loggerMock)

    log.Infof(
        ctx,
        "info test %d %t %s",
        []any{1, true, "param sprintf"},
        log.NewField("key-field", "value-field"),
    )

    loggerMock.AssertExpectations(t)
}

func TestErrorSuccess(t *testing.T) {
    loggerMock := &log.LoggerMock{}
    loggerMock.
        On(
            "Error",
            "error test",
            log.Field{Key: "key-field", Value: "value-field"},
        ).
        Once()

    ctx := log.WithLogger(context.Background(), loggerMock)

    log.Error(
        ctx,
        stderrors.New("error test"),
        log.NewField("key-field", "value-field"),
    )

    loggerMock.AssertExpectations(t)
}

func TestErrorSuccess_WithStackTrace(t *testing.T) {
    loggerMock := &log.LoggerMock{}
    loggerMock.
        On(
            "Error",
            mock.MatchedBy(func(errorMessage string) bool {
                return strings.Contains(errorMessage, "github.com/diegodesousas/go-devkit/pkg/log_test.TestErrorSuccess_WithStackTrace")
            }),
            log.Field{Key: "key-field", Value: "value-field"},
        ).
        Once()

    ctx := log.WithLogger(context.Background(), loggerMock)

    log.Error(
        ctx,
        errors.New("error test"),
        log.NewField("key-field", "value-field"),
    )

    loggerMock.AssertExpectations(t)
}

func TestErrorSuccess_WithoutError(t *testing.T) {
    loggerMock := &log.LoggerMock{}

    ctx := log.WithLogger(context.Background(), loggerMock)

    log.Error(ctx, nil)

    loggerMock.AssertExpectations(t)
}

func TestDebugSuccess(t *testing.T) {
    loggerMock := &log.LoggerMock{}
    loggerMock.
        On(
            "Debug",
            "debug test",
            log.Field{Key: "key-field", Value: "value-field"},
        ).
        Once()

    ctx := log.WithLogger(context.Background(), loggerMock)

    log.Debug(
        ctx,
        "debug test",
        log.NewField("key-field", "value-field"),
    )

    loggerMock.AssertExpectations(t)
}

func TestDebugfSuccess(t *testing.T) {
    loggerMock := &log.LoggerMock{}
    loggerMock.
        On(
            "Debug",
            "info test 1 true param sprintf",
            log.Field{Key: "key-field", Value: "value-field"},
        ).
        Once()

    ctx := log.WithLogger(context.Background(), loggerMock)

    log.Debugf(
        ctx,
        "info test %d %t %s",
        []any{1, true, "param sprintf"},
        log.NewField("key-field", "value-field"),
    )

    loggerMock.AssertExpectations(t)
}

func TestWarnSuccess(t *testing.T) {
    loggerMock := &log.LoggerMock{}
    loggerMock.
        On(
            "Warn",
            "warn test",
            log.Field{Key: "key-field", Value: "value-field"},
        ).
        Once()

    ctx := log.WithLogger(context.Background(), loggerMock)

    log.Warn(
        ctx,
        "warn test",
        log.NewField("key-field", "value-field"),
    )

    loggerMock.AssertExpectations(t)
}

func TestWarnfSuccess(t *testing.T) {
    loggerMock := &log.LoggerMock{}
    loggerMock.
        On(
            "Warn",
            "info test 1 true param sprintf",
            log.Field{Key: "key-field", Value: "value-field"},
        ).
        Once()

    ctx := log.WithLogger(context.Background(), loggerMock)

    log.Warnf(
        ctx,
        "info test %d %t %s",
        []any{1, true, "param sprintf"},
        log.NewField("key-field", "value-field"),
    )

    loggerMock.AssertExpectations(t)
}

func TestFatalErrorSuccess(t *testing.T) {
    loggerMock := &log.LoggerMock{}
    loggerMock.
        On(
            "Fatal",
            "error test",
            log.Field{Key: "key-field", Value: "value-field"},
        ).
        Once()

    ctx := log.WithLogger(context.Background(), loggerMock)

    log.FatalError(
        ctx,
        errors.New("error test"),
        log.NewField("key-field", "value-field"),
    )

    loggerMock.AssertExpectations(t)
}

func TestFatalSuccess(t *testing.T) {
    loggerMock := &log.LoggerMock{}
    loggerMock.
        On(
            "Fatal",
            "info test",
            log.Field{Key: "key-field", Value: "value-field"},
        ).
        Once()

    ctx := log.WithLogger(context.Background(), loggerMock)

    log.Fatal(
        ctx,
        "info test",
        log.NewField("key-field", "value-field"),
    )

    loggerMock.AssertExpectations(t)
}

func TestFatalfSuccess(t *testing.T) {
    loggerMock := &log.LoggerMock{}
    loggerMock.
        On(
            "Fatal",
            "info test 1 true param sprintf",
            log.Field{Key: "key-field", Value: "value-field"},
        ).
        Once()

    ctx := log.WithLogger(context.Background(), loggerMock)

    log.Fatalf(
        ctx,
        "info test %d %t %s",
        []any{1, true, "param sprintf"},
        log.NewField("key-field", "value-field"),
    )

    loggerMock.AssertExpectations(t)
}
