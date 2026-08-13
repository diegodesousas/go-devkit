package log_test

import (
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "testing"

    "github.com/diegodesousas/go-devkit/pkg/log"
    "github.com/stretchr/testify/assert"
)

type logMessage struct {
    RequestID string `json:"RequestID"`
    Level     string `json:"level"`
    Msg       string `json:"msg"`
}

func TestWithExitFunc(t *testing.T) {
    var testCode int
    mockExitFunc := func(code int) {
        testCode = code
    }

    buf := &bytes.Buffer{}
    logger := log.New(log.WithOutput(buf), log.WithExitFunc(mockExitFunc))
    logger.Fatal("Fatal Test")

    assert.Contains(t, buf.String(), "Fatal")

    expectedCode := 1
    assert.Equal(t, testCode, expectedCode)
}

func TestWithJsonFormat(t *testing.T) {
    buf := &bytes.Buffer{}
    logger := log.New(log.WithOutput(buf), log.WithJSONFormat())
    logger.WithFields(log.Field{Key: "RequestID", Value: "XXX-XXX"}).Info("Test")
    t.Log(buf.String())
    gotMessage := logMessage{}
    expectedMessage := logMessage{
        RequestID: "XXX-XXX",
        Level:     "info",
        Msg:       "Test",
    }
    err := json.Unmarshal(buf.Bytes(), &gotMessage)
    assert.NoError(t, err)
    assert.Equal(t, gotMessage, expectedMessage)
}

func TestLoggerFromContext(t *testing.T) {
    buf := &bytes.Buffer{}
    ctx := context.Background()

    logger := log.New(log.WithOutput(buf))
    subLogger := logger.WithFields(log.Field{Key: "RequestID", Value: "XXX-XXX"})
    assert.NotEqual(t, logger, subLogger)

    ctx = log.WithLogger(ctx, subLogger)
    loggerFromContext := log.FromContext(ctx)

    assert.Equal(t, subLogger, loggerFromContext)
    assert.NotEqual(t, loggerFromContext, logger)

    ctx = log.WithFields(ctx, log.Field{Key: "TraceID", Value: "YYY-YYY"})

    logger.Info("Main logger")
    assert.NotContains(t, buf.String(), "RequestID=XXX-XXX")

    loggerFromContext = log.FromContext(ctx)
    loggerFromContext.Info("Sub logger")
    assert.Contains(t, buf.String(), "RequestID=XXX-XXX")
    assert.Contains(t, buf.String(), "TraceID=YYY-YYY")
}

func TestNewLoggerFromEmptyContext(t *testing.T) {
    newLogger := log.FromContext(context.Background())
    assert.NotNil(t, newLogger)
    assert.ObjectsAreEqual(log.New(), newLogger)
}

func TestWithLevel(t *testing.T) {
    buf := &bytes.Buffer{}
    mockMsg := "Mock log message"
    tests := []struct {
        name      string
        args      log.Level
        want      string
        logMethod func(log.Logger)
    }{
        {
            name: "Info level",
            args: log.InfoLevel,
            want: "level=info",
            logMethod: func(l log.Logger) {
                l.Info(mockMsg)
            },
        },
        {
            name: "Error level",
            args: log.ErrorLevel,
            want: "level=error",
            logMethod: func(l log.Logger) {
                l.Error(mockMsg)
            },
        },
        {
            name: "Warn level",
            args: log.WarnLevel,
            want: "level=warn",
            logMethod: func(l log.Logger) {
                l.Warn(mockMsg)
            },
        },
        {
            name: "Debug level",
            args: log.DebugLevel,
            want: "level=debug",
            logMethod: func(l log.Logger) {
                l.Debug(mockMsg)
            },
        },
    }
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            buf.Reset()
            logger := log.New(
                log.WithLevel(tt.args),
                log.WithOutput(buf),
            )
            tt.logMethod(logger)
            assert.Contains(t, buf.String(), tt.want)
            assert.Contains(t, buf.String(), fmt.Sprintf("msg=%q", mockMsg))
        })
    }

}
