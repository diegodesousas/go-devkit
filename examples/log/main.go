package main

import (
    "context"

    "github.com/diegodesousas/go-devkit/pkg/log"
    "github.com/pkg/errors"
)

func main() {
    // generates a new logger instance and algo creates a child logger
    iLog := log.New()
    iLog = iLog.WithFields(
        log.Field{Key: "AnyKey", Value: "AnyValue"},
    )
    iLog.Info("Info level message")
    subLogger := iLog.WithFields(log.Field{
        Key:   "k1",
        Value: "v1",
    })
    subLogger.Info("teste")

    ctx := context.Background()
    ctx = log.WithLogger(ctx, subLogger)
    log.Error(ctx, returnError(), log.Field{
        Key:   "Key 1",
        Value: "Value 1",
    })
    iLog.Warn("A Warn message")
    iLog.Debug("A Debug message message that won't show because of the level")

    debugLogger := log.New(log.WithLevel(log.DebugLevel))
    debugLogger.Debug("This one will print")

    // passing and retrieving a logger json formatted logger from a context
    jsonLogger := log.New(log.WithJSONFormat())
    ctx = context.Background()
    ctx = log.WithLogger(ctx, jsonLogger)
    log.Info(ctx, "Error level message")
    log.Error(ctx, returnError(), log.Field{
        Key:   "Key 1",
        Value: "Value 1",
    })
}

func returnError() error {
    return errors.New("An Err message with some extra Fields")
}
