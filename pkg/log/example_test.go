package log_test

import (
	"context"
	"os"

	"github.com/diegodesousas/go-devkit/pkg/log"
	"github.com/pkg/errors"
)

// The output carries a timestamp, so this example is compiled but not run.
func ExampleNew() {
	logger := log.New(
		log.WithLevel(log.DebugLevel),
		log.WithJSONFormat(),
		log.WithOutput(os.Stdout),
	)

	logger.Info("server started", log.NewField("port", 8080))
}

// The package-level helpers read the logger out of the context, which is how a
// handler logs without being handed one.
func ExampleFromContext() {
	ctx := log.WithLogger(context.Background(), log.New(log.WithJSONFormat()))

	log.Info(ctx, "order accepted", log.NewField("order-id", "42"))

	// The Logger itself is reachable when a method on it is needed directly.
	log.FromContext(ctx).Warn("stock is low")
}

// Fields attached to the context appear on every later entry, which is what
// makes a trace id usable for correlation.
func ExampleWithFields() {
	ctx := log.WithLogger(context.Background(), log.New(log.WithJSONFormat()))
	ctx = log.WithFields(ctx, log.NewField("trace-id", "01H..."))

	log.Info(ctx, "charging card")
	log.Info(ctx, "card charged") // also carries trace-id
}

// Error extracts the stack trace recorded by github.com/pkg/errors and appends
// it to the message. An error wrapped with the standard library's %w has no
// stack to extract, which is why this repository wraps with errors.Wrap.
func ExampleError() {
	ctx := log.WithLogger(context.Background(), log.New(log.WithJSONFormat()))

	err := errors.Wrap(os.ErrNotExist, "loading config")

	log.Error(ctx, err, log.NewField("path", "/etc/app.yaml"))
}
