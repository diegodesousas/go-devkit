// Package log provides a leveled, field-oriented logger that travels through
// the request context.
//
// The Logger interface is deliberately narrow so the backend can be replaced
// without touching callers. The implementation returned by New is backed by
// logrus.
//
// There are two ways to log. The Logger methods take an explicit receiver:
//
//	logger := log.New(log.WithJSONFormat(), log.WithLevel(log.DebugLevel))
//	logger.Info("server started", log.NewField("port", 8080))
//
// The package-level helpers instead pull the logger out of a context, which is
// how the rest of the devkit logs - httpserver and the Kafka consumer both put
// a logger in the context before invoking user code:
//
//	log.Info(ctx, "order accepted", log.NewField("order-id", id))
//
// When the context carries no logger, FromContext falls back to a logger built
// with the package defaults rather than returning nil.
//
// Fields accumulate. WithFields derives a context whose logger carries extra
// fields, so a value attached once at the edge shows up on every later entry:
//
//	ctx = log.WithFields(ctx, log.NewField("trace-id", traceID))
//
// Error is the entry point for errors: it extracts the stack trace from errors
// wrapped with github.com/pkg/errors and appends it to the message. That is why
// this repository wraps with errors.Wrap and errors.WithStack rather than the
// standard library's %w verb.
//
// The formatting helpers - Infof, Debugf, Warnf and Fatalf - end in
// "format string, args ...any" so that go vet checks the verbs against the
// arguments. They do not accept fields; combine fmt.Sprintf with Info when an
// entry needs both.
package log
