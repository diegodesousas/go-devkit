// Package metrics emits StatsD metrics through a client carried in the
// context.
//
// The client is created once and injected by middleware:
//
//	metric, err := metrics.New()
//	if err != nil {
//		return err
//	}
//
//	server := httpserver.New(
//		httpserver.WithMiddlewares(metrics.Metrics(metric)),
//	)
//
// Instrumented code then emits without holding a reference to the client:
//
//	metrics.Increment(ctx, "orders.accepted", "channel:web")
//	metrics.Histogram(ctx, "orders.total", total, 1, "currency:brl")
//
// The context lookup is the point: a handler deep in a call chain emits a
// metric without the client being threaded through every signature.
//
// It also means emission is silently a no-op when the context carries no
// client. That is deliberate - a missing metrics client should not break a
// request - but it does mean a metric that never appears in Datadog is usually
// a middleware that was never installed. Only the HTTP middleware injects
// today, so Kafka consumers and background jobs have no way to emit.
//
// New reads no configuration: the agent address is fixed at localhost:8125.
package metrics
