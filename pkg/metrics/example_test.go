package metrics_test

import (
	"context"
	"net/http"

	"github.com/diegodesousas/go-devkit/pkg/httpserver"
	"github.com/diegodesousas/go-devkit/pkg/metrics"
)

// Requires a Datadog agent on localhost:8125, so this example is compiled but
// not run.
func ExampleMetrics() {
	metric, err := metrics.New()
	if err != nil {
		panic(err)
	}

	// The middleware is what puts the client in the request context. Without
	// it, every emission downstream is silently a no-op.
	httpserver.New(
		httpserver.WithMiddlewares(metrics.Metrics(metric)),
		httpserver.WithRoutes(
			httpserver.NewPost("/orders", handleOrder),
		),
	)
}

func handleOrder(w http.ResponseWriter, r *http.Request) error {
	ctx := r.Context()

	// No client is threaded through the signature: it comes from the context.
	metrics.Increment(ctx, "orders.accepted", "channel:web")
	metrics.Histogram(ctx, "orders.total", 250, 1, "currency:brl")

	return nil
}

// Emission outside an HTTP request is currently a no-op, because the middleware
// is the only thing that injects a client.
func ExampleIncrement_withoutClient() {
	metrics.Increment(context.Background(), "orders.accepted") // does nothing
}
