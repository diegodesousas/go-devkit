package metrics

import (
	"context"
	"net/http"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/diegodesousas/go-devkit/pkg/log"
)

// metricKey is the context key holding the Metric client. An unexported struct
// type cannot collide with a key set by another package, which a plain string
// can.
type metricKey struct{}

// defaultRate is the statsd sample rate used when a caller does not pick one.
// It must be 1: the rate is a sampling probability, so zero would mean "never
// emit this metric".
const defaultRate = 1.0

// Metric is the StatsD client. It aliases the full datadog-go client
// interface, so a substitute has to implement every method - in practice this
// is satisfied by a real client, not a hand-written fake.
type Metric interface {
	statsd.ClientInterface
}

// New connects to the Datadog agent.
//
// The address is fixed at localhost:8125 - the usual agent endpoint for a
// sidecar or host-local agent, but not configurable.
func New() (Metric, error) {
	return statsd.New("localhost:8125")
}

func putInContext(parent context.Context, metric statsd.ClientInterface) context.Context {
	return context.WithValue(parent, metricKey{}, metric)
}

func fromContext(ctx context.Context) Metric {
	metric, ok := ctx.Value(metricKey{}).(Metric)
	if !ok {
		return nil
	}

	return metric
}

// Increment adds one to the named counter, tagged with tags.
//
// It is a no-op when ctx carries no Metric, so a metric that never reaches
// Datadog usually means the Metrics middleware was not installed. Emission
// failures are logged, not returned.
func Increment(ctx context.Context, name string, tags ...string) {
	if metric := fromContext(ctx); metric != nil {
		err := metric.Incr(name, tags, defaultRate)
		if err != nil {
			log.FromContext(ctx).Error(err.Error())
		}
	}
}

// Histogram records value in the named histogram at the given sample rate
// (1 emits every point). No-op when ctx carries no Metric.
func Histogram(ctx context.Context, name string, value float64, rate float64, tags ...string) {
	if metric := fromContext(ctx); metric != nil {
		err := metric.Histogram(name, value, tags, rate)
		if err != nil {
			log.FromContext(ctx).Error(err.Error())
		}
	}
}

// Gauge sets the named gauge to value at the given sample rate (1 emits every
// point). No-op when ctx carries no Metric.
func Gauge(ctx context.Context, name string, value float64, rate float64, tags ...string) {
	if metric := fromContext(ctx); metric != nil {
		err := metric.Gauge(name, value, tags, rate)
		if err != nil {
			log.FromContext(ctx).Error(err.Error())
		}
	}
}

// Metrics returns middleware putting metric in the request context, which is
// what makes Increment, Histogram and Gauge work downstream.
//
// It is the only injection point in this package, so code outside an HTTP
// request - Kafka consumers, background jobs - currently has no way to emit.
func Metrics(metric Metric) func(handler http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := putInContext(r.Context(), metric)
			r = r.WithContext(ctx)
			next.ServeHTTP(w, r)
		})
	}
}
